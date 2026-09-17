"""
Emporia Vue Cloud → MQTT bridge.

Polls Emporia's `getDeviceListUsages` API on a 60-second cadence and
publishes per-channel power (watts) + per-channel name to MQTT, with
HA Discovery configs so each circuit becomes a `sensor.<...>_power`
in Home Assistant. Also publishes a JSON sidecar topic per device
(`emporia/<gid>/usage`) carrying the full instant-usage payload — for
downstream Telegraf consumers that want to ingest all channels as one
measurement.

Co-exists with HA's `emporia_vue` custom_component without entity-id
collision — all entities live under a separate HA device
("Emporia Bridge: <device_name>") with unique_id prefix
`emporia_mqtt_bridge_<device_gid>_*`.

Auth uses the same pyemvue Cognito flow HA uses (pyemvue's built-in user
pool us-east-2_ghlOXVLi1 and app client 4qte47jbstod8apnfic0bunmrq, the same
for every account). Token re-auth on `NotAuthorized`.
"""

from __future__ import annotations

import json
import logging
import os
import re
import signal
import sys
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone

import paho.mqtt.client as mqtt  # noqa: F401  (pulled in via ha_mqtt_bridge but explicit for clarity)
import requests
from pyemvue import PyEmVue
from pyemvue.enums import Scale, Unit
from ha_mqtt_bridge import (
    ThreadedPublisher,
    availability_block,
    build_device_block,
    build_discovery_payload,
    configure_logging,
    register_github_error_reporter,
    watch_ha_birth,
)


# --- production-error reporter ---------------------------------------
# Installs sys.excepthook + threading.excepthook so every uncaught
# exception flows through GitHub repository_dispatch → the Production
# Error Intake workflow → Claude auto-fix PR. Silently disabled when
# GITHUB_ERROR_TOKEN is unset (e.g. local dev).
register_github_error_reporter("emporia-vue-mqtt-bridge")
# ---------------------------------------------------------------------
USERNAME = os.environ["EMPORIA_USERNAME"]
PASSWORD = os.environ["EMPORIA_PASSWORD"]

MQTT_HOST = os.environ.get("MQTT_HOST", "mosquitto")
MQTT_PORT = int(os.environ.get("MQTT_PORT", "1883"))
MQTT_USER = os.environ.get("MQTT_USER", "")
MQTT_PASS = os.environ["MQTT_PASSWORD"]
# Off by default (current behaviour) — set MQTT_TLS=1 for a broker that
# requires TLS; MQTT_CA_FILE points at a custom CA bundle (system trust
# store is used when unset).
MQTT_TLS = os.environ.get("MQTT_TLS", "0") != "0"
MQTT_CA_FILE = os.environ.get("MQTT_CA_FILE") or None

POLL_INTERVAL = int(os.environ.get("POLL_INTERVAL", "60"))
DISCOVERY_PREFIX = os.environ.get("HA_DISCOVERY_PREFIX", "homeassistant")
TOPIC_PREFIX = os.environ.get("MQTT_TOPIC_PREFIX", "emporia")
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()

BRIDGE_LWT_TOPIC = f"{TOPIC_PREFIX}/bridge/online"


# Sanitize channel names into slug suitable for topics + unique_ids.
# Emporia names look like "7/9 - Dryer", "Bedroom 2 & Office",
# "Balance", "Main". Strip the leading "N -" / "N/M -" prefix that's just the
# breaker-position number, then slugify the rest.
def _channel_slug(channel_num: str, raw_name: str | None) -> str:
    # pyemvue uses `"1,2,3"` as the canonical channel-num for the Mains (whole-house
    # aggregate). Emporia's API returns a null `name` for it, so without this
    # carve-out the slug would come out as "channel_1_2_3" — ugly + breaks tag
    # consistency in InfluxDB. Same for the synthetic Balance channel.
    if channel_num == "1,2,3":
        return "main"
    if channel_num.lower() == "balance":
        return "balance"
    name = raw_name or f"channel_{channel_num}"
    # Strip leading breaker-position prefix: "N - " or "N/M - " or "N,M - "
    name = re.sub(r"^[\d,/]+\s*-\s*", "", name).strip()
    # Lowercase + replace non-alphanumeric runs with single underscore
    slug = re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_")
    return slug or f"channel_{channel_num}"


def _channel_display(channel_num: str, raw_name: str | None) -> str:
    if channel_num == "1,2,3":
        return "Main"
    if channel_num.lower() == "balance":
        return "Balance"
    if not raw_name:
        return f"Channel {channel_num}"
    return re.sub(r"^[\d,/]+\s*-\s*", "", raw_name).strip() or f"Channel {channel_num}"


@dataclass
class Channel:
    device_gid: int
    channel_num: str        # may be "1", "2", ..., "1,2,3" (mains), or "Balance"
    slug: str
    display: str
    type: str | None
    watts: float | None = None
    kwh_per_min: float | None = None
    # Running kWh total, integrated from `watts` over wall-clock time
    # between polls (a left-Riemann sum of the same power reading the
    # `_power` sensor already publishes — equivalent to what users were
    # doing by hand with HA's own Riemann-sum "Integration" helper
    # against that entity). Seeded from the last retained value on
    # startup so a bridge restart doesn't reset the Energy dashboard's
    # history to zero; see `seed_energy_from_retained`.
    energy_kwh: float = 0.0


@dataclass
class Device:
    device_gid: int
    name: str
    model: str
    firmware: str | None
    channels: list[Channel] = field(default_factory=list)


# -------------------------------------------------------------- Auth + polling


def emporia_login() -> PyEmVue:
    """Create a logged-in PyEmVue client. Raises on failure."""
    vue = PyEmVue()
    vue.login(username=USERNAME, password=PASSWORD)
    return vue


def is_auth_error(vue: PyEmVue, exc: BaseException) -> bool:
    """True if `exc` looks like an expired/invalid Emporia session.

    Prefers pyemvue's own exception types over string-matching:

      - `requests.HTTPError` with a 401 response — pyemvue's internal
        `Auth.request()` already retries once on 401 with a token
        refresh; this only fires when that refresh itself didn't fix it
        (`response.raise_for_status()` in e.g. `get_devices()` raises
        this for a 401 that persists after the retry).
      - `NotAuthorizedException` — the boto3-generated exception
        pycognito raises from `Auth.refresh_tokens()` when the refresh
        token itself is no longer valid. Generated dynamically per
        botocore client, so it's read off the live `vue` instance
        rather than imported.

    Falls back to the original substring check for anything neither
    type covers (pyemvue doesn't guarantee every auth-shaped failure
    surfaces as one of the above)."""
    if isinstance(exc, requests.HTTPError):
        resp = exc.response
        if resp is not None and resp.status_code == 401:
            return True
    try:
        not_authorized = vue.auth.cognito.client.exceptions.NotAuthorizedException
    except AttributeError:
        not_authorized = None
    if not_authorized is not None and isinstance(exc, not_authorized):
        return True
    err_str = str(exc)
    return "NotAuthorized" in err_str or "Unauthorized" in err_str or "401" in err_str


def discover_devices(vue: PyEmVue) -> list[Device]:
    devices: list[Device] = []
    for d in vue.get_devices():
        ch_list: list[Channel] = []
        for c in d.channels:
            num = str(c.channel_num)
            ch_list.append(Channel(
                device_gid=d.device_gid,
                channel_num=num,
                slug=_channel_slug(num, c.name),
                display=_channel_display(num, c.name),
                type=getattr(c, "type", None),
            ))
        devices.append(Device(
            device_gid=d.device_gid,
            name=d.device_name or f"Vue {d.device_gid}",
            model=d.model or "Vue",
            firmware=getattr(d, "firmware", None),
            channels=ch_list,
        ))
    return devices


def poll_instant_usage(vue: PyEmVue, devices: list[Device]) -> None:
    """Mutate `devices` in place with fresh per-channel watts.

    Note: Emporia returns multiple `Device` objects sharing the same
    `device_gid` (one for the Vue 2 itself with the Mains channel, one
    for the 16-channel expander with the sub-circuits, one virtual
    object holding the synthetic Balance channel). The API response is
    keyed by device_gid → DeviceUsage, with ALL channels from all
    sibling devices merged. We iterate each of our `Device` objects
    independently and look up its own subset of channels, so all
    Channel objects across all Device-with-same-gid siblings get
    populated. An earlier version of this function used a dict keyed
    only on gid which overwrote sibling devices, dropping Mains +
    Balance + Air Handler from publication.
    """
    gids = sorted({d.device_gid for d in devices})
    if not gids:
        return
    usages = vue.get_device_list_usage(
        deviceGids=gids,
        instant=None,
        scale=Scale.MINUTE.value,
        unit=Unit.KWH.value,
    )
    for d in devices:
        dev_usage = usages.get(d.device_gid)
        if dev_usage is None:
            continue
        for ch in d.channels:
            ch_usage = dev_usage.channels.get(ch.channel_num)
            if ch_usage is None:
                ch.kwh_per_min = None
                ch.watts = None
                continue
            kwh_per_min = ch_usage.usage
            if kwh_per_min is None:
                ch.kwh_per_min = None
                ch.watts = None
                continue
            ch.kwh_per_min = float(kwh_per_min)
            # Convert kWh/min → watts: kWh × 60 (to kw, sustained for 1h) × 1000 (W)
            ch.watts = float(kwh_per_min) * 60.0 * 1000.0


def accumulate_energy(devices: list[Device], elapsed_hours: float) -> None:
    """Integrate each channel's current `watts` over `elapsed_hours`
    (the wall-clock time since the previous poll) into `energy_kwh`.

    A left-Riemann sum, same approximation HA's own "Integration -
    Riemann sum" helper makes against a power sensor — this just bakes
    that into the bridge so the result is a real `total_increasing`
    energy entity usable in HA's Energy dashboard without any
    per-install helper configuration. Downtime is not back-filled: the
    total simply resumes accumulating from wherever it left off,
    which is the same limitation any restart-seeded integration has.
    """
    if elapsed_hours <= 0:
        return
    for d in devices:
        for ch in d.channels:
            if ch.watts is None:
                continue
            ch.energy_kwh += (ch.watts / 1000.0) * elapsed_hours


def energy_state_topic(device_gid: int, slug: str) -> str:
    return f"{TOPIC_PREFIX}/{device_gid}/{slug}/energy_kwh"


def seed_energy_from_retained(
    pub: ThreadedPublisher,
    devices: list[Device],
    log: logging.Logger,
    wait_s: float = 2.5,
) -> None:
    """Read back each channel's last-published `energy_kwh` (published
    retained) so the running total survives a bridge restart — this
    stack has no data volume to persist to (see docker-compose.example.yml),
    so the retained MQTT message IS the persistence layer.

    Subscribes, blocks briefly for the broker to deliver any retained
    message, then swaps in a no-op handler so a later echo of the
    bridge's own publish (or a stray duplicate delivery) can't
    overwrite the now-live running total mid-flight.
    """
    by_topic: dict[str, Channel] = {
        energy_state_topic(d.device_gid, ch.slug): ch
        for d in devices for ch in d.channels
    }
    if not by_topic:
        return

    def _on_retained(topic: str, payload: bytes) -> None:
        ch = by_topic.get(topic)
        if ch is None:
            return
        try:
            ch.energy_kwh = float(payload.decode())
        except (ValueError, UnicodeDecodeError):
            log.warning("bad retained energy_kwh payload on %s: %r", topic, payload)

    for topic in by_topic:
        pub.subscribe(topic, _on_retained)
    if not pub.wait_until_connected(timeout=10.0):
        log.warning("MQTT not connected after 10s; energy totals may start from 0")
    time.sleep(wait_s)
    noop = lambda _topic, _payload: None  # noqa: E731
    for topic in by_topic:
        pub.subscribe(topic, noop)
    seeded = sum(1 for ch in {id(c): c for c in by_topic.values()}.values() if ch.energy_kwh)
    log.info("energy totals seeded from retained state: %d/%d channel(s) non-zero",
              seeded, len(by_topic))


# -------------------------------------------------------------- HA Discovery


def _device_block(d: Device) -> dict:
    return build_device_block(
        identifiers=[f"emporia_mqtt_bridge_{d.device_gid}"],
        name=f"Emporia Bridge: {d.name}",
        manufacturer="Emporia",
        model=d.model,
        sw_version=d.firmware or None,
    )


def discovery_specs(devices: list[Device]) -> list[tuple[str, str, dict]]:
    items: list[tuple[str, str, dict]] = []
    avail = availability_block(BRIDGE_LWT_TOPIC)
    for d in devices:
        dev_uid = f"emporia_mqtt_bridge_{d.device_gid}"
        device_block = _device_block(d)
        base = f"{TOPIC_PREFIX}/{d.device_gid}"

        for ch in d.channels:
            slug = ch.slug
            unique_id = f"{dev_uid}_{slug}_power"
            object_id = unique_id
            items.append((
                "sensor",
                f"{dev_uid}/{slug}_power",
                build_discovery_payload(
                    name=f"{ch.display} Power",
                    unique_id=unique_id,
                    object_id=object_id,
                    state_topic=f"{base}/{slug}/watts",
                    device=device_block,
                    device_class="power",
                    unit_of_measurement="W",
                    state_class="measurement",
                    icon="mdi:flash",
                    **avail,
                ),
            ))
            # NEW unique_id suffix (`_energy`, distinct from `_power`) so
            # existing power entities and their HA history are untouched.
            energy_unique_id = f"{dev_uid}_{slug}_energy"
            items.append((
                "sensor",
                f"{dev_uid}/{slug}_energy",
                build_discovery_payload(
                    name=f"{ch.display} Energy",
                    unique_id=energy_unique_id,
                    object_id=energy_unique_id,
                    state_topic=energy_state_topic(d.device_gid, slug),
                    device=device_block,
                    device_class="energy",
                    unit_of_measurement="kWh",
                    state_class="total_increasing",
                    icon="mdi:lightning-bolt",
                    **avail,
                ),
            ))
    return items


# -------------------------------------------------------------- publish


def publish_channels(pub: ThreadedPublisher, devices: list[Device]) -> int:
    n = 0
    for d in devices:
        base = f"{TOPIC_PREFIX}/{d.device_gid}"
        usage_payload = {}
        for ch in d.channels:
            if ch.watts is None:
                continue
            pub.publish_state(f"{base}/{ch.slug}/watts", f"{ch.watts:.1f}")
            pub.publish_state(energy_state_topic(d.device_gid, ch.slug), f"{ch.energy_kwh:.6f}")
            usage_payload[ch.slug] = {
                "watts": round(ch.watts, 1),
                "kwh_per_min": round(ch.kwh_per_min or 0, 6),
                "energy_kwh": round(ch.energy_kwh, 6),
                "channel": ch.channel_num,
                "display": ch.display,
            }
            n += 1
        # JSON sidecar (one payload per device per poll — for Telegraf
        # `json_v2` consumers that want a single MQTT subscribe across
        # all channels)
        if usage_payload:
            pub.publish_state(
                f"{base}/usage",
                json.dumps({
                    "device_gid": d.device_gid,
                    "name": d.name,
                    "model": d.model,
                    "polled_at": datetime.now(timezone.utc).isoformat(),
                    "channels": usage_payload,
                }),
            )
    return n


# -------------------------------------------------------------- main


def main() -> int:
    log = configure_logging("emporia-vue-mqtt-bridge", LOG_LEVEL)
    log.info("starting; poll=%ss", POLL_INTERVAL)

    vue = emporia_login()
    log.info("emporia login ok; user=%s", USERNAME)

    devices = discover_devices(vue)
    total_channels = sum(len(d.channels) for d in devices)
    log.info(
        "discovered %d device(s) / %d total channels: %s",
        len(devices), total_channels,
        ", ".join(f"{d.name} ({d.model}, {len(d.channels)} ch)" for d in devices),
    )

    pub = ThreadedPublisher(
        host=MQTT_HOST, port=MQTT_PORT, username=MQTT_USER, password=MQTT_PASS,
        client_id=f"emporia-vue-mqtt-bridge-{uuid.uuid4().hex[:8]}",
        lwt_topic=BRIDGE_LWT_TOPIC, discovery_prefix=DISCOVERY_PREFIX,
        health_path="/tmp/healthy", tls=MQTT_TLS, ca_file=MQTT_CA_FILE,
    )
    pub.start()

    def publish_all_discovery() -> None:
        count = 0
        for component, unique_id, payload in discovery_specs(devices):
            pub.publish_discovery(component=component, unique_id=unique_id, payload=payload)
            count += 1
        log.info("discovery published: %d entities", count)

    publish_all_discovery()

    # No data volume on this stack (see docker-compose.example.yml) — the
    # running energy total is persisted as a retained MQTT topic instead;
    # read it back before the first poll so a restart doesn't zero it.
    seed_energy_from_retained(pub, devices, log)

    watch_ha_birth(pub, publish_all_discovery, discovery_prefix=DISCOVERY_PREFIX)

    stopping = False

    def on_signal(signum, _frame):
        nonlocal stopping
        log.info("signal %s, shutting down", signum)
        stopping = True

    signal.signal(signal.SIGTERM, on_signal)
    signal.signal(signal.SIGINT, on_signal)

    last_poll_monotonic = time.monotonic()

    while not stopping:
        try:
            poll_instant_usage(vue, devices)
            now_mono = time.monotonic()
            elapsed_hours = max(0.0, (now_mono - last_poll_monotonic) / 3600.0)
            last_poll_monotonic = now_mono
            accumulate_energy(devices, elapsed_hours)
            n = publish_channels(pub, devices)
            log.debug("poll ok: %d channel reading(s) published", n)
        except Exception as e:
            if is_auth_error(vue, e):
                log.warning("auth expired (%s); re-logging in", e)
                try:
                    vue = emporia_login()
                except Exception:
                    log.exception("re-login failed")
                    time.sleep(30)
            else:
                log.exception("poll failed")

        for _ in range(POLL_INTERVAL):
            if stopping:
                break
            time.sleep(1)

    pub.stop()
    log.info("shutdown clean")
    return 0


if __name__ == "__main__":
    sys.exit(main())
