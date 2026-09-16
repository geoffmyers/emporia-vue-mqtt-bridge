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
import os
import re
import signal
import sys
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone

import paho.mqtt.client as mqtt  # noqa: F401  (pulled in via ha_mqtt_bridge but explicit for clarity)
from pyemvue import PyEmVue
from pyemvue.enums import Scale, Unit
from ha_mqtt_bridge import (
    ThreadedPublisher,
    availability_block,
    build_device_block,
    build_discovery_payload,
    configure_logging,
    register_github_error_reporter,
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
            usage_payload[ch.slug] = {
                "watts": round(ch.watts, 1),
                "kwh_per_min": round(ch.kwh_per_min or 0, 6),
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
        health_path="/tmp/healthy",
    )
    pub.start()

    # Publish discovery once on startup
    discovery_count = 0
    for component, unique_id, payload in discovery_specs(devices):
        pub.publish_discovery(component=component, unique_id=unique_id, payload=payload)
        discovery_count += 1
    log.info("discovery published: %d entities", discovery_count)

    stopping = False

    def on_signal(signum, _frame):
        nonlocal stopping
        log.info("signal %s, shutting down", signum)
        stopping = True

    signal.signal(signal.SIGTERM, on_signal)
    signal.signal(signal.SIGINT, on_signal)

    while not stopping:
        try:
            poll_instant_usage(vue, devices)
            n = publish_channels(pub, devices)
            log.debug("poll ok: %d channel reading(s) published", n)
        except Exception as e:
            # pyemvue raises NotAuthorizedException via its inner cognito client;
            # rather than introspect the exception class hierarchy, just blanket
            # re-auth on any failure that smells like an auth error.
            err_str = str(e)
            if "NotAuthorized" in err_str or "Unauthorized" in err_str or "401" in err_str:
                log.warning("auth expired (%s); re-logging in", err_str)
                try:
                    vue = emporia_login()
                except Exception as e2:
                    log.error("re-login failed: %s", e2)
                    time.sleep(30)
            else:
                log.error("poll failed: %s", err_str)

        for _ in range(POLL_INTERVAL):
            if stopping:
                break
            time.sleep(1)

    pub.stop()
    log.info("shutdown clean")
    return 0


if __name__ == "__main__":
    sys.exit(main())
