# Architecture

A single Python process built on [`pyemvue`](https://github.com/magico13/PyEmVue):
log in once, discover devices and channels, then poll on a fixed interval.

## Auth

`pyemvue` handles the AWS Cognito login flow Emporia's own apps use; the
bridge just calls `PyEmVue.login(username, password)`. `pyemvue`'s internal
Cognito wrapper renews the session transparently for normal operation. If a
poll fails with an error whose text contains `NotAuthorized`, `Unauthorized`
or `401`, the bridge treats it as an expired session and logs in again from
scratch rather than trying to introspect `pyemvue`'s exception hierarchy.

## Device and channel discovery

On startup, `discover_devices()` calls `vue.get_devices()` once and builds a
`Device` per physical unit (a Vue 2 controller, or an expander panel), each
holding a list of `Channel`s. Emporia can return more than one `Device`
object sharing the same `device_gid` — for example, a Vue 2 plus its
expander panel both report under the controller's ID — so usage polling
looks up each `Device` object's own channel subset independently rather than
keying by `device_gid` alone; an earlier version that did key by `device_gid`
silently dropped some channels.

## Channel-name sanitizing

Emporia's channel names carry a leading breaker-position prefix, e.g.
`"7/9 - Dryer"`, and use two synthetic channel numbers: `"1,2,3"` for
the whole-house Mains aggregate, and `"Balance"` for the unmonitored
remainder. `_channel_slug()` maps these to the `main` and `balance` slugs
respectively, and for a normal circuit strips the leading `N -` / `N/M -`
prefix and snake-cases what's left (`"7/9 - Dryer"` → `dryer`).
The slug is used in both the MQTT topic path and the Home Assistant entity
ID, so renaming a circuit in the Emporia app changes its entity ID on the
next bridge restart.

## Poll cycle

Every `POLL_INTERVAL` seconds (default 60), `poll_instant_usage()` calls
`vue.get_device_list_usage()` for all discovered device GIDs at once,
requesting one-minute-scale kWh. Emporia's API returns energy, not power, so
the bridge converts: `watts = kwh_per_minute × 60 × 1000`. Each channel's
watts (and the raw kWh/min) are then published, plus a per-device JSON
sidecar summarizing every channel in one payload.

## Module layout

| Path | Role |
|---|---|
| `app/main.py` | Login, device/channel discovery, poll loop, watts conversion, Home Assistant Discovery payload assembly, MQTT publish |
| `app/requirements.txt` | `paho-mqtt`, `pyemvue` |
| `_shared/ha-mqtt-bridge-toolkit/` | Vendored at publish time — MQTT client, Discovery payload builders, topic/logging helpers |
| `_shared/python-github-error-reporter/` | Vendored at publish time — optional production-error reporting |
