<p align="center">
  <img src="docs/icon.svg" width="96" height="96" alt="Emporia Vue MQTT Bridge icon">
</p>

# Emporia Vue MQTT Bridge

<!-- BADGES:START -->
![Python 3.12](https://img.shields.io/badge/Python-3.12-3776ab?style=flat-square&logo=python)
[![Licence GPL-3.0-or-later](https://img.shields.io/badge/licence-GPL--3.0--or--later-blue?style=flat-square)](LICENSE.md)
[![PRs welcome](https://img.shields.io/badge/PRs-welcome-brightgreen?style=flat-square)](CONTRIBUTING.md)
<!-- BADGES:END -->

## Table of Contents

- [Description](#description)
- [Features](#features)
- [Requirements](#requirements)
- [Installation](#installation)
- [Usage](#usage)
- [Configuration](#configuration)
- [Architecture](#architecture)
- [Credits](#credits)
- [Contributing](#contributing)
- [License](#license)

## Description

A small Python daemon that polls an [Emporia Vue](https://emporiaenergy.com/)
energy monitor's cloud API for per-circuit power draw and republishes it over
MQTT with Home Assistant [MQTT Discovery](https://www.home-assistant.io/integrations/mqtt/#mqtt-discovery),
so every monitored circuit shows up as its own power sensor.

Home Assistant already has an official `emporia_vue` integration that polls
the same account. This bridge exists alongside it rather than replacing it:
it publishes the same per-circuit readings as plain MQTT topics, in parallel
under a separate device, so any MQTT consumer — Telegraf into a time-series
database, Node-RED, another home automation system — can subscribe without
going through Home Assistant's recorder at all. The two can run side by side
with no entity collision.

## Features

- Polls Emporia's cloud API for every device and circuit on your account and
  publishes instantaneous power (in watts) per circuit.
- Publishes Home Assistant MQTT Discovery configs automatically — no manual
  YAML.
- Runs alongside the official `emporia_vue` integration without entity-id
  collisions; entities live under their own `Emporia Bridge: <device name>`
  device.
- Publishes a JSON sidecar topic per device with every channel's reading in
  one payload per poll, for consumers that want a single subscription
  instead of one topic per circuit.
- Sanitizes Emporia's raw circuit names (which carry a leading breaker-slot
  number, e.g. `"7/9 - Dryer"`) into stable slugs (`dryer`) used
  in both the MQTT topic and the Home Assistant entity ID.
- Re-authenticates automatically on any auth-looking failure.

## Requirements

- **Docker** with the Compose plugin (Compose v2.17+, for `additional_contexts`)
- An MQTT broker reachable from the container, with Home Assistant's MQTT
  integration pointed at the same broker
- An [Emporia Vue](https://emporiaenergy.com/) energy monitor already set up
  and paired to an Emporia account (Vue 2, plus an optional expander panel
  for individual circuits)

## Installation

```bash
git clone https://github.com/geoffmyers/emporia-vue-mqtt-bridge.git
cd emporia-vue-mqtt-bridge

cp .env.example .env
cp docker-compose.example.yml docker-compose.yml
```

Edit `.env` with your Emporia account credentials and MQTT broker details
(see [Configuration](#configuration)), then build and start the bridge:

```bash
docker compose build
docker compose up -d
docker compose logs -f
```

The application code is bind-mounted from `./app`, so after the first build
a code change only needs a container restart, not a rebuild.

The image is also published on the GitHub Container Registry as
`ghcr.io/geoffmyers/emporia-vue-mqtt-bridge`, for `linux/amd64` and `linux/arm64`, with the
application code in it: `docker compose pull` fetches it instead of
building. The compose file still mounts `./app` over that copy, so the
code in your checkout is what runs.

## Usage

On startup, the bridge logs in, discovers every device and channel on your
Emporia account, publishes Home Assistant Discovery configs, and starts
polling every `POLL_INTERVAL` seconds (60 by default).

Each Emporia device (a Vue 2 controller, or an expander panel) becomes its
own Home Assistant device named `Emporia Bridge: <device name>`, with one
`sensor.<channel>_power` entity per circuit (`device_class: power`,
`unit_of_measurement: W`). The whole-house aggregate channel Emporia calls
`Mains` becomes the `main` slug, and its unmonitored-remainder channel
becomes `balance`.

A device also gets a `usage` topic (`emporia/<device_gid>/usage`) carrying a
single JSON payload per poll with every channel's current watts and
kWh-per-minute — useful if you want one MQTT subscription for a whole
device instead of one topic per circuit.

Renaming a circuit in the Emporia app changes its slug (and therefore its
Home Assistant entity ID) on the next bridge restart, since the slug is
derived from the circuit name.

## Configuration

Environment variables, set in `.env` (`.env.example` lists them all):

| Variable | Default | Description |
|---|---|---|
| `EMPORIA_USERNAME` | *(required)* | Emporia account email |
| `EMPORIA_PASSWORD` | *(required)* | Emporia account password |
| `MQTT_HOST` | `mosquitto` | MQTT broker hostname |
| `MQTT_PORT` | `1883` | MQTT broker port |
| `MQTT_USER` | *(empty)* | MQTT username |
| `MQTT_PASSWORD` | *(required)* | MQTT password |
| `POLL_INTERVAL` | `60` | Seconds between polls |
| `HA_DISCOVERY_PREFIX` | `homeassistant` | Home Assistant MQTT Discovery topic prefix |
| `MQTT_TOPIC_PREFIX` | `emporia` | Prefix for this bridge's own MQTT topics |
| `LOG_LEVEL` | `INFO` | Python log level |
| `GITHUB_ERROR_TOKEN` | *(unset)* | Optional. A GitHub token with `repo` scope; when set together with `GITHUB_REPO`, uncaught exceptions are filed as GitHub issues via `repository_dispatch` (see [Credits](#credits)) |
| `GITHUB_REPO` | *(unset)* | Optional. `owner/name` of the repo to file error reports against |
| `GITHUB_ERROR_ENVIRONMENT` | `production` | Optional. Environment label attached to filed error reports |

## Architecture

```
Emporia cloud API  ◄──poll──  emporia-vue-mqtt-bridge  ──publish──►  MQTT broker  ──►  Home Assistant
                                (Python, pyemvue)                     (mosquitto)      (MQTT Discovery)
```

See [ARCHITECTURE.md](ARCHITECTURE.md) for the auth flow, the watts
conversion and the channel-slug algorithm.

## Credits

- Talks to Emporia's cloud API through
  [`pyemvue`](https://github.com/magico13/PyEmVue), the same community
  library Home Assistant's own `emporia_vue` integration uses.
- MQTT client, Home Assistant Discovery payloads and topic/logging helpers
  come from this repository's own `ha-mqtt-bridge-toolkit` package, vendored
  in at `_shared/ha-mqtt-bridge-toolkit/` when this repository is published.
- Optional production-error reporting uses this repository's own
  `python-github-error-reporter` package, vendored in the same way at
  `_shared/python-github-error-reporter/`.
- The README icon is the [Font Awesome](https://fontawesome.com/) `bolt`
  glyph, used under [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/).
- This project is not affiliated with, endorsed by, or supported by Emporia
  Energy.

Written by Geoff Myers.

## Contributing

Bug reports and pull requests are welcome. See [CONTRIBUTING.md](CONTRIBUTING.md)
for setup, checks and how this repository is published.

## License

This program is free software: you can redistribute it and/or modify it under
the terms of the GNU General Public License as published by the Free Software
Foundation, either version 3 of the License, or (at your option) any later
version.

This program is distributed in the hope that it will be useful, but WITHOUT ANY
WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
PARTICULAR PURPOSE. See [LICENSE.md](LICENSE.md) for the full text of the GNU
General Public License.

SPDX-License-Identifier: `GPL-3.0-or-later`
