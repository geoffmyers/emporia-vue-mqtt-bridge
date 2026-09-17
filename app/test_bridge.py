"""Unit tests for the Emporia Vue bridge's channel-slug derivation, energy
accumulation, auth-error detection, and HA Discovery payloads — synthetic
fixtures only, no network access.

Mirrors govee-mqtt-bridge's `test_discovery_regression.py` layout: stub
out `pyemvue`/`paho`/`requests` (and point at the in-repo toolkit source)
so `main.py` imports cleanly without real credentials, the `pyemvue`
package, or the vendored toolkit being pip-installed. Run with::

    cd app
    EMPORIA_USERNAME=x EMPORIA_PASSWORD=x MQTT_PASSWORD=x \\
        python -m pytest test_bridge.py -q
"""

from __future__ import annotations

import os
import sys
import types
import unittest

_REQUIRED = {
    "EMPORIA_USERNAME": "x@example.com",
    "EMPORIA_PASSWORD": "test",
    "MQTT_PASSWORD": "test",
}
for k, v in _REQUIRED.items():
    os.environ.setdefault(k, v)


def _stub_module(name: str, **attrs: object) -> None:
    if name in sys.modules:
        return
    mod = types.ModuleType(name)
    for attr_name, attr_val in attrs.items():
        setattr(mod, attr_name, attr_val)
    sys.modules[name] = mod


class _FakeHTTPError(Exception):
    def __init__(self, response=None):
        super().__init__("http error")
        self.response = response


_stub_module("requests")
sys.modules["requests"].RequestException = Exception  # type: ignore[attr-defined]
sys.modules["requests"].HTTPError = _FakeHTTPError  # type: ignore[attr-defined]

_stub_module("paho")
_stub_module("paho.mqtt")
mqtt_stub = types.ModuleType("paho.mqtt.client")


class _FakeCallbackAPIVersion:
    VERSION2 = 2


class _FakeMqttClient:
    def __init__(self, *_, **__):
        pass


mqtt_stub.CallbackAPIVersion = _FakeCallbackAPIVersion
mqtt_stub.Client = _FakeMqttClient
sys.modules["paho.mqtt.client"] = mqtt_stub

_stub_module("pyemvue")
sys.modules["pyemvue"].PyEmVue = object  # type: ignore[attr-defined]
enums_stub = types.ModuleType("pyemvue.enums")


class _Scale:
    MINUTE = types.SimpleNamespace(value="1MIN")


class _Unit:
    KWH = types.SimpleNamespace(value="KWH")


enums_stub.Scale = _Scale
enums_stub.Unit = _Unit
sys.modules["pyemvue.enums"] = enums_stub

if "ha_mqtt_bridge" not in sys.modules:
    here = os.path.dirname(os.path.abspath(__file__))
    toolkit = os.path.normpath(
        os.path.join(here, "..", "..", "..", "_shared", "ha-mqtt-bridge-toolkit")
    )
    sys.path.insert(0, toolkit)

import main  # noqa: E402


# -------------------------------------------------------------- channel slug/display


class TestChannelSlug(unittest.TestCase):
    def test_mains_carve_out(self):
        self.assertEqual(main._channel_slug("1,2,3", None), "main")
        self.assertEqual(main._channel_display("1,2,3", None), "Main")

    def test_balance_carve_out(self):
        self.assertEqual(main._channel_slug("Balance", None), "balance")
        self.assertEqual(main._channel_display("Balance", None), "Balance")

    def test_strips_breaker_prefix(self):
        self.assertEqual(main._channel_slug("7", "7/9 - Dryer"), "dryer")
        self.assertEqual(main._channel_display("7", "7/9 - Dryer"), "Dryer")

    def test_falls_back_to_channel_number(self):
        self.assertEqual(main._channel_slug("12", None), "channel_12")
        self.assertEqual(main._channel_display("12", None), "Channel 12")

    def test_multi_word_name_lowercased_and_joined(self):
        self.assertEqual(main._channel_slug("3", "3 - Bedroom 2 & Office"), "bedroom_2_office")


# -------------------------------------------------------------- energy accumulation


class TestAccumulateEnergy(unittest.TestCase):
    def _device_with_channel(self, watts):
        ch = main.Channel(
            device_gid=1, channel_num="1", slug="main", display="Main",
            type=None, watts=watts,
        )
        return main.Device(device_gid=1, name="Vue", model="Vue2", firmware=None, channels=[ch]), ch

    def test_integrates_watts_over_elapsed_hours(self):
        dev, ch = self._device_with_channel(watts=1000.0)  # 1 kW
        main.accumulate_energy([dev], elapsed_hours=1.0)
        self.assertAlmostEqual(ch.energy_kwh, 1.0)
        # A second hour at the same power accumulates, doesn't overwrite.
        main.accumulate_energy([dev], elapsed_hours=1.0)
        self.assertAlmostEqual(ch.energy_kwh, 2.0)

    def test_zero_elapsed_is_noop(self):
        dev, ch = self._device_with_channel(watts=5000.0)
        main.accumulate_energy([dev], elapsed_hours=0.0)
        self.assertEqual(ch.energy_kwh, 0.0)

    def test_negative_elapsed_is_noop(self):
        # Clock skew / monotonic-clock edge case — must never go backward.
        dev, ch = self._device_with_channel(watts=5000.0)
        main.accumulate_energy([dev], elapsed_hours=-1.0)
        self.assertEqual(ch.energy_kwh, 0.0)

    def test_none_watts_skipped(self):
        dev, ch = self._device_with_channel(watts=None)
        main.accumulate_energy([dev], elapsed_hours=1.0)
        self.assertEqual(ch.energy_kwh, 0.0)


# -------------------------------------------------------------- auth-error detection


class _FakeResponse:
    def __init__(self, status_code):
        self.status_code = status_code


class _FakeVueNoAuthAttr:
    """A PyEmVue stand-in whose `.auth.cognito...` chain is absent, so
    `is_auth_error` must fall through to the substring check."""
    class auth:  # noqa: N801 - mirrors the real attribute-chain shape
        class cognito:
            class client:
                pass


class TestIsAuthError(unittest.TestCase):
    def test_http_401_is_auth_error(self):
        exc = main.requests.HTTPError(response=_FakeResponse(401))
        self.assertTrue(main.is_auth_error(_FakeVueNoAuthAttr(), exc))

    def test_http_500_is_not_auth_error(self):
        exc = main.requests.HTTPError(response=_FakeResponse(500))
        self.assertFalse(main.is_auth_error(_FakeVueNoAuthAttr(), exc))

    def test_string_fallback_not_authorized(self):
        exc = RuntimeError("NotAuthorized: token expired")
        self.assertTrue(main.is_auth_error(_FakeVueNoAuthAttr(), exc))

    def test_unrelated_error_is_not_auth_error(self):
        exc = ValueError("bad channel payload")
        self.assertFalse(main.is_auth_error(_FakeVueNoAuthAttr(), exc))


# -------------------------------------------------------------- discovery


class TestDiscoverySpecs(unittest.TestCase):
    def setUp(self):
        self.ch = main.Channel(
            device_gid=42, channel_num="7", slug="dryer", display="Dryer", type=None,
        )
        self.device = main.Device(
            device_gid=42, name="Vue", model="Vue2", firmware="1.2.3", channels=[self.ch],
        )

    def test_power_and_energy_entities_both_present(self):
        items = main.discovery_specs([self.device])
        unique_ids = {payload["unique_id"] for _, _, payload in items}
        self.assertIn("emporia_mqtt_bridge_42_dryer_power", unique_ids)
        self.assertIn("emporia_mqtt_bridge_42_dryer_energy", unique_ids)

    def test_energy_entity_is_ha_energy_dashboard_shaped(self):
        items = main.discovery_specs([self.device])
        energy_payload = next(
            p for _, _, p in items if p["unique_id"] == "emporia_mqtt_bridge_42_dryer_energy"
        )
        self.assertEqual(energy_payload["device_class"], "energy")
        self.assertEqual(energy_payload["state_class"], "total_increasing")
        self.assertEqual(energy_payload["unit_of_measurement"], "kWh")
        self.assertEqual(
            energy_payload["state_topic"],
            main.energy_state_topic(42, "dryer"),
        )

    def test_power_entity_unique_id_unchanged(self):
        # The energy sensor is additive — the pre-existing power sensor's
        # unique_id (and therefore its HA history) must be untouched.
        items = main.discovery_specs([self.device])
        power_payload = next(
            p for _, _, p in items if p["unique_id"] == "emporia_mqtt_bridge_42_dryer_power"
        )
        self.assertEqual(power_payload["device_class"], "power")
        self.assertEqual(power_payload["unit_of_measurement"], "W")


if __name__ == "__main__":
    unittest.main()
