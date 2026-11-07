"""The new knobs must reach the session config, and bad values must be rejected."""

import pytest

from scrapling.engines._browsers._stealth import AsyncStealthySession


class TestResourceProfileConfig:
    def test_speed_profile_is_accepted(self):
        session = AsyncStealthySession(resource_profile="speed")
        assert session._config.resource_profile == "speed"
        assert session._config.block_analytics is False

    def test_fidelity_profile_is_accepted(self):
        assert AsyncStealthySession(resource_profile="fidelity")._config.resource_profile == "fidelity"

    def test_an_unknown_profile_is_rejected(self):
        # `validate()` wraps msgspec's ValidationError, so the surfaced type is TypeError.
        with pytest.raises(TypeError, match="resource_profile must be one of none/speed/fidelity, got invalid"):
            AsyncStealthySession(resource_profile="invalid")

    def test_the_legacy_switch_is_not_rewritten_at_construction(self):
        # Compatibility lives in the route handler, not in the config: disable_resources=True
        # keeps meaning "speed" at call time, but the stored profile stays untouched.
        session = AsyncStealthySession(disable_resources=True)
        assert session._config.disable_resources is True
        assert session._config.resource_profile == "none"

    def test_analytics_blocking_is_accepted(self):
        session = AsyncStealthySession(block_analytics=True)
        assert session._config.block_analytics is True
        assert session._config.resource_profile == "none"

    def test_both_knobs_together(self):
        session = AsyncStealthySession(resource_profile="speed", block_analytics=True)
        assert (session._config.resource_profile, session._config.block_analytics) == ("speed", True)
