"""The three resource profiles and the analytics URL matcher."""

import pytest

from scrapling.engines.toolbelt.resource_profiles import (
    ANALYTICS_URL_PATTERNS,
    FIDELITY_PROFILE,
    PROFILE_NAMES,
    RESOURCE_PROFILES,
    SPEED_PROFILE,
    is_analytics_url,
    should_block,
)


class TestProfiles:
    def test_speed_drops_the_visually_heavy_types(self):
        for resource_type in ("image", "font", "media", "stylesheet", "websocket"):
            assert resource_type in SPEED_PROFILE

    def test_fidelity_keeps_css_and_sockets_but_drops_tracking_cargo(self):
        assert "stylesheet" not in FIDELITY_PROFILE
        assert "websocket" not in FIDELITY_PROFILE
        assert "beacon" in FIDELITY_PROFILE
        assert "csp_report" in FIDELITY_PROFILE

    def test_none_blocks_nothing(self):
        assert RESOURCE_PROFILES["none"] == frozenset()
        assert RESOURCE_PROFILES == {"speed": SPEED_PROFILE, "fidelity": FIDELITY_PROFILE, "none": frozenset()}
        assert PROFILE_NAMES == ("none", "speed", "fidelity")

    def test_speed_is_strictly_wider_than_fidelity(self):
        assert FIDELITY_PROFILE < SPEED_PROFILE


class TestAnalyticsUrls:
    @pytest.mark.parametrize(
        "url",
        [
            "https://www.google-analytics.com/analytics.js",
            "https://example.com/gtag/js?id=G-XXX",  # first-party path, no host match
            "https://hm.baidu.com/hm.js?abc",
            "https://connect.facebook.net/en_US/fbevents.js",
            "https://static.hotjar.com/c/hotjar-123.js",
            "https://cdn.segment.com/analytics.js/v1/x/analytics.min.js",
            "https://mc.yandex.ru/metrika/tag.js",
            "https://example.com/GTAG/JS",
        ],
    )
    def test_analytics_urls_are_recognised(self, url):
        assert is_analytics_url(url) is True

    @pytest.mark.parametrize(
        "url",
        [
            "https://example.com/app.js",
            "https://example.com/styles/main.css",
            "https://example.com/api/v1/items",
            "https://example.com/analytics-dashboard",  # page, not the script
        ],
    )
    def test_ordinary_urls_are_not(self, url):
        assert is_analytics_url(url) is False

    def test_patterns_are_compiled_and_listed(self):
        assert len(ANALYTICS_URL_PATTERNS) == 28
        assert all(hasattr(p, "search") for p in ANALYTICS_URL_PATTERNS)


class TestShouldBlock:
    def test_speed_blocks_images(self):
        assert should_block("https://example.com/a.png", "image", resource_profile="speed") is True

    def test_fidelity_lets_images_through(self):
        assert should_block("https://example.com/a.png", "image", resource_profile="fidelity") is False

    def test_fidelity_still_blocks_beacons(self):
        assert should_block("https://example.com/b", "beacon", resource_profile="fidelity") is True

    def test_analytics_script_is_blocked_when_asked(self):
        url = "https://www.google-analytics.com/analytics.js"
        assert should_block(url, "script", block_analytics=True) is True

    def test_analytics_flag_does_not_block_ordinary_scripts(self):
        assert should_block("https://example.com/app.js", "script", block_analytics=True) is False

    def test_defaults_block_nothing(self):
        assert should_block("https://example.com/app.js", "script") is False
        assert should_block("https://example.com/app.js", "script", resource_profile="none", block_analytics=False) is False

    def test_analytics_works_without_any_profile(self):
        assert should_block("https://example.com/gtag/js", "script", resource_profile="none", block_analytics=True) is True

    def test_unknown_profile_is_treated_as_none(self):
        assert should_block("https://example.com/a.png", "image", resource_profile="turbo") is False
