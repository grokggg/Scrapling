"""Presets: the two profiles must stay wired to the thresholds they advertise.

Arithmetic at `upgrade_at_risk=80`: one CAPTCHA (60 * 0.9 = 54) stays quiet, two (108)
cross the line. At APT's 20 both a CAPTCHA and a FORBIDDEN (49.5) escalate on the first
signal, which is the point of that profile. The tests pin both boundaries.
"""

import pytest

from scrapling.spiders.adaptive import AdaptiveConfig, AdaptiveEngine, FetchLevel
from scrapling.spiders.presets import APT_PRESET, ENGINEERING_PRESET
from scrapling.spiders.signals import BlockSignal

DOMAIN = "example.com"


def _level_after(signal: BlockSignal, confidence: float, config: AdaptiveConfig) -> FetchLevel:
    engine = AdaptiveEngine(config)
    engine.observe(DOMAIN, signal, confidence)
    return FetchLevel(engine.stats()[DOMAIN]["level"])


class TestThresholds:
    def test_engineering_escalates_late(self):
        assert ENGINEERING_PRESET.upgrade_at_risk == 80.0
        assert ENGINEERING_PRESET.downgrade_after_success == 15
        assert ENGINEERING_PRESET.cooldown_seconds == 120.0

    def test_apt_escalates_early(self):
        assert APT_PRESET.upgrade_at_risk == 20.0
        assert APT_PRESET.downgrade_after_success == 30
        assert APT_PRESET.cooldown_seconds == 300.0

    def test_both_are_valid_configs(self):
        for preset in (ENGINEERING_PRESET, APT_PRESET):
            assert isinstance(preset, AdaptiveConfig)
            assert len(preset.level_delays) == len(FetchLevel)
            assert len(preset.level_concurrency) == len(FetchLevel)
            assert preset.level_delays[0] == 0.0

    def test_tables_are_monotonic_by_level(self):
        for preset in (ENGINEERING_PRESET, APT_PRESET):
            assert list(preset.level_delays) == sorted(preset.level_delays)
            assert list(preset.level_concurrency) == sorted(preset.level_concurrency, reverse=True)


class TestOneSignal:
    def test_apt_escalates_on_a_single_captcha(self):
        assert _level_after(BlockSignal.CAPTCHA, 0.9, APT_PRESET) == FetchLevel.HEADERS

    def test_engineering_holds_a_single_ratelimit(self):
        assert _level_after(BlockSignal.RATELIMIT, 0.95, ENGINEERING_PRESET) == FetchLevel.DIRECT

    def test_engineering_holds_a_single_forbidden(self):
        assert _level_after(BlockSignal.FORBIDDEN, 0.9, ENGINEERING_PRESET) == FetchLevel.DIRECT

    def test_engineering_holds_a_single_captcha(self):
        # 60 * 0.9 = 54 < 80: one CAPTCHA is below the engineering line.
        assert _level_after(BlockSignal.CAPTCHA, 0.9, ENGINEERING_PRESET) == FetchLevel.DIRECT

    def test_engineering_escalates_on_two_captchas(self):
        engine = AdaptiveEngine(ENGINEERING_PRESET)
        engine.observe(DOMAIN, BlockSignal.CAPTCHA, 0.9)
        assert engine.stats()[DOMAIN]["risk"] == pytest.approx(54.0)

        engine.observe(DOMAIN, BlockSignal.CAPTCHA, 0.9)  # 108 -> capped at 100, then halved
        assert engine.stats()[DOMAIN]["level"] == FetchLevel.HEADERS
        assert engine.stats()[DOMAIN]["risk"] == pytest.approx(50.0)

    def test_apt_escalates_on_a_single_forbidden(self):
        assert _level_after(BlockSignal.FORBIDDEN, 0.9, APT_PRESET) == FetchLevel.HEADERS

    def test_apt_preset_is_untouched(self):
        assert APT_PRESET.upgrade_at_risk == 20.0

        engine = AdaptiveEngine(APT_PRESET)
        for _ in range(3):
            engine.observe(DOMAIN, BlockSignal.CAPTCHA, 0.9)
        assert engine.stats()[DOMAIN]["level"] == FetchLevel.PROXY

        engine.observe(DOMAIN, BlockSignal.CAPTCHA, 0.9)
        assert engine.stats()[DOMAIN]["level"] == FetchLevel.BROWSER


class TestAccumulation:
    def test_engineering_needs_six_empty_pages(self):
        engine = AdaptiveEngine(ENGINEERING_PRESET)
        for _ in range(5):
            engine.observe(DOMAIN, BlockSignal.EMPTY, 0.7)  # 14 each
        assert engine.stats()[DOMAIN]["level"] == FetchLevel.DIRECT
        assert engine.stats()[DOMAIN]["risk"] == pytest.approx(70.0)

        engine.observe(DOMAIN, BlockSignal.EMPTY, 0.7)  # 84 >= 80
        assert engine.stats()[DOMAIN]["level"] == FetchLevel.HEADERS

    def test_apt_ladder_is_the_same_ladder(self):
        engine = AdaptiveEngine(APT_PRESET)
        for _ in range(5):
            engine.observe(DOMAIN, BlockSignal.CHALLENGE, 0.9)
        assert engine.stats()[DOMAIN]["level"] == FetchLevel.BROWSER

    def test_presets_plan_tables_are_used(self):
        engine = AdaptiveEngine(APT_PRESET)
        assert engine.plan(DOMAIN) == (FetchLevel.DIRECT, 0.0, 50)
