"""Adaptive escalation state machine: risk, ladder, cooling, downgrade, races."""

import threading

import pytest

from scrapling.spiders.adaptive import AdaptiveConfig, AdaptiveEngine, DomainProfile, FetchLevel
from scrapling.spiders.presets import APT_PRESET
from scrapling.spiders.signals import BlockSignal

DOMAIN = "example.com"
CAPTCHA = (BlockSignal.CAPTCHA, 0.85)


class _Clock:
    """Monotonic stand-in so cooling can be crossed without sleeping."""

    def __init__(self, now: float = 1000.0):
        self.now = now

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


def _engine(**kwargs) -> tuple[AdaptiveEngine, _Clock]:
    clock = _Clock()
    return AdaptiveEngine(clock=clock, **kwargs), clock


class TestPlan:
    def test_initial_level_is_direct(self):
        engine, _ = _engine()
        assert engine.plan(DOMAIN) == (FetchLevel.DIRECT, 0.0, 10)

    def test_plan_creates_a_profile(self):
        engine, _ = _engine()
        engine.plan(DOMAIN)
        assert DOMAIN in engine.stats()

    def test_plan_is_read_only(self):
        engine, _ = _engine()
        engine.plan(DOMAIN)
        engine.plan(DOMAIN)
        assert engine.stats()[DOMAIN]["level"] == FetchLevel.DIRECT

    @pytest.mark.parametrize("level,delay,concurrency", [(1, 0.1, 8), (2, 0.3, 6), (3, 1.0, 4), (4, 2.0, 2)])
    def test_level_tables_are_wired(self, level, delay, concurrency):
        config = AdaptiveConfig()
        assert config.level_delays[level] == delay
        assert config.level_concurrency[level] == concurrency


class TestEscalation:
    def test_one_captcha_escalates_to_headers(self):
        engine, clock = _engine()
        engine.observe(DOMAIN, *CAPTCHA)

        state = engine.stats()[DOMAIN]
        assert state["level"] == FetchLevel.HEADERS
        assert state["risk"] == pytest.approx(25.5)  # 60 * 0.85 = 51, halved on upgrade
        assert state["cooling_until"] == pytest.approx(clock.now + 60.0)
        assert engine.plan(DOMAIN) == (FetchLevel.HEADERS, 0.1, 8)

    def test_three_captchas_reach_proxy(self):
        engine, _ = _engine()
        for _ in range(3):
            engine.observe(DOMAIN, *CAPTCHA)
        assert engine.stats()[DOMAIN]["level"] == FetchLevel.PROXY

    def test_four_captchas_reach_browser(self):
        engine, _ = _engine()
        for _ in range(4):
            engine.observe(DOMAIN, *CAPTCHA)
        assert engine.stats()[DOMAIN]["level"] == FetchLevel.BROWSER
        assert engine.plan(DOMAIN) == (FetchLevel.BROWSER, 2.0, 2)

    def test_browser_is_the_ceiling(self):
        engine, _ = _engine()
        for _ in range(12):
            engine.observe(DOMAIN, *CAPTCHA)
        assert engine.stats()[DOMAIN]["level"] == FetchLevel.BROWSER
        assert engine.stats()[DOMAIN]["risk"] <= 100.0

    def test_risk_is_halved_on_upgrade_only(self):
        engine, _ = _engine()
        engine.observe(DOMAIN, *CAPTCHA)
        assert engine.stats()[DOMAIN]["risk"] == pytest.approx(25.5)
        engine.observe(DOMAIN, *CAPTCHA)
        assert engine.stats()[DOMAIN]["risk"] == pytest.approx(38.25)  # (25.5 + 51) / 2

    def test_risk_is_capped(self):
        engine, _ = _engine()
        for _ in range(20):
            engine.observe(DOMAIN, BlockSignal.CHALLENGE, 1.0)
        assert engine.stats()[DOMAIN]["risk"] == pytest.approx(100.0)

    def test_low_confidence_is_floored_at_forty_percent(self):
        engine, _ = _engine()
        engine.observe(DOMAIN, BlockSignal.EMPTY, 0.0)
        assert engine.stats()[DOMAIN]["risk"] == pytest.approx(8.0)  # 20 * 0.4

    def test_none_signal_is_ignored(self):
        engine, _ = _engine()
        engine.observe(DOMAIN, BlockSignal.NONE, 0.9)
        assert engine.stats() == {}

    def test_below_threshold_does_not_escalate(self):
        engine, _ = _engine()
        engine.observe(DOMAIN, BlockSignal.EMPTY, 0.7)  # 14 < 30
        state = engine.stats()[DOMAIN]
        assert state["level"] == FetchLevel.DIRECT
        assert state["risk"] == pytest.approx(14.0)


class TestCooldownAndDowngrade:
    def test_no_downgrade_inside_cooldown(self):
        engine, clock = _engine()
        engine.observe(DOMAIN, *CAPTCHA)

        for _ in range(10):
            clock.advance(1.0)
            engine.observe_success(DOMAIN)

        state = engine.stats()[DOMAIN]
        assert state["level"] == FetchLevel.HEADERS
        assert state["success_streak"] == 10

    def test_downgrade_after_cooldown_and_streak(self):
        engine, clock = _engine()
        engine.observe(DOMAIN, *CAPTCHA)
        clock.advance(61.0)

        for _ in range(9):
            engine.observe_success(DOMAIN)
        assert engine.stats()[DOMAIN]["level"] == FetchLevel.HEADERS

        engine.observe_success(DOMAIN)
        state = engine.stats()[DOMAIN]
        assert state["level"] == FetchLevel.DIRECT
        assert state["success_streak"] == 0

    def test_downgrade_is_one_step_per_streak(self):
        engine, clock = _engine()
        for _ in range(4):
            engine.observe(DOMAIN, *CAPTCHA)
        assert engine.stats()[DOMAIN]["level"] == FetchLevel.BROWSER

        clock.advance(61.0)
        for _ in range(10):
            engine.observe_success(DOMAIN)

        state = engine.stats()[DOMAIN]
        assert state["level"] == FetchLevel.PROXY
        assert state["success_streak"] == 0

    def test_direct_never_downgrades(self):
        engine, clock = _engine()
        clock.advance(1000.0)
        for _ in range(50):
            engine.observe_success(DOMAIN)
        assert engine.stats()[DOMAIN]["level"] == FetchLevel.DIRECT


class TestIsolation:
    def test_domains_do_not_share_state(self):
        engine, _ = _engine()
        engine.observe("a.example", *CAPTCHA)
        engine.observe("a.example", *CAPTCHA)

        assert engine.stats()["a.example"]["level"] == FetchLevel.TLS
        assert engine.plan("b.example")[0] == FetchLevel.DIRECT
        assert "b.example" not in engine.stats() or engine.stats()["b.example"]["risk"] == 0.0

    def test_signal_counts_are_tracked_per_domain(self):
        engine, _ = _engine()
        engine.observe("a.example", *CAPTCHA)
        engine.observe("a.example", BlockSignal.FORBIDDEN, 0.9)
        assert engine.stats()["a.example"]["signal_counts"] == {"CAPTCHA": 1, "FORBIDDEN": 1}


class TestProxyFallback:
    def test_proxy_tier_collapses_when_no_proxy(self):
        engine, _ = _engine(proxy_available=lambda: False)
        for _ in range(3):
            engine.observe(DOMAIN, *CAPTCHA)

        assert engine.stats()[DOMAIN]["level"] == FetchLevel.TLS
        assert engine.plan(DOMAIN) == (FetchLevel.TLS, 0.3, 6)

    def test_plan_collapses_a_stored_proxy_level_when_the_pool_drains(self):
        available = {"ok": True}
        engine, _ = _engine(proxy_available=lambda: available["ok"])
        for _ in range(3):
            engine.observe(DOMAIN, *CAPTCHA)
        assert engine.plan(DOMAIN)[0] == FetchLevel.PROXY

        available["ok"] = False
        assert engine.plan(DOMAIN)[0] == FetchLevel.TLS

        available["ok"] = True
        assert engine.plan(DOMAIN)[0] == FetchLevel.PROXY


class TestThreadSafety:
    def test_concurrent_observe_is_consistent(self):
        engine, _ = _engine()

        def hammer():
            for _ in range(10):
                engine.observe(DOMAIN, *CAPTCHA)

        threads = [threading.Thread(target=hammer) for _ in range(10)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        state = engine.stats()[DOMAIN]
        assert state["signal_counts"] == {"CAPTCHA": 100}
        assert state["level"] == FetchLevel.BROWSER
        assert 0.0 < state["risk"] <= 100.0

    def test_concurrent_plan_and_success_do_not_deadlock(self):
        engine, clock = _engine()
        engine.observe(DOMAIN, *CAPTCHA)
        clock.advance(120.0)

        def caller():
            for _ in range(50):
                engine.plan(DOMAIN)
                engine.observe_success(DOMAIN)

        threads = [threading.Thread(target=caller) for _ in range(8)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        assert 0 <= engine.stats()[DOMAIN]["level"] <= FetchLevel.BROWSER


class TestSessionReset:
    def test_apt_ladder_is_three_to_proxy_four_to_browser(self):
        engine = AdaptiveEngine(APT_PRESET)
        for _ in range(3):
            engine.observe(DOMAIN, BlockSignal.CAPTCHA, 0.9)
        assert engine.stats()[DOMAIN]["level"] == FetchLevel.PROXY

        engine.observe(DOMAIN, BlockSignal.CAPTCHA, 0.9)
        assert engine.stats()[DOMAIN]["level"] == FetchLevel.BROWSER

    def test_fifth_captcha_at_browser_requests_a_reset(self):
        engine = AdaptiveEngine(APT_PRESET)
        for _ in range(4):
            engine.observe(DOMAIN, BlockSignal.CAPTCHA, 0.9)
        assert engine.should_reset(DOMAIN) is False

        engine.observe(DOMAIN, BlockSignal.CAPTCHA, 0.9)
        state = engine.stats()[DOMAIN]
        assert state["level"] == FetchLevel.BROWSER
        assert state["risk"] == pytest.approx(100.0)
        assert state["reset_pending"] is True
        assert engine.should_reset(DOMAIN) is True

    def test_reset_drops_back_to_direct_and_counts(self):
        engine = AdaptiveEngine(APT_PRESET)
        for _ in range(5):
            engine.observe(DOMAIN, BlockSignal.CAPTCHA, 0.9)

        engine.reset_session(DOMAIN)

        assert engine.should_reset(DOMAIN) is False
        assert engine.stats() == {}
        assert engine.plan(DOMAIN) == (FetchLevel.DIRECT, 0.0, 50)
        assert engine.stats()[DOMAIN]["reset_count"] == 1
        assert engine._reset_history[DOMAIN] == 1

    def test_reset_counts_accumulate_across_generations(self):
        engine = AdaptiveEngine(APT_PRESET)
        for _ in range(2):
            for _ in range(5):
                engine.observe(DOMAIN, BlockSignal.CAPTCHA, 0.9)
            engine.reset_session(DOMAIN)

        assert engine._reset_history[DOMAIN] == 2
        engine.plan(DOMAIN)  # re-creates the profile with the domain's reset history
        assert engine.stats()[DOMAIN]["reset_count"] == 2
        assert engine.stats()[DOMAIN]["level"] == FetchLevel.DIRECT

    def test_reset_pending_is_per_domain(self):
        engine = AdaptiveEngine(APT_PRESET)
        for _ in range(5):
            engine.observe("hot.example", BlockSignal.CAPTCHA, 0.9)
        engine.observe("cold.example", *CAPTCHA)

        assert engine.should_reset("hot.example") is True
        assert engine.should_reset("cold.example") is False
        assert engine.should_reset("never-seen.example") is False

    def test_unknown_domain_never_needs_a_reset(self):
        engine, _ = _engine()
        assert engine.should_reset("nobody.example") is False
        assert "nobody.example" not in engine.stats()

    def test_concurrent_resets_are_counted_exactly(self):
        engine = AdaptiveEngine(APT_PRESET)

        def hammer():
            engine.reset_session(DOMAIN)

        threads = [threading.Thread(target=hammer) for _ in range(10)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        assert engine._reset_history[DOMAIN] == 10
        engine.plan(DOMAIN)
        assert engine.stats()[DOMAIN]["level"] == FetchLevel.DIRECT
        assert engine.stats()[DOMAIN]["reset_count"] == 10


class TestConfig:
    def test_level_tables_must_match_the_ladder(self):
        with pytest.raises(ValueError):
            AdaptiveConfig(level_delays=(0.0, 1.0))

    def test_negative_cooldown_is_rejected(self):
        with pytest.raises(ValueError):
            AdaptiveConfig(cooldown_seconds=-1)

    def test_zero_success_streak_is_rejected(self):
        with pytest.raises(ValueError):
            AdaptiveConfig(downgrade_after_success=0)

    def test_profile_defaults(self):
        profile = DomainProfile("example.com")
        assert profile.level == FetchLevel.DIRECT
        assert profile.signal_counts["CAPTCHA"] == 0  # defaultdict semantics

    def test_stats_returns_a_snapshot(self):
        engine, _ = _engine()
        engine.observe(DOMAIN, *CAPTCHA)
        snapshot = engine.stats()
        snapshot[DOMAIN]["signal_counts"]["CAPTCHA"] = 99
        assert engine.stats()[DOMAIN]["signal_counts"]["CAPTCHA"] == 1


class TestStatsSurface:
    def test_stats_shape_is_stable(self):
        engine = AdaptiveEngine(APT_PRESET)
        engine.observe(DOMAIN, BlockSignal.CAPTCHA, 0.9)

        stats = engine.stats()[DOMAIN]

        assert set(stats) == {
            "level",
            "level_name",
            "risk",
            "cooling_until",
            "success_streak",
            "signal_counts",
            "reset_pending",
            "reset_count",
        }
        assert stats["level"] == int(FetchLevel.HEADERS)
        assert stats["level_name"] == "HEADERS"
        assert stats["signal_counts"]["CAPTCHA"] == 1
        assert stats["reset_count"] == 0

    def test_stats_after_reset_reports_the_history(self):
        engine = AdaptiveEngine(APT_PRESET)
        for _ in range(5):
            engine.observe(DOMAIN, BlockSignal.CAPTCHA, 0.9)
        assert engine.should_reset(DOMAIN) is True

        engine.reset_session(DOMAIN)
        assert DOMAIN not in engine.stats()  # profile is gone, the history is not

        assert engine.plan(DOMAIN)[0] == FetchLevel.DIRECT
        stats = engine.stats()[DOMAIN]
        assert stats["level"] == int(FetchLevel.DIRECT)
        assert stats["risk"] == 0.0
        assert stats["reset_count"] == 1
