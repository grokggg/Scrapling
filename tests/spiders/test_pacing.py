"""Pacing: distribution shape, burst pauses, hour multipliers, rotation, thread safety."""

import threading
from datetime import datetime
from random import Random

import pytest

from scrapling.spiders.pacing import PacingConfig, PacingController


def _controller(**kwargs) -> PacingController:
    kwargs.setdefault("minute_jitter", 0.0)
    return PacingController(PacingConfig(**kwargs), rng=Random(7))


class TestDistributions:
    def test_fixed_stays_inside_the_jitter_band(self):
        pacing = _controller(base_delay=1.0, jitter_ratio=0.2, distribution="fixed")

        samples = [pacing.next_delay("a.com") for _ in range(100)]

        assert all(0.8 <= value <= 1.2 for value in samples)

    def test_fixed_without_jitter_is_a_constant(self):
        pacing = _controller(base_delay=1.5, jitter_ratio=0.0, distribution="fixed")

        assert {pacing.next_delay("a.com") for _ in range(20)} == {1.5}

    def test_uniform_spreads_over_the_band(self):
        pacing = _controller(base_delay=1.0, jitter_ratio=0.5, distribution="uniform")

        samples = [pacing.next_delay("a.com") for _ in range(100)]

        assert len(set(samples)) >= 5
        assert all(0.5 <= value <= 1.5 for value in samples)

    def test_lognormal_is_right_skewed_around_the_base(self):
        pacing = _controller(base_delay=1.0, distribution="lognormal", distribution_params={"sigma": 0.5})

        samples = sorted(pacing.next_delay("a.com") for _ in range(100))

        assert samples[9] < 1.0 < samples[89]

    def test_pareto_has_a_tail(self):
        pacing = _controller(base_delay=1.0, distribution="pareto", distribution_params={"alpha": 1.5})

        samples = [pacing.next_delay("a.com") for _ in range(100)]

        assert max(samples) > 5.0

    def test_unknown_distribution_is_loud(self):
        pacing = _controller(distribution="cauchy")

        with pytest.raises(ValueError):
            pacing.next_delay("a.com")


class TestHourShape:
    def test_hour_multiplier_scales_the_delay(self):
        multipliers = tuple(0.5 if hour == 3 else 1.0 for hour in range(24))
        pacing = PacingController(
            PacingConfig(
                base_delay=1.0,
                jitter_ratio=0.0,
                distribution="fixed",
                minute_jitter=0.0,
                hour_multipliers=multipliers,
            ),
            clock=lambda: datetime(2026, 11, 7, 3, 0),
        )

        assert pacing.next_delay("a.com") == pytest.approx(0.5)

    def test_daytime_is_not_slowed_down(self):
        multipliers = tuple(1.5 if 8 <= hour < 20 else 0.5 for hour in range(24))
        pacing = PacingController(
            PacingConfig(base_delay=2.0, jitter_ratio=0.0, distribution="fixed", minute_jitter=0.0,
                         hour_multipliers=multipliers),
            clock=lambda: datetime(2026, 11, 7, 14, 0),
        )

        assert pacing.next_delay("a.com") == pytest.approx(3.0)

    def test_a_short_multiplier_table_is_safe(self):
        pacing = PacingController(
            PacingConfig(base_delay=1.0, jitter_ratio=0.0, distribution="fixed", minute_jitter=0.0,
                         hour_multipliers=(0.5,)),
            clock=lambda: datetime(2026, 11, 7, 23, 0),
        )

        assert pacing.next_delay("a.com") == pytest.approx(1.0)


class TestBurst:
    def test_the_pause_lands_after_burst_size_intervals(self):
        pacing = _controller(base_delay=0.1, jitter_ratio=0.0, distribution="fixed",
                             burst_size=5, burst_pause_min=2.0, burst_pause_max=8.0)

        first_five = [pacing.next_delay("a.com") for _ in range(5)]
        pause = pacing.next_delay("a.com")

        assert all(value == pytest.approx(0.1) for value in first_five)
        assert 2.0 <= pause <= 8.0

    def test_the_burst_counter_resets_after_the_pause(self):
        pacing = _controller(base_delay=0.1, jitter_ratio=0.0, distribution="fixed",
                             burst_size=3, burst_pause_min=2.0, burst_pause_max=2.0)

        [pacing.next_delay("a.com") for _ in range(3)]
        assert pacing.next_delay("a.com") == pytest.approx(2.0)

        assert pacing.next_delay("a.com") == pytest.approx(0.1)

    def test_burst_size_zero_never_pauses(self):
        pacing = _controller(base_delay=0.5, jitter_ratio=0.0, distribution="fixed")

        assert {pacing.next_delay("a.com") for _ in range(50)} == {0.5}

    def test_each_domain_has_its_own_burst_budget(self):
        pacing = _controller(base_delay=0.1, jitter_ratio=0.0, distribution="fixed",
                             burst_size=2, burst_pause_min=5.0, burst_pause_max=5.0)

        pacing.next_delay("a.com")
        pacing.next_delay("a.com")
        assert pacing.next_delay("b.com") == pytest.approx(0.1)
        assert pacing.next_delay("a.com") == pytest.approx(5.0)


class TestRotation:
    def test_rotation_fires_every_n_requests(self):
        pacing = _controller(rotate_session_every=10)

        for _ in range(9):
            pacing.record_request("a.com")
            assert pacing.should_rotate("a.com") is False

        pacing.record_request("a.com")

        assert pacing.should_rotate("a.com") is True

    def test_mark_rotated_clears_the_counter(self):
        pacing = _controller(rotate_session_every=3)
        for _ in range(3):
            pacing.record_request("a.com")

        pacing.mark_rotated("a.com")

        assert pacing.should_rotate("a.com") is False
        pacing.record_request("a.com")
        assert pacing.should_rotate("a.com") is False

    def test_rotation_is_off_by_default(self):
        pacing = _controller()

        for _ in range(100):
            pacing.record_request("a.com")

        assert pacing.should_rotate("a.com") is False

    def test_rotation_is_per_domain(self):
        pacing = _controller(rotate_session_every=2)
        pacing.record_request("a.com")
        pacing.record_request("a.com")

        assert pacing.should_rotate("a.com") is True
        assert pacing.should_rotate("b.com") is False


class TestDomainOverrides:
    def test_a_domain_override_wins_over_the_base_config(self):
        slow = PacingConfig(base_delay=5.0, jitter_ratio=0.0, distribution="fixed", minute_jitter=0.0)
        pacing = PacingController(
            PacingConfig(base_delay=1.0, jitter_ratio=0.0, distribution="fixed", minute_jitter=0.0,
                         domain_overrides={"example.com": slow})
        )

        assert pacing.next_delay("example.com") == pytest.approx(5.0)
        assert pacing.next_delay("other.com") == pytest.approx(1.0)

    def test_an_override_can_carry_its_own_burst(self):
        bursty = PacingConfig(base_delay=0.1, jitter_ratio=0.0, distribution="fixed", minute_jitter=0.0,
                              burst_size=1, burst_pause_min=4.0, burst_pause_max=4.0)
        pacing = PacingController(
            PacingConfig(base_delay=0.1, jitter_ratio=0.0, distribution="fixed", minute_jitter=0.0,
                         domain_overrides={"a.com": bursty})
        )

        pacing.next_delay("a.com")

        assert pacing.next_delay("a.com") == pytest.approx(4.0)
        assert pacing.next_delay("b.com") == pytest.approx(0.1)


class TestConcurrency:
    def test_parallel_recording_does_not_lose_counts(self):
        pacing = _controller(rotate_session_every=10_000)

        def worker():
            for _ in range(100):
                pacing.record_request("a.com")

        threads = [threading.Thread(target=worker) for _ in range(10)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        assert pacing.stats()["a.com"]["count"] == 1000

    def test_parallel_delay_draws_keep_the_burst_budget(self):
        pacing = _controller(base_delay=0.0, jitter_ratio=0.0, distribution="fixed",
                             burst_size=50, burst_pause_min=1.0, burst_pause_max=1.0)

        def worker():
            for _ in range(10):
                pacing.next_delay("a.com")

        threads = [threading.Thread(target=worker) for _ in range(5)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        # 50 draws happened, so the next one must be the pause
        assert pacing.next_delay("a.com") == pytest.approx(1.0)


class TestStats:
    def test_stats_reports_the_shape_the_engine_reads(self):
        pacing = _controller(base_delay=1.0, jitter_ratio=0.0, distribution="fixed")

        pacing.record_request("a.com")
        pacing.record_request("a.com")
        pacing.next_delay("a.com")

        snapshot = pacing.stats()

        assert set(snapshot) == {"a.com"}
        assert snapshot["a.com"]["count"] == 2
        assert snapshot["a.com"]["next_delay_estimate"] == pytest.approx(1.0)
        assert snapshot["a.com"]["in_burst_pause"] is False

    def test_stats_flags_a_pending_burst_pause(self):
        pacing = _controller(base_delay=0.1, jitter_ratio=0.0, distribution="fixed",
                             burst_size=2, burst_pause_min=3.0, burst_pause_max=3.0)

        pacing.next_delay("a.com")
        pacing.next_delay("a.com")

        assert pacing.stats()["a.com"]["in_burst_pause"] is False  # pause not emitted yet
        pacing.next_delay("a.com")
        assert pacing.stats()["a.com"]["in_burst_pause"] is True

    def test_stats_is_empty_before_any_work(self):
        assert _controller().stats() == {}
