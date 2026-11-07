"""Tests for the retry policy layer: decision table, backoff, and block boundary.

Pure unit tests — no network, no sleeping (the RNG is seeded instead).
"""

import random
from types import SimpleNamespace

import pytest

from scrapling.engines.toolbelt.custom import Response
from scrapling.spiders.request import Request
from scrapling.spiders.retry import (
    RetryDecision,
    RetryEngine,
    RetryPolicy,
    attempt_of,
    has_proxy,
    make_retry,
)
from scrapling.spiders.spider import Spider


class _BlockProbe(Spider):
    """Minimal concrete spider, used only to exercise `is_blocked`."""

    name = "retry_block_probe"

    async def parse(self, response):
        yield None


def _engine(**overrides) -> RetryEngine:
    """Engine with deterministic timing: no jitter unless a test asks for it."""
    overrides.setdefault("jitter", 0.0)
    return RetryEngine(RetryPolicy(**overrides), rng=random.Random(1234))


def _request(attempt: int = 0, proxy: str | None = None) -> Request:
    kwargs = {"proxy": proxy} if proxy else {}
    return Request("https://example.com/page", _retry_count=attempt, **kwargs)


def _response(status: int, headers: dict | None = None) -> Response:
    return Response(
        "https://example.com/page",
        "<html><body>probe</body></html>",
        status,
        "probe",
        {},
        headers or {},
        {},
        _log=False,
    )


class TestPolicyDefaults:
    def test_defaults_come_from_the_single_table(self):
        policy = RetryPolicy()
        assert policy.max_attempts == 5
        assert policy.base_delay == 0.5
        assert policy.max_delay == 120.0
        assert policy.jitter == 0.4
        assert policy.max_retry_after == 300.0
        assert 429 in policy.retry_on_status
        assert 403 in policy.no_retry_on_status

    @pytest.mark.parametrize(
        "kwargs",
        [
            {"max_attempts": -1},
            {"base_delay": 0},
            {"max_delay": 0},
            {"jitter": 1.5},
            {"max_retry_after": -1},
        ],
    )
    def test_invalid_policies_are_rejected(self, kwargs):
        with pytest.raises(ValueError):
            RetryPolicy(**kwargs)


class TestDecide:
    def test_5xx_on_first_attempt_gets_high_priority(self):
        decision, delay = _engine().decide(_request(0), _response(500))
        assert decision is RetryDecision.RETRY_HIGH_PRIORITY
        assert delay > 0

    def test_5xx_on_later_attempt_keeps_priority(self):
        decision, _ = _engine().decide(_request(1), _response(500))
        assert decision is RetryDecision.RETRY_SAME_PRIORITY

    def test_other_5xx_codes_are_covered_too(self):
        engine = _engine()
        for status in (502, 503, 504, 522, 524):
            assert engine.decide(_request(0), _response(status))[0] is RetryDecision.RETRY_HIGH_PRIORITY

    def test_429_without_proxy_gets_high_priority(self):
        decision, _ = _engine().decide(_request(0), _response(429))
        assert decision is RetryDecision.RETRY_HIGH_PRIORITY

    def test_429_with_proxy_rotates_the_proxy(self):
        request = _request(0, proxy="http://127.0.0.1:8080")
        decision, _ = _engine().decide(request, _response(429))
        assert decision is RetryDecision.RETRY_WITH_NEW_PROXY

    def test_hard_client_errors_give_up(self):
        engine = _engine()
        for status in (400, 401, 403, 404, 405, 410, 422):
            assert engine.decide(_request(0), _response(status))[0] is RetryDecision.GIVE_UP

    def test_unknown_status_gives_up(self):
        assert _engine().decide(_request(0), _response(409))[0] is RetryDecision.GIVE_UP

    def test_budget_exhausted_gives_up(self):
        decision, delay = _engine().decide(_request(5), _response(500))
        assert decision is RetryDecision.GIVE_UP
        assert delay == 0.0

    def test_timeout_retries_then_respects_the_budget(self):
        engine = _engine()
        assert engine.decide(_request(0), exc=TimeoutError())[0] is RetryDecision.RETRY_SAME_PRIORITY
        assert engine.decide(_request(4), exc=ConnectionError())[0] is RetryDecision.RETRY_SAME_PRIORITY
        assert engine.decide(_request(5), exc=TimeoutError())[0] is RetryDecision.GIVE_UP

    def test_non_retryable_exception_gives_up(self):
        assert _engine().decide(_request(0), exc=ValueError("boom"))[0] is RetryDecision.GIVE_UP

    def test_response_missing_gives_up(self):
        assert _engine().decide(_request(0))[0] is RetryDecision.GIVE_UP


class TestNextDelay:
    def test_exponential_steps(self):
        engine = _engine(base_delay=0.5, max_delay=120.0)
        assert [engine.next_delay(i) for i in range(3)] == [0.5, 1.0, 2.0]

    def test_cap_is_applied_after_jitter(self):
        policy = RetryPolicy(base_delay=1.0, max_delay=120.0, jitter=0.5)
        worst = max(
            RetryEngine(policy, rng=random.Random(seed)).next_delay(10) for seed in range(200)
        )
        assert worst <= 120.0
        assert worst > 0.0

    def test_jitter_spreads_within_the_ratio(self):
        policy = RetryPolicy(base_delay=1.0, max_delay=120.0, jitter=0.5)
        values = [RetryEngine(policy, rng=random.Random(seed)).next_delay(0) for seed in range(200)]
        assert min(values) >= 0.5
        assert max(values) <= 1.5

    def test_floor_is_one_tenth(self):
        engine = _engine(base_delay=0.0001)
        assert engine.next_delay(0) == pytest.approx(0.1)

    def test_retry_after_is_honoured(self):
        delay = _engine(base_delay=0.5).next_delay(3, retry_after=5)
        assert 5.0 <= delay <= 5.25

    def test_retry_after_above_ceiling_is_ignored(self):
        delay = _engine(base_delay=0.5, max_retry_after=300.0).next_delay(0, retry_after=500)
        assert delay == pytest.approx(0.5)

    def test_retry_after_is_capped_by_max_delay(self):
        delay = _engine(base_delay=0.5, max_delay=120.0, max_retry_after=300.0).next_delay(0, retry_after=200)
        assert 120.0 <= delay <= 120.25

    def test_retry_after_header_is_parsed(self):
        engine = _engine(base_delay=0.5)
        decision, delay = engine.decide(_request(0), _response(503, {"Retry-After": "7"}))
        assert decision is RetryDecision.RETRY_HIGH_PRIORITY
        assert 7.0 <= delay <= 7.25


class TestRequestAdapters:
    def test_attempt_of_reads_the_private_field_only(self):
        assert attempt_of(_request(3)) == 3
        # A public `retry_count` is not the field Request defines; it must not be read,
        # otherwise the counter would be silently dropped by Request.copy().
        assert attempt_of(SimpleNamespace(retry_count=3)) == 0
        assert attempt_of(SimpleNamespace()) == 0

    def test_make_retry_does_not_bump_the_counter(self):
        request = _request(2)
        retry = make_retry(request, RetryDecision.RETRY_SAME_PRIORITY)
        assert retry._retry_count == 2
        assert request._retry_count == 2

    def test_make_retry_sets_dont_filter(self):
        retry = make_retry(_request(), RetryDecision.RETRY_SAME_PRIORITY)
        assert retry.dont_filter is True

    def test_make_retry_lowers_priority(self):
        retry = make_retry(_request(), RetryDecision.RETRY_HIGH_PRIORITY)
        assert retry.priority == -1

    def test_make_retry_strips_proxy_without_touching_the_original(self):
        request = _request(0, proxy="http://127.0.0.1:8080")
        assert has_proxy(request) is True
        retry = make_retry(request, RetryDecision.RETRY_WITH_NEW_PROXY)
        assert has_proxy(retry) is False
        assert has_proxy(request) is True

    def test_make_retry_refuses_give_up(self):
        with pytest.raises(ValueError):
            make_retry(_request(), RetryDecision.GIVE_UP)


class TestBlockedBoundary:
    """429 and 5xx are transient, so they must not enter the blocked-retry path."""

    @pytest.mark.asyncio
    async def test_transient_statuses_are_not_blocked(self):
        spider = _BlockProbe()
        for status in (429, 500, 502, 503, 504):
            assert await spider.is_blocked(_response(status)) is False

    @pytest.mark.asyncio
    async def test_hard_blocks_stay_blocked(self):
        spider = _BlockProbe()
        for status in (401, 403, 407, 444):
            assert await spider.is_blocked(_response(status)) is True
