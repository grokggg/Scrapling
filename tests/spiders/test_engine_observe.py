"""Engine observe loop: every response feeds the ladder, and a dead ladder resets itself."""

import pytest

from scrapling.engines._browsers._stealth import FetchFailure
from scrapling.engines.toolbelt.custom import Response
from scrapling.spiders.adaptive import AdaptiveEngine, FetchLevel
from scrapling.spiders.engine import CrawlerEngine
from scrapling.spiders.presets import APT_PRESET, ENGINEERING_PRESET
from scrapling.spiders.request import Request
from scrapling.spiders.retry import RetryPolicy
from scrapling.spiders.spider import Spider

DOMAIN = "example.com"
OTHER = "other.com"
CAPTCHA = b"<html><title>Just a moment...</title> checking your browser cf-chl</html>"
LONG = b"<html><body>" + b"content " * 200 + b"</body></html>"


class _Spider(Spider):
    name = "observe"
    start_urls = [f"https://{DOMAIN}/"]

    async def parse(self, response):
        yield {"url": response.url}


def _response(url: str = f"https://{DOMAIN}/", body: bytes = LONG, status: int = 200) -> Response:
    return Response(url, body, status, "probe", {}, {}, {}, _log=False)


def _engine(preset=APT_PRESET) -> CrawlerEngine:
    spider = _Spider()
    engine = CrawlerEngine(spider, spider._session_manager)
    engine._adaptive_engine = AdaptiveEngine(preset)
    return engine


def _serving(engine: CrawlerEngine, body: bytes = LONG, status: int = 200, sids: list | None = None, other_body=None):
    async def fake_fetch(request):
        if sids is not None:
            sids.append(request.sid)
        served = other_body if (other_body is not None and request.domain == OTHER) else body
        return _response(request.url, served, status)

    engine.session_manager.fetch = fake_fetch


class TestObserveOnResponse:
    @pytest.mark.asyncio
    async def test_clean_response_builds_a_success_streak(self):
        engine = _engine()
        _serving(engine)

        await engine._process_request(Request(f"https://{DOMAIN}/a"))

        stats = engine._adaptive_engine.stats()[DOMAIN]
        assert stats["success_streak"] == 1
        assert stats["risk"] == 0.0
        assert stats["level"] == int(FetchLevel.DIRECT)

    @pytest.mark.asyncio
    async def test_short_body_counts_as_empty(self):
        engine = _engine(ENGINEERING_PRESET)
        _serving(engine, body=b"<html></html>")

        await engine._process_request(Request(f"https://{DOMAIN}/a"))

        stats = engine._adaptive_engine.stats()[DOMAIN]
        assert stats["signal_counts"]["EMPTY"] == 1
        assert stats["risk"] == pytest.approx(14.0)  # 20 * 0.7
        assert stats["level"] == int(FetchLevel.DIRECT)

    @pytest.mark.asyncio
    async def test_forbidden_escalates_on_the_first_hit_under_apt(self):
        engine = _engine()
        _serving(engine, status=403)

        await engine._process_request(Request(f"https://{DOMAIN}/a"))

        stats = engine._adaptive_engine.stats()[DOMAIN]
        assert stats["signal_counts"]["FORBIDDEN"] == 1
        assert stats["risk"] == pytest.approx(24.75)  # 55 * 0.9, halved by the upgrade
        assert stats["level"] == int(FetchLevel.HEADERS)

    @pytest.mark.asyncio
    async def test_challenge_escalates_once(self):
        engine = _engine()
        _serving(engine, body=CAPTCHA)

        await engine._process_request(Request(f"https://{DOMAIN}/a"))

        stats = engine._adaptive_engine.stats()[DOMAIN]
        assert stats["signal_counts"]["CHALLENGE"] == 1
        assert stats["level"] == int(FetchLevel.HEADERS)

    @pytest.mark.asyncio
    async def test_engineering_holds_a_single_captcha(self):
        engine = _engine(ENGINEERING_PRESET)
        _serving(engine, body=CAPTCHA)

        await engine._process_request(Request(f"https://{DOMAIN}/a"))

        stats = engine._adaptive_engine.stats()[DOMAIN]
        assert stats["risk"] == pytest.approx(63.0)  # 70 * 0.9, below the 80 threshold
        assert stats["level"] == int(FetchLevel.DIRECT)


class TestObserveOnException:
    @pytest.mark.asyncio
    async def test_timeout_accumulates_low_weight_risk_without_escalating(self):
        engine = _engine()
        engine.spider.retry_policy = RetryPolicy(max_attempts=0)

        async def boom(request):
            raise TimeoutError("timed out")

        engine.session_manager.fetch = boom

        await engine._process_request(Request(f"https://{DOMAIN}/a"))

        stats = engine._adaptive_engine.stats()[DOMAIN]
        assert stats["signal_counts"]["EMPTY"] == 1
        assert stats["risk"] == pytest.approx(8.0)  # 20 * max(0.4, 0.3)
        assert stats["level"] == int(FetchLevel.DIRECT)

    @pytest.mark.asyncio
    async def test_repeated_timeouts_add_up(self):
        engine = _engine(APT_PRESET)
        engine.spider.retry_policy = RetryPolicy(max_attempts=0)

        async def boom(request):
            raise TimeoutError("timed out")

        engine.session_manager.fetch = boom
        for _ in range(3):
            await engine._process_request(Request(f"https://{DOMAIN}/a"))

        stats = engine._adaptive_engine.stats()[DOMAIN]
        assert stats["signal_counts"]["EMPTY"] == 3
        assert stats["risk"] == pytest.approx(12.0)  # 3 * 8 = 24, halved by the upgrade
        assert stats["level"] == int(FetchLevel.HEADERS)


class TestSessionReset:
    @pytest.mark.asyncio
    async def test_fifth_captcha_at_browser_marks_the_domain_for_reset(self):
        engine = _engine()
        _serving(engine, body=CAPTCHA)

        for _ in range(4):
            await engine._process_request(Request(f"https://{DOMAIN}/a"))

        assert engine._adaptive_engine.plan(DOMAIN)[0] == FetchLevel.BROWSER
        assert engine._adaptive_engine.should_reset(DOMAIN) is False

        await engine._process_request(Request(f"https://{DOMAIN}/b"))

        assert engine._adaptive_engine.should_reset(DOMAIN) is True
        assert engine._adaptive_engine.stats()[DOMAIN]["reset_pending"] is True

    @pytest.mark.asyncio
    async def test_next_request_resets_every_session_and_climbs_from_direct(self):
        engine = _engine()
        sids: list = []
        _serving(engine, body=CAPTCHA, sids=sids)

        for _ in range(5):
            await engine._process_request(Request(f"https://{DOMAIN}/a"))
        assert engine._adaptive_engine.should_reset(DOMAIN) is True

        await engine._process_request(Request(f"https://{DOMAIN}/after-reset"))

        # The reset request itself ran, on the fresh DIRECT rung.
        assert sids[-1] == f"static:0:{DOMAIN}"
        assert engine._adaptive_engine.stats()[DOMAIN]["reset_count"] == 1
        assert engine._adaptive_engine.should_reset(DOMAIN) is False
        # Nothing from the previous ladder survived.
        assert [sid for sid in engine.session_manager.session_ids if sid.endswith(DOMAIN)] == [f"static:0:{DOMAIN}"]

    @pytest.mark.asyncio
    async def test_reset_does_not_drop_the_in_flight_request(self):
        engine = _engine()
        for _ in range(5):
            engine._adaptive_engine.observe(DOMAIN, __import__("scrapling.spiders.signals", fromlist=["BlockSignal"]).BlockSignal.CAPTCHA, 0.9)
        assert engine._adaptive_engine.should_reset(DOMAIN) is True

        sids: list = []
        _serving(engine, sids=sids)
        request = Request(f"https://{DOMAIN}/keep-me")
        await engine._process_request(request)

        assert sids == [f"static:0:{DOMAIN}"]
        assert engine.stats.items_scraped == 1
        assert engine.stats.failed_requests_count == 0

    @pytest.mark.asyncio
    async def test_reset_only_touches_the_domain_that_tripped(self):
        engine = _engine()
        _serving(engine, body=CAPTCHA)
        for _ in range(5):
            await engine._process_request(Request(f"https://{DOMAIN}/a"))
        engine.ensure_other = engine.session_manager.ensure_domain_session(OTHER, FetchLevel.DIRECT)
        other_session = engine.session_manager.get(engine.ensure_other)

        await engine._process_request(Request(f"https://{DOMAIN}/b"))

        assert engine.session_manager.get(engine.ensure_other) is other_session


class TestDomainsAreIndependent:
    @pytest.mark.asyncio
    async def test_signals_on_one_domain_leave_the_other_at_direct(self):
        engine = _engine()
        _serving(engine, body=CAPTCHA, other_body=LONG)

        await engine._process_request(Request(f"https://{DOMAIN}/a"))
        await engine._process_request(Request("https://other.com/b"))

        stats = engine._adaptive_engine.stats()
        assert stats[DOMAIN]["level"] == int(FetchLevel.HEADERS)
        assert stats[OTHER]["level"] == int(FetchLevel.DIRECT)


class TestSpiderSurface:
    @pytest.mark.asyncio
    async def test_adaptive_stats_is_empty_without_a_crawl(self):
        assert _Spider().adaptive_stats() == {}

    @pytest.mark.asyncio
    async def test_adaptive_stats_reads_the_engine(self):
        engine = _engine()
        _serving(engine, body=CAPTCHA)
        await engine._process_request(Request(f"https://{DOMAIN}/a"))
        engine.spider._engine = engine

        stats = engine.spider.adaptive_stats()

        assert stats[DOMAIN]["level"] == int(FetchLevel.HEADERS)
        assert {"level", "risk", "signal_counts", "reset_count"} <= set(stats[DOMAIN])


class TestFailureStats:
    def test_no_running_engine_means_no_stats(self):
        class Lone(Spider):
            name = "lone"
            start_urls = ["https://example.com/"]

            async def parse(self, response):
                yield None

        assert Lone().failure_stats() == {}

    def test_browser_failures_are_reported_per_session(self):
        class Watching(Spider):
            name = "watching"
            start_urls = ["https://example.com/"]

            async def parse(self, response):
                yield None

        spider = Watching()
        spider._engine = CrawlerEngine(spider, spider._session_manager)
        sid = spider._session_manager.ensure_domain_session(DOMAIN, FetchLevel.BROWSER)
        spider._session_manager.get(sid)._failures = [
            FetchFailure(url=f"https://{DOMAIN}/", exc_type="TimeoutError", exc_msg="boom", attempt=n, elapsed_ms=12.5)
            for n in (1, 2)
        ]

        assert spider.failure_stats() == {sid: {"total": 2, "by_type": {"TimeoutError": 2}}}
