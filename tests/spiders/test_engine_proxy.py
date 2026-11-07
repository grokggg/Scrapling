"""Engine ↔ proxy pool: injection at the PROXY rung, TLS fallback, release on outcomes."""

import pytest

from scrapling.engines.toolbelt.custom import Response
from scrapling.spiders.adaptive import AdaptiveEngine, FetchLevel
from scrapling.spiders.engine import CrawlerEngine
from scrapling.spiders.presets import APT_PRESET
from scrapling.spiders.proxy_pool import ProxyPool, ProxyState
from scrapling.spiders.request import Request
from scrapling.spiders.retry import RetryPolicy
from scrapling.spiders.signals import BlockSignal
from scrapling.spiders.spider import Spider

DOMAIN = "example.com"
PROXY = "http://user:pass@10.0.0.1:8000"
LONG = b"<html><body>" + b"content " * 400 + b"</body></html>"


class _Spider(Spider):
    name = "proxy"
    start_urls = [f"https://{DOMAIN}/"]

    async def parse(self, response):
        yield {"url": response.url}


def _response(url: str = f"https://{DOMAIN}/", status: int = 200) -> Response:
    return Response(url, LONG, status, "probe", {}, {}, {}, _log=False)


def _engine(pool: ProxyPool | None = None) -> CrawlerEngine:
    spider = _Spider()
    if pool is not None:
        spider.proxy_pool = pool
    engine = CrawlerEngine(spider, spider._session_manager)
    engine._adaptive_engine = AdaptiveEngine(APT_PRESET)
    return engine


def _escalate(engine: CrawlerEngine, level: FetchLevel) -> None:
    """APT escalates on every signal, so N signals land exactly on level N."""
    for _ in range(int(level)):
        engine._adaptive_engine.observe(DOMAIN, BlockSignal.CAPTCHA, 0.9)
    assert engine._adaptive_engine.plan(DOMAIN)[0] == level


class _Recorder:
    def __init__(self) -> None:
        self.requests = []

    async def __call__(self, request):
        self.requests.append(request)
        return _response(request.url)

    @property
    def last(self):
        return self.requests[-1]


class TestInjection:
    @pytest.mark.asyncio
    async def test_proxy_rung_injects_a_proxy(self):
        pool = ProxyPool()
        pool.add(PROXY)
        engine = _engine(pool)
        _escalate(engine, FetchLevel.PROXY)
        recorder = _Recorder()
        engine.session_manager.fetch = recorder

        await engine._process_request(Request(f"https://{DOMAIN}/a"))

        assert recorder.last._session_kwargs["proxy"] == PROXY
        assert recorder.last.sid == f"static:3:{DOMAIN}"

    @pytest.mark.asyncio
    async def test_without_a_pool_the_rung_falls_back_to_tls(self):
        engine = _engine()
        _escalate(engine, FetchLevel.PROXY)
        recorder = _Recorder()
        engine.session_manager.fetch = recorder

        await engine._process_request(Request(f"https://{DOMAIN}/a"))

        assert "proxy" not in recorder.last._session_kwargs
        assert recorder.last.sid == f"static:2:{DOMAIN}"

    @pytest.mark.asyncio
    async def test_a_dry_pool_falls_back_to_tls(self):
        engine = _engine(ProxyPool())  # registered but empty
        _escalate(engine, FetchLevel.PROXY)
        recorder = _Recorder()
        engine.session_manager.fetch = recorder

        await engine._process_request(Request(f"https://{DOMAIN}/a"))

        assert "proxy" not in recorder.last._session_kwargs
        assert recorder.last.sid == f"static:2:{DOMAIN}"

    @pytest.mark.asyncio
    async def test_direct_rung_never_injects(self):
        pool = ProxyPool()
        pool.add(PROXY)
        engine = _engine(pool)
        recorder = _Recorder()
        engine.session_manager.fetch = recorder

        await engine._process_request(Request(f"https://{DOMAIN}/a"))

        assert "proxy" not in recorder.last._session_kwargs
        assert recorder.last.sid == f"static:0:{DOMAIN}"


class TestRelease:
    @pytest.mark.asyncio
    async def test_success_is_reported_with_latency_and_domain(self):
        pool = ProxyPool()
        pool.add(PROXY)
        engine = _engine(pool)
        _escalate(engine, FetchLevel.PROXY)
        engine.session_manager.fetch = _Recorder()

        await engine._process_request(Request(f"https://{DOMAIN}/a"))

        entry = pool.get(PROXY)
        assert entry.success_count == 1
        assert entry.fail_count == 0
        assert entry.domain_stats[DOMAIN]["success"] == 1
        assert entry.avg_latency_ms >= 0.0

    @pytest.mark.asyncio
    async def test_exception_is_reported_as_failure(self):
        pool = ProxyPool()
        pool.add(PROXY)
        engine = _engine(pool)
        engine.spider.retry_policy = RetryPolicy(max_attempts=0)
        _escalate(engine, FetchLevel.PROXY)

        async def boom(request):
            raise ConnectionError("proxy refused")

        engine.session_manager.fetch = boom

        await engine._process_request(Request(f"https://{DOMAIN}/a"))

        entry = pool.get(PROXY)
        assert entry.fail_count == 1
        assert entry.consecutive_fail == 1
        assert entry.state is ProxyState.DEGRADED
        assert entry.score == 85.0

    @pytest.mark.asyncio
    async def test_a_429_is_a_proxy_failure_and_an_escalation(self):
        pool = ProxyPool()
        pool.add(PROXY)
        engine = _engine(pool)
        engine.spider.retry_policy = RetryPolicy(max_attempts=0)
        _escalate(engine, FetchLevel.PROXY)

        async def limited(request):
            return _response(request.url, status=429)

        engine.session_manager.fetch = limited
        await engine._process_request(Request(f"https://{DOMAIN}/a"))

        entry = pool.get(PROXY)
        assert entry.fail_count == 1
        assert engine._adaptive_engine.stats()[DOMAIN]["signal_counts"]["RATELIMIT"] == 1
        assert engine._adaptive_engine.plan(DOMAIN)[0] == FetchLevel.BROWSER

        seen = []

        async def ok(request):
            seen.append(request.sid)
            return _response(request.url)

        engine.session_manager.fetch = ok
        await engine._process_request(Request(f"https://{DOMAIN}/b"))

        assert seen == [f"browser:4:{DOMAIN}"]

    @pytest.mark.asyncio
    async def test_three_failures_bench_the_proxy(self):  # noqa: D401
        pool = ProxyPool()
        pool.add(PROXY)
        engine = _engine(pool)
        engine.spider.retry_policy = RetryPolicy(max_attempts=0)
        _escalate(engine, FetchLevel.PROXY)

        async def boom(request):
            raise ConnectionError("proxy refused")

        engine.session_manager.fetch = boom
        for _ in range(3):
            await engine._process_request(Request(f"https://{DOMAIN}/a"))

        assert pool.get(PROXY).state is ProxyState.COOLING

        # Three dead proxies also escalate (each exception is a low-weight signal), so the
        # next request is on a stronger rung and carries no proxy at all.
        recorder = _Recorder()
        engine.session_manager.fetch = recorder
        await engine._process_request(Request(f"https://{DOMAIN}/b"))

        assert "proxy" not in recorder.last._session_kwargs
        assert recorder.last.sid == f"browser:4:{DOMAIN}"


class TestIsolation:
    @pytest.mark.asyncio
    async def test_two_domains_keep_their_own_proxy_history(self):
        pool = ProxyPool()
        pool.add(PROXY)
        engine = _engine(pool)
        engine.session_manager.fetch = _Recorder()

        await engine._process_request(Request(f"https://{DOMAIN}/a"))
        await engine._process_request(Request("https://other.com/b"))

        assert "proxy" not in engine.stats.proxies  # no proxy at DIRECT
        assert pool.get(PROXY).domain_stats == {}


class TestBrowserRung:
    @pytest.mark.asyncio
    async def test_browser_without_a_pool_stays_browser(self):
        engine = _engine()
        _escalate(engine, FetchLevel.BROWSER)
        recorder = _Recorder()
        engine.session_manager.fetch = recorder

        await engine._process_request(Request(f"https://{DOMAIN}/a"))

        assert recorder.last.sid == f"browser:4:{DOMAIN}"
        assert "proxy" not in recorder.last._session_kwargs

    @pytest.mark.asyncio
    async def test_browser_with_a_pool_rides_the_proxy(self):
        pool = ProxyPool()
        pool.add(PROXY)
        engine = _engine(pool)
        _escalate(engine, FetchLevel.BROWSER)
        recorder = _Recorder()
        engine.session_manager.fetch = recorder

        await engine._process_request(Request(f"https://{DOMAIN}/a"))

        assert recorder.last.sid == f"browser:4:{DOMAIN}"
        assert recorder.last._session_kwargs["proxy"] == PROXY

    @pytest.mark.asyncio
    async def test_browser_with_a_dry_pool_still_runs(self):
        engine = _engine(ProxyPool())
        _escalate(engine, FetchLevel.BROWSER)
        recorder = _Recorder()
        engine.session_manager.fetch = recorder

        await engine._process_request(Request(f"https://{DOMAIN}/a"))

        assert recorder.last.sid == f"browser:4:{DOMAIN}"
        assert "proxy" not in recorder.last._session_kwargs
