"""BROWSER rung failures degrade to TLS once, in-request, before the retry engine sees them."""

import pytest

from scrapling.engines.toolbelt.custom import Response
from scrapling.spiders.adaptive import AdaptiveEngine, FetchLevel
from scrapling.spiders.engine import CrawlerEngine
from scrapling.spiders.presets import APT_PRESET
from scrapling.spiders.request import Request
from scrapling.spiders.signals import BlockSignal
from scrapling.spiders.spider import Spider

DOMAIN = "example.com"
BODY = b"<html><body>" + b"content " * 400 + b"</body></html>"


class _Spider(Spider):
    name = "fallback"
    start_urls = [f"https://{DOMAIN}/"]

    async def parse(self, response):
        yield {"url": response.url}


def _response(url: str = f"https://{DOMAIN}/", status: int = 200) -> Response:
    return Response(url, BODY, status, "probe", {}, {}, {}, _log=False)


def _engine(fallback: bool = True) -> CrawlerEngine:
    spider = _Spider()
    spider.browser_fallback_to_static = fallback
    engine = CrawlerEngine(spider, spider._session_manager)
    engine._adaptive_engine = AdaptiveEngine(APT_PRESET)
    for _ in range(int(FetchLevel.BROWSER)):
        engine._adaptive_engine.observe(DOMAIN, BlockSignal.CAPTCHA, 0.9)
    assert engine._adaptive_engine.plan(DOMAIN)[0] is FetchLevel.BROWSER
    return engine


class _Scripted:
    """Fails on the browser rung, and on TLS if `tls_fails` is set."""

    def __init__(self, tls_fails: bool = False) -> None:
        self.tls_fails = tls_fails
        self.calls = []

    async def __call__(self, request):
        self.calls.append(request.sid)
        if request.sid.startswith("browser:"):
            raise TimeoutError("page.goto: Timeout 30000ms exceeded")
        if self.tls_fails:
            raise ConnectionError("tls also refused")
        return _response(request.url)

    @property
    def browser_calls(self):
        return [sid for sid in self.calls if sid.startswith("browser:")]

    @property
    def tls_calls(self):
        return [sid for sid in self.calls if sid.startswith("static:")]


@pytest.mark.asyncio
async def test_a_browser_failure_retries_the_same_request_at_tls():
    engine = _engine()
    scripted = _Scripted()
    engine.session_manager.fetch = scripted
    request = Request(f"https://{DOMAIN}/a")

    await engine._process_request(request)

    assert len(scripted.calls) == 2
    assert scripted.browser_calls[0].startswith("browser:4:")
    assert scripted.tls_calls[0] == f"static:2:{DOMAIN}"
    assert request.sid == f"static:2:{DOMAIN}"


@pytest.mark.asyncio
async def test_a_successful_fallback_never_reaches_the_retry_engine():
    engine = _engine()
    engine.session_manager.fetch = _Scripted()
    request = Request(f"https://{DOMAIN}/b")

    await engine._process_request(request)

    assert engine.stats.failed_requests_count == 0
    assert len(engine.scheduler) == 0
    assert engine.stats.requests_count > 0


@pytest.mark.asyncio
async def test_a_failed_fallback_still_hands_over_to_the_retry_engine():
    engine = _engine()
    scripted = _Scripted(tls_fails=True)
    engine.session_manager.fetch = scripted
    request = Request(f"https://{DOMAIN}/c")

    await engine._process_request(request)

    assert len(scripted.calls) == 2, "one hop only: TLS failing must not fall back again"
    assert len(engine.scheduler) == 1
    assert engine.stats.failed_requests_count == 0


@pytest.mark.asyncio
async def test_the_switch_turns_the_fallback_off():
    engine = _engine(fallback=False)
    scripted = _Scripted()
    engine.session_manager.fetch = scripted
    request = Request(f"https://{DOMAIN}/d")

    await engine._process_request(request)

    assert scripted.tls_calls == []
    assert len(scripted.calls) == 1
    assert len(engine.scheduler) == 1


@pytest.mark.asyncio
async def test_other_rungs_never_fall_back():
    engine = CrawlerEngine(_Spider(), _Spider()._session_manager)
    engine._adaptive_engine = AdaptiveEngine(APT_PRESET)
    for _ in range(int(FetchLevel.TLS)):
        engine._adaptive_engine.observe(DOMAIN, BlockSignal.CAPTCHA, 0.9)
    assert engine._adaptive_engine.plan(DOMAIN)[0] is FetchLevel.TLS

    scripted = _Scripted(tls_fails=True)  # the TLS rung is the one under test here
    engine.session_manager.fetch = scripted

    await engine._process_request(Request(f"https://{DOMAIN}/e"))

    assert len(scripted.calls) == 1
    assert scripted.browser_calls == []
    assert len(engine.scheduler) == 1


@pytest.mark.asyncio
async def test_the_downgrade_is_recorded_as_evidence():
    engine = _engine()
    engine.session_manager.fetch = _Scripted()
    before = engine._adaptive_engine.stats()[DOMAIN]["risk"]

    await engine._process_request(Request(f"https://{DOMAIN}/f"))

    after = engine._adaptive_engine.stats()[DOMAIN]["risk"]
    assert after > before, "the browser failure must feed the ladder, fallback or not"


@pytest.mark.asyncio
async def test_the_browser_proxy_is_not_reused_by_the_tls_attempt():
    engine = _engine()
    scripted = _Scripted()
    engine.session_manager.fetch = scripted
    request = Request(f"https://{DOMAIN}/g")
    request._session_kwargs["proxy"] = "http://dead:1"

    await engine._process_request(request)

    assert "proxy" not in request._session_kwargs
    assert "proxies" not in request._session_kwargs
