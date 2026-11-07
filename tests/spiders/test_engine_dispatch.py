"""Engine ↔ SessionManager wiring: sid assignment at enqueue and at fetch time."""

import pytest

from scrapling.engines.toolbelt.custom import Response
from scrapling.spiders.adaptive import AdaptiveEngine, FetchLevel
from scrapling.spiders.engine import CrawlerEngine
from scrapling.spiders.presets import APT_PRESET, ENGINEERING_PRESET
from scrapling.spiders.request import Request
from scrapling.spiders.signals import BlockSignal
from scrapling.spiders.spider import Spider

DOMAIN = "example.com"


def _response(url: str = f"https://{DOMAIN}/", status: int = 200) -> Response:
    return Response(url, b"<html><body>" + b"payload " * 100 + b"</body></html>", status, "probe", {}, {}, {}, _log=False)


class _Spider(Spider):
    """Default configure_sessions(): one `static:__default__` session, framework owns sid dispatch."""

    name = "dispatch"
    start_urls = [f"https://{DOMAIN}/"]

    async def parse(self, response):
        yield {"url": response.url}


def _engine(spider=None) -> CrawlerEngine:
    spider = spider or _Spider()
    return CrawlerEngine(spider, spider._session_manager)


class TestNormalizeRequest:
    def test_empty_sid_becomes_the_domain_static_sid(self):
        engine = _engine()
        request = Request(f"https://{DOMAIN}/page")
        assert request.sid == ""

        engine._normalize_request(request)

        assert request.sid == f"static:0:{DOMAIN}"

    def test_two_domains_normalize_to_two_sids(self):
        engine = _engine()
        first, second = Request(f"https://{DOMAIN}/a"), Request("https://other.com/b")

        engine._normalize_request(first)
        engine._normalize_request(second)

        assert first.sid != second.sid

    def test_explicit_sid_is_preserved(self):
        engine = _engine()
        request = Request(f"https://{DOMAIN}/page", sid="custom")

        engine._normalize_request(request)

        assert request.sid == "custom"

    def test_hostless_request_falls_back_to_the_default_session(self):
        engine = _engine()
        request = Request("https://example.com/")
        request.url = ""  # simulate a request with no host

        engine._normalize_request(request)

        assert request.sid == "static:0:__default__"

    def test_custom_session_config_keeps_the_upstream_default_contract(self):
        class _Custom(_Spider):
            name = "custom"

            def configure_sessions(self, manager):
                manager.add("mine", object())

        engine = _engine(_Custom())
        request = Request(f"https://{DOMAIN}/page")

        engine._normalize_request(request)

        assert request.sid == "mine"


class TestProcessRequestDispatch:
    @pytest.mark.asyncio
    async def test_fetch_uses_the_domain_sid(self):
        engine = _engine()
        seen = []

        async def fake_fetch(request):
            seen.append(request.sid)
            return _response(request.url)

        engine.session_manager.fetch = fake_fetch
        await engine._process_request(Request(f"https://{DOMAIN}/page"))

        assert seen == [f"static:0:{DOMAIN}"]
        assert f"static:0:{DOMAIN}" in engine.session_manager

    @pytest.mark.asyncio
    async def test_two_domains_fetch_through_two_sessions(self):
        engine = _engine()
        seen = []

        async def fake_fetch(request):
            seen.append(request.sid)
            return _response(request.url)

        engine.session_manager.fetch = fake_fetch
        await engine._process_request(Request(f"https://{DOMAIN}/a"))
        await engine._process_request(Request("https://other.com/b"))

        assert seen == [f"static:0:{DOMAIN}", "static:0:other.com"]

    @pytest.mark.asyncio
    async def test_explicit_sid_is_not_overridden_at_fetch_time(self):
        engine = _engine()
        seen = []

        async def fake_fetch(request):
            seen.append(request.sid)
            return _response(request.url)

        engine.session_manager.fetch = fake_fetch
        await engine._process_request(Request(f"https://{DOMAIN}/page", sid="custom"))

        assert seen == ["custom"]

    @pytest.mark.asyncio
    async def test_missing_adaptive_engine_defaults_to_direct(self):
        engine = _engine()
        engine._adaptive_engine = None
        seen = []

        async def fake_fetch(request):
            seen.append(request.sid)
            return _response(request.url)

        engine.session_manager.fetch = fake_fetch
        await engine._process_request(Request(f"https://{DOMAIN}/page"))

        assert seen == [f"static:0:{DOMAIN}"]
        assert not any(sid.startswith("browser:") for sid in engine.session_manager.session_ids)

    @pytest.mark.asyncio
    async def test_browser_level_requests_a_browser_session(self):
        engine = _engine()
        engine._adaptive_engine = AdaptiveEngine(APT_PRESET)
        for _ in range(4):
            engine._adaptive_engine.observe(DOMAIN, BlockSignal.CAPTCHA, 0.9)
        seen = []

        async def fake_fetch(request):
            seen.append(request.sid)
            return _response(request.url)

        engine.session_manager.fetch = fake_fetch
        await engine._process_request(Request(f"https://{DOMAIN}/page"))

        assert seen == [f"browser:4:{DOMAIN}"]


class TestTheLadderProducesNewSessions:
    """Level-encoded sids: an upgrade allocates a new session instead of re-using one."""

    @pytest.mark.asyncio
    async def test_upgrade_switches_the_fetch_to_a_new_sid(self):
        engine = _engine()
        engine._adaptive_engine = AdaptiveEngine(ENGINEERING_PRESET)
        seen = []

        async def fake_fetch(request):
            seen.append(request.sid)
            return _response(request.url)

        engine.session_manager.fetch = fake_fetch

        await engine._process_request(Request(f"https://{DOMAIN}/a"))
        engine._adaptive_engine.observe(DOMAIN, BlockSignal.CAPTCHA, 0.9)
        engine._adaptive_engine.observe(DOMAIN, BlockSignal.CAPTCHA, 0.9)  # 108 >= 80 -> HEADERS
        assert engine._adaptive_engine.plan(DOMAIN)[0] == FetchLevel.HEADERS
        await engine._process_request(Request(f"https://{DOMAIN}/b"))

        assert seen == [f"static:0:{DOMAIN}", f"static:1:{DOMAIN}"]
        # The old rung stays registered; it is the session that carries the live cookies.
        assert f"static:0:{DOMAIN}" in engine.session_manager
