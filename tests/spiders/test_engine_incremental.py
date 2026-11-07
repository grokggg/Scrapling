"""Engine-side incremental crawling: conditional headers in, 304 / unchanged 200 rewritten."""

import pytest

from scrapling.engines.toolbelt.custom import Response
from scrapling.spiders.adaptive import AdaptiveEngine
from scrapling.spiders.engine import CrawlerEngine
from scrapling.spiders.incremental import IncrementalCache
from scrapling.spiders.incremental_storage import MemoryStorage
from scrapling.spiders.presets import APT_PRESET
from scrapling.spiders.request import Request
from scrapling.spiders.spider import Spider

DOMAIN = "example.com"
URL = f"https://{DOMAIN}/page"
BODY = b"<html><body>" + b"same " * 200 + b"</body></html>"
NEW_BODY = b"<html><body>" + b"changed " * 200 + b"</body></html>"


class _Spider(Spider):
    name = "incremental_engine"
    start_urls = [URL]

    async def parse(self, response):
        yield {"url": response.url, "size": len(response.body)}


def _response(body: bytes = BODY, status: int = 200, headers: dict | None = None, url: str = URL) -> Response:
    return Response(url, body, status, "OK", {}, headers or {}, {}, _log=False)


def _engine(cache: IncrementalCache | None) -> CrawlerEngine:
    spider = _Spider()
    spider.incremental_cache = cache
    engine = CrawlerEngine(spider, spider._session_manager)
    engine._adaptive_engine = AdaptiveEngine(APT_PRESET)
    return engine


class _Fetcher:
    """Answers with whatever is queued and records the headers the request actually carried."""

    def __init__(self, *responses: Response) -> None:
        self._responses = list(responses)
        self.seen = []
        self.calls = 0

    async def __call__(self, request):
        self.calls += 1
        self.seen.append(dict(request._session_kwargs.get("headers") or {}))
        return self._responses.pop(0)


@pytest.mark.asyncio
async def test_without_a_cache_nothing_is_conditional():
    engine = _engine(None)
    fetcher = _Fetcher(_response())
    engine.session_manager.fetch = fetcher

    await engine._process_request(Request(URL))

    assert fetcher.seen == [{}]
    assert "incremental_hits" not in engine.stats.custom_stats


@pytest.mark.asyncio
async def test_a_stored_etag_becomes_if_none_match():
    engine = _engine(IncrementalCache(MemoryStorage(), ttl_seconds=3600))
    engine.session_manager.fetch = _Fetcher(_response(headers={"ETag": '"v1"'}))
    await engine._process_request(Request(URL))

    fetcher = _Fetcher(_response(body=NEW_BODY))
    engine.session_manager.fetch = fetcher
    await engine._process_request(Request(URL))

    assert fetcher.seen[0]["If-None-Match"] == '"v1"'


@pytest.mark.asyncio
async def test_caller_headers_win_over_conditional_ones():
    cache = IncrementalCache(MemoryStorage(), ttl_seconds=3600)
    cache.put(URL, _response(headers={"ETag": '"v1"'}))
    engine = _engine(cache)
    fetcher = _Fetcher(_response(body=BODY, headers={"ETag": '"v1"'}))
    engine.session_manager.fetch = fetcher
    request = Request(URL)
    request._session_kwargs["headers"] = {"If-None-Match": '"caller"', "X-Trace": "1"}

    await engine._process_request(request)

    assert fetcher.seen[0]["If-None-Match"] == '"caller"'
    assert fetcher.seen[0]["X-Trace"] == "1"


@pytest.mark.asyncio
async def test_a_304_is_served_from_the_cache_as_a_200():
    cache = IncrementalCache(MemoryStorage(), ttl_seconds=3600)
    cache.put(URL, _response(headers={"ETag": '"v1"'}))
    engine = _engine(cache)
    engine.session_manager.fetch = _Fetcher(_response(body=b"", status=304))

    await engine._process_request(Request(URL))

    assert engine.stats.custom_stats["incremental_hits"] == 1
    assert engine.stats.response_status_count == {"status_200": 1}
    assert engine.stats.requests_count == 1


@pytest.mark.asyncio
async def test_a_200_with_the_same_body_counts_as_a_hit():
    cache = IncrementalCache(MemoryStorage(), ttl_seconds=3600)
    cache.put(URL, _response())
    engine = _engine(cache)
    engine.session_manager.fetch = _Fetcher(_response(body=BODY))

    await engine._process_request(Request(URL))

    assert engine.stats.custom_stats["incremental_hits"] == 1
    assert "incremental_misses" not in engine.stats.custom_stats


@pytest.mark.asyncio
async def test_a_changed_200_refreshes_the_cache():
    cache = IncrementalCache(MemoryStorage(), ttl_seconds=3600)
    cache.put(URL, _response())
    engine = _engine(cache)
    engine.session_manager.fetch = _Fetcher(_response(body=NEW_BODY))

    await engine._process_request(Request(URL))

    assert engine.stats.custom_stats["incremental_misses"] == 1
    assert "incremental_hits" not in engine.stats.custom_stats
    assert cache.get(URL).body_size == len(NEW_BODY)


@pytest.mark.asyncio
async def test_the_first_200_populates_the_cache():
    cache = IncrementalCache(MemoryStorage(), ttl_seconds=3600)
    engine = _engine(cache)
    engine.session_manager.fetch = _Fetcher(_response(headers={"Last-Modified": "Wed, 21 Oct 2015 07:28:00 GMT"}))

    await engine._process_request(Request(URL))

    assert cache.conditional_headers(URL) == {"If-Modified-Since": "Wed, 21 Oct 2015 07:28:00 GMT"}
    assert engine.stats.custom_stats["incremental_misses"] == 1


@pytest.mark.asyncio
async def test_a_post_never_goes_incremental():
    cache = IncrementalCache(MemoryStorage(), ttl_seconds=3600)
    cache.put(URL, _response(headers={"ETag": '"v1"'}))
    engine = _engine(cache)
    fetcher = _Fetcher(_response(body=NEW_BODY, status=201))
    engine.session_manager.fetch = fetcher
    request = Request(URL)
    request._session_kwargs["method"] = "POST"

    await engine._process_request(request)

    assert fetcher.seen == [{}]
    assert "incremental_hits" not in engine.stats.custom_stats


@pytest.mark.asyncio
async def test_an_unknown_url_sends_nothing_and_gets_stored():
    cache = IncrementalCache(MemoryStorage(), ttl_seconds=3600)
    engine = _engine(cache)
    fetcher = _Fetcher(_response())
    engine.session_manager.fetch = fetcher

    await engine._process_request(Request(URL))

    assert fetcher.seen == [{}]
    assert cache.get(URL) is not None


@pytest.mark.asyncio
async def test_a_cache_hit_still_runs_the_pipeline():
    cache = IncrementalCache(MemoryStorage(), ttl_seconds=3600)
    cache.put(URL, _response(headers={"ETag": '"v1"'}))
    engine = _engine(cache)
    engine.session_manager.fetch = _Fetcher(_response(body=b"", status=304))

    seen = []
    original = engine._adaptive_engine.observe_success

    def spy(domain):
        seen.append(domain)
        return original(domain)

    engine._adaptive_engine.observe_success = spy
    parsed = []
    engine.spider.parse = lambda response, _p=parsed: _p.append(response)

    await engine._process_request(Request(URL))

    assert seen == [DOMAIN], "a cache hit is a clean response and must feed the ladder"
    assert len(parsed) == 1
    assert parsed[0].body == BODY
