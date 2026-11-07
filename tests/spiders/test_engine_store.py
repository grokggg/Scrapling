"""Engine → store wiring: what gets persisted, when it is skipped, and what failure costs."""

import pytest

from scrapling.engines.toolbelt.custom import Response
from scrapling.spiders.adaptive import AdaptiveEngine
from scrapling.spiders.engine import CrawlerEngine
from scrapling.spiders.presets import APT_PRESET
from scrapling.spiders.request import Request
from scrapling.spiders.spider import Spider
from scrapling.spiders.stores import MemoryStore

URL = "https://example.com/a"
BODY = b"<html><body><h1>quoted</h1></body></html>"


class _Spider(Spider):
    name = "engine_store"
    start_urls = [URL]

    async def parse(self, response):
        yield {"url": response.url}


def _response(status: int = 200, body: bytes = BODY, headers: dict | None = None, url: str = URL) -> Response:
    return Response(url, body, status, "OK", {}, headers or {}, {}, _log=False)


def _engine(store=None, *, dedup: bool = False, spider=None) -> CrawlerEngine:
    spider = spider or _Spider()
    spider.store = store
    spider.dedup_enabled = dedup
    engine = CrawlerEngine(spider, spider._session_manager)
    engine._adaptive_engine = AdaptiveEngine(APT_PRESET)
    return engine


class _Fetcher:
    def __init__(self, *responses: Response) -> None:
        self._responses = list(responses)

    async def __call__(self, request):
        return self._responses.pop(0)


async def _run(engine: CrawlerEngine, url: str = URL, response: Response | None = None):
    engine.session_manager.fetch = _Fetcher(response or _response(url=url))
    request = Request(url)
    await engine._process_request(request)
    return request


@pytest.mark.asyncio
async def test_without_a_store_nothing_is_written():
    engine = _engine(None)

    await _run(engine)

    assert engine._dedup is None
    assert engine.stats.custom_stats == {}


@pytest.mark.asyncio
async def test_a_200_is_stored_as_new():
    store = MemoryStore()
    engine = _engine(store)

    await _run(engine)

    assert store.count() == 1
    assert engine.stats.stored_new == 1
    assert engine.stats.stored_updated == 0
    saved = store.get(URL)
    assert saved.item["status"] == 200
    assert saved.item["url"] == URL


@pytest.mark.asyncio
async def test_the_same_url_twice_updates_the_row():
    store = MemoryStore()
    engine = _engine(store)

    await _run(engine)
    await _run(engine, response=_response(body=BODY + b"<!-- rev2 -->"))

    assert store.count() == 1
    assert engine.stats.stored_new == 1
    assert engine.stats.stored_updated == 1
    assert "rev2" in store.get(URL).item["body_preview"]


@pytest.mark.asyncio
async def test_dedup_skips_a_second_url_with_the_same_content():
    class Flat(_Spider):
        name = "engine_store_flat"

        def extract_item(self, request, response):
            return {"title": "same"}

    store = MemoryStore()
    engine = _engine(store, dedup=True, spider=Flat())

    await _run(engine, "https://example.com/a")
    await _run(engine, "https://example.com/b")

    assert store.count() == 1
    assert engine.stats.dedup_skipped == 1
    assert engine.stats.stored_new == 1


@pytest.mark.asyncio
async def test_without_dedup_both_urls_are_kept():
    class Flat(_Spider):
        name = "engine_store_flat_off"

        def extract_item(self, request, response):
            return {"title": "same"}

    store = MemoryStore()
    engine = _engine(store, dedup=False, spider=Flat())

    await _run(engine, "https://example.com/a")
    await _run(engine, "https://example.com/b")

    assert store.count() == 2
    assert engine.stats.dedup_skipped == 0


@pytest.mark.asyncio
async def test_a_404_is_not_stored():
    store = MemoryStore()
    engine = _engine(store)

    await _run(engine, response=_response(status=404, body=b"missing"))

    assert store.count() == 0
    assert engine.stats.stored_new == 0


@pytest.mark.asyncio
async def test_a_broken_store_does_not_break_the_crawl():
    class Broken(MemoryStore):
        def put(self, item):
            raise RuntimeError("disk on fire")

    class Counting(_Spider):
        name = "engine_store_broken"
        parsed = 0

        async def parse(self, response):
            Counting.parsed += 1
            yield {"url": response.url}

    engine = _engine(Broken(), spider=Counting())

    await _run(engine)

    assert Counting.parsed == 1, "the response still reached parse()"
    assert engine.stats.requests_count == 1


@pytest.mark.asyncio
async def test_an_empty_body_extracts_nothing():
    store = MemoryStore()
    engine = _engine(store)

    await _run(engine, response=_response(body=b""))

    assert store.count() == 0


@pytest.mark.asyncio
async def test_a_spider_extractor_overrides_the_preview():
    class Quotes(_Spider):
        name = "engine_store_quotes"

        def extract_item(self, request, response):
            return {"quote": "simplicity", "author": "PEP 20"}

    store = MemoryStore()
    engine = _engine(store, spider=Quotes())

    await _run(engine)

    assert store.get(URL).item == {"quote": "simplicity", "author": "PEP 20"}


@pytest.mark.asyncio
async def test_the_request_sid_and_meta_travel_with_the_row():
    store = MemoryStore()
    engine = _engine(store)
    request = Request(URL)
    request.meta["depth"] = 2
    engine.session_manager.fetch = _Fetcher(_response())

    await engine._process_request(request)

    saved = store.get(URL)
    assert saved.source_sid == request.sid
    assert saved.meta == {"depth": 2}


@pytest.mark.asyncio
async def test_a_304_hit_is_stored_once_not_twice():
    store = MemoryStore()
    engine = _engine(store)

    await _run(engine)
    await _run(engine)  # same body, same url

    assert store.count() == 1
    assert engine.stats.stored_new == 1
    assert engine.stats.stored_updated == 1
