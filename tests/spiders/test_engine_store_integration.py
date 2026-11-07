"""Store wiring end-to-end: fetch → extract → upsert, and a real spider subclass."""

import pytest

from scrapling.engines.toolbelt.custom import Response
from scrapling.spiders.adaptive import AdaptiveEngine
from scrapling.spiders.engine import CrawlerEngine
from scrapling.spiders.incremental import IncrementalCache
from scrapling.spiders.incremental_storage import MemoryStorage
from scrapling.spiders.presets import APT_PRESET
from scrapling.spiders.request import Request
from scrapling.spiders.spider import Spider
from scrapling.spiders.stores import SQLiteStore

QUOTES = b"""<html><body>
<div class="quote"><span class="text">"Simplicity is prerequisite for reliability."</span>
<span class="author">Edsger W. Dijkstra</span></div>
</body></html>"""


def _response(body: bytes = QUOTES, status: int = 200, headers: dict | None = None, url: str = "") -> Response:
    return Response(url, body, status, "OK", {}, headers or {}, {}, _log=False)


class QuotesSpider(Spider):
    """What a real spider looks like: parser for HTML, extractor for the store row."""

    name = "quotes_store"
    start_urls = ["https://quotes.example/page/1"]

    async def parse(self, response):
        yield {"text": response.css("span.text::text").get()}

    def extract_item(self, request, response):
        return {
            "text": response.css("span.text::text").get(),
            "author": response.css("span.author::text").get(),
        }


def _engine(spider: Spider) -> CrawlerEngine:
    engine = CrawlerEngine(spider, spider._session_manager)
    engine._adaptive_engine = AdaptiveEngine(APT_PRESET)
    return engine


class _Fetcher:
    def __init__(self, *responses: Response) -> None:
        self._responses = list(responses)

    async def __call__(self, request):
        return self._responses.pop(0)


async def _crawl(engine: CrawlerEngine, url: str, response: Response) -> None:
    engine.session_manager.fetch = _Fetcher(response)
    await engine._process_request(Request(url))


@pytest.mark.asyncio
async def test_a_full_pass_reaches_the_store(tmp_path):
    spider = QuotesSpider()
    spider.store = SQLiteStore(tmp_path / "quotes.db")
    engine = _engine(spider)

    await _crawl(engine, spider.start_urls[0], _response(url=spider.start_urls[0]))

    row = spider.store.get(spider.start_urls[0])
    assert row.item["author"] == "Edsger W. Dijkstra"
    assert row.item["text"].startswith('"Simplicity')
    assert spider.store.count() == 1
    spider.store.close()


@pytest.mark.asyncio
async def test_a_304_reuses_the_cache_and_still_refreshes_the_row(tmp_path):
    url = "https://quotes.example/page/1"
    spider = QuotesSpider()
    spider.store = SQLiteStore(tmp_path / "quotes.db")
    spider.incremental_cache = IncrementalCache(MemoryStorage(), ttl_seconds=3600)
    engine = _engine(spider)

    await _crawl(engine, url, _response(headers={"ETag": '"q1"'}, url=url))
    first = spider.store.get(url)
    await _crawl(engine, url, _response(body=b"", status=304, url=url))
    second = spider.store.get(url)

    assert engine.stats.incremental_hits == 1
    assert second.fetched_at >= first.fetched_at
    assert second.item == first.item, "the cached body produced the same row"
    assert spider.store.count() == 1
    spider.store.close()


@pytest.mark.asyncio
async def test_a_subclassed_spider_needs_no_engine_changes(tmp_path):
    spider = QuotesSpider()
    spider.store = SQLiteStore(tmp_path / "quotes.db")
    spider.dedup_enabled = True
    engine = _engine(spider)

    await _crawl(engine, spider.start_urls[0], _response(url=spider.start_urls[0]))
    await _crawl(engine, "https://quotes.example/page/2", _response(url="https://quotes.example/page/2"))

    assert spider.store.count() == 1, "page 2 is the same quote, dropped by dedup"
    assert (engine.stats.stored_new, engine.stats.dedup_skipped) == (1, 1)
    assert engine.stats.custom_stats == {}
    spider.store.close()


@pytest.mark.asyncio
async def test_the_engine_survives_a_store_that_cannot_write(tmp_path):
    class Failing(SQLiteStore):
        def put(self, item):
            raise OSError("no space left")

    spider = QuotesSpider()
    spider.store = Failing(tmp_path / "quotes.db")
    engine = _engine(spider)
    engine.session_manager.fetch = _Fetcher(_response(url=spider.start_urls[0]))

    await engine._process_request(Request(spider.start_urls[0]))

    assert engine.stats.requests_count == 1
    assert engine.stats.failed_requests_count == 0, "a storage hiccup is not a fetch failure"
    spider.store.close()
