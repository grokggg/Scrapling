"""Queue injection: a spider can hand the engine its own queue, and the engine uses it."""

import pytest

from scrapling.spiders.engine import CrawlerEngine
from scrapling.spiders.queues.memory import MemoryQueue
from scrapling.spiders.request import Request
from scrapling.spiders.session import SessionManager
from scrapling.spiders.spider import Spider

URL = "https://example.com/a"


class _Response:
    def __init__(self, status: int = 200, body: bytes = b"<html>ok</html>", url: str = URL):
        self.status = status
        self.body = body
        self.url = url
        self.encoding = "utf-8"
        self.headers = {}
        self.meta = {}
        self.request = None

    def __str__(self) -> str:
        return self.url


class _Session:
    name = "fake"

    def __init__(self, *answers):
        self._answers = list(answers) or [_Response()]
        self._is_alive = False

    async def __aenter__(self):
        self._is_alive = True
        return self

    async def __aexit__(self, *exc_info):
        self._is_alive = False

    async def fetch(self, url, **kwargs):
        answer = self._answers.pop(0) if len(self._answers) > 1 else self._answers[0]
        if isinstance(answer, Exception):
            raise answer
        return answer


class _ProbeSpider(Spider):
    name = "queue_injection_probe"
    start_urls = [URL]
    concurrent_requests = 1

    async def parse(self, response):
        yield {"url": URL}


class SpyQueue:
    """A backend that only records: proves the engine goes through the injected object."""

    def __init__(self, *, fail_on_enqueue: bool = False):
        self._inner = MemoryQueue()
        self.enqueued: list[str] = []
        self.completed: list[str] = []
        self.snapshots = 0
        self.fail_on_enqueue = fail_on_enqueue

    async def enqueue(self, request: Request) -> bool:
        if self.fail_on_enqueue:
            raise RuntimeError("queue unavailable")
        self.enqueued.append(request.url)
        return await self._inner.enqueue(request)

    async def dequeue(self) -> Request:
        return await self._inner.dequeue()

    async def complete(self, request: Request) -> None:
        self.completed.append(request.url)
        await self._inner.complete(request)

    @property
    def is_empty(self) -> bool:
        return self._inner.is_empty

    def __len__(self) -> int:
        return len(self._inner)

    async def snapshot(self):
        self.snapshots += 1
        return await self._inner.snapshot()

    async def restore(self, data) -> None:
        await self._inner.restore(data)


def _engine(spider: Spider) -> CrawlerEngine:
    manager = SessionManager()
    manager.add("default", _Session())
    engine = CrawlerEngine(spider, manager)
    engine._per_domain_sessions = False
    engine.session_manager.ensure_domain_session = lambda domain, level: "default"
    return engine


def test_default_backend_is_the_in_memory_queue():
    engine = _engine(_ProbeSpider())

    assert isinstance(engine.scheduler._backend, MemoryQueue)


def test_injected_backend_is_used_verbatim():
    spider = _ProbeSpider()
    spy = SpyQueue()
    spider.queue_backend = spy

    engine = _engine(spider)

    assert engine.scheduler._backend is spy


@pytest.mark.asyncio
async def test_a_crawl_runs_through_the_injected_queue():
    spider = _ProbeSpider()
    spy = SpyQueue()
    spider.queue_backend = spy
    engine = _engine(spider)

    result = await engine.crawl()

    assert result.requests_count == 1
    assert spy.enqueued == [URL]
    assert spy.completed == [URL]


@pytest.mark.asyncio
async def test_queue_errors_propagate_instead_of_being_swallowed():
    """No swallowing layer exists for queue failures; they surface as-is."""
    spider = _ProbeSpider()
    spider.queue_backend = SpyQueue(fail_on_enqueue=True)
    engine = _engine(spider)

    with pytest.raises(RuntimeError, match="queue unavailable"):
        await engine.crawl()
