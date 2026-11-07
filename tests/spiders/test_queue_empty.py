"""QueueEmpty / QueueClosed: what "no work right now" looks like to the crawl loop."""

import time

import pytest

from scrapling.spiders.engine import CrawlerEngine
from scrapling.spiders.queue_errors import QueueClosed, QueueEmpty
from scrapling.spiders.queues.memory import MemoryQueue
from scrapling.spiders.queues.postgres import PostgresQueue
from scrapling.spiders.request import Request
from scrapling.spiders.session import SessionManager
from scrapling.spiders.spider import Spider

URL = "https://example.com/a"


class _Response:
    def __init__(self, url: str = URL):
        self.status = 200
        self.body = b"ok"
        self.url = url
        self.encoding = "utf-8"
        self.headers = {}
        self.meta = {}
        self.request = None

    def __str__(self) -> str:
        return self.url


class _Session:
    name = "fake"

    def __init__(self):
        self._is_alive = False

    async def __aenter__(self):
        self._is_alive = True
        return self

    async def __aexit__(self, *exc_info):
        self._is_alive = False

    async def fetch(self, url, **kwargs):
        return _Response(url)


class _Spider(Spider):
    name = "queue_empty_probe"
    start_urls = [URL]

    async def parse(self, response):
        yield {"url": URL}


class _StubScheduler:
    """Stands in for the queue so the loop's reactions can be counted."""

    def __init__(self, *, closed=False, idle_after=1):
        self.dequeues = 0
        self.enqueued = 0
        self.closed = closed
        self.idle_after = idle_after

    async def enqueue(self, request):
        self.enqueued += 1
        return True

    async def dequeue(self):
        self.dequeues += 1
        if self.closed:
            raise QueueClosed()
        raise QueueEmpty()

    async def complete(self, request):
        return None

    @property
    def is_empty(self):
        return self.dequeues >= self.idle_after

    async def snapshot(self):
        return ([], set())

    async def restore(self, data):
        return None


def _engine(spider: Spider) -> CrawlerEngine:
    manager = SessionManager()
    manager.add("default", _Session())
    engine = CrawlerEngine(spider, manager)
    engine._per_domain_sessions = False
    engine.session_manager.ensure_domain_session = lambda domain, level: "default"
    engine._checkpoint_system_enabled = False
    return engine


# -- the exceptions themselves ----------------------------------------------------------------


def test_queue_empty_is_a_plain_exception():
    assert QueueEmpty.__bases__ == (Exception,), "must not inherit BaseException directly"
    assert not issubclass(QueueEmpty, QueueClosed)
    assert not issubclass(QueueClosed, QueueEmpty)


def test_queue_closed_is_an_exception():
    assert issubclass(QueueClosed, Exception)
    with pytest.raises(QueueClosed):
        raise QueueClosed()


# -- MemoryQueue ------------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_memory_queue_raises_when_nothing_arrives():
    queue = MemoryQueue(dequeue_timeout=0.05)

    started = time.monotonic()
    with pytest.raises(QueueEmpty):
        await queue.dequeue()
    assert time.monotonic() - started >= 0.05


@pytest.mark.asyncio
async def test_memory_queue_returns_immediately_when_work_exists():
    queue = MemoryQueue(dequeue_timeout=5.0)
    request = Request(URL)
    await queue.enqueue(request)

    started = time.monotonic()
    assert await queue.dequeue() is request
    assert time.monotonic() - started < 0.5


# -- PostgresQueue, with a pool that answers one call ------------------------------------------


class _Conn:
    def __init__(self, row, is_empty):
        self._row = row
        self._is_empty = is_empty

    async def fetchrow(self, sql, *args):
        return self._row

    async def fetchval(self, sql, *args):
        return self._is_empty


class _Acquire:
    def __init__(self, conn):
        self._conn = conn

    async def __aenter__(self):
        return self._conn

    async def __aexit__(self, *exc_info):
        return False


class _Pool:
    def __init__(self, row=None, is_empty=True):
        self.conn = _Conn(row, is_empty)

    def acquire(self):
        return _Acquire(self.conn)


@pytest.mark.asyncio
async def test_postgres_queue_raises_when_the_claim_returns_no_row():
    queue = PostgresQueue("postgresql://fake/db")
    queue._pool = _Pool(row=None, is_empty=True)

    with pytest.raises(QueueEmpty):
        await queue.dequeue()
    assert queue.is_empty is True


@pytest.mark.asyncio
async def test_postgres_queue_returns_the_claimed_request():
    queue = PostgresQueue("postgresql://fake/db")
    payload = queue._dump(Request(URL, priority=4, meta={"k": 1}))
    queue._pool = _Pool(row={"request_json": payload, "fingerprint": b"fp"}, is_empty=False)

    request = await queue.dequeue()

    assert request.url == URL
    assert request.priority == 4
    assert request.meta == {"k": 1}
    assert request._lease_token


# -- the crawl loop ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_loop_exits_after_queue_empty_once_the_queue_reports_idle():
    engine = _engine(_Spider())
    stub = _StubScheduler(idle_after=3)
    engine.scheduler = stub

    started = time.monotonic()
    stats = await engine.crawl()
    elapsed = time.monotonic() - started

    assert stub.dequeues == 3, "two waits, then the queue reports idle"
    assert elapsed >= 0.15, f"the retries must be spaced out ({elapsed:.3f}s)"
    assert stats.requests_count == 0


@pytest.mark.asyncio
async def test_loop_waits_when_the_queue_is_empty_but_work_may_follow():
    engine = _engine(_Spider())
    stub = _StubScheduler(idle_after=4)
    engine.scheduler = stub

    started = time.monotonic()
    await engine.crawl()
    elapsed = time.monotonic() - started

    assert stub.dequeues == 4, "a bare QueueEmpty must not end the crawl"
    assert elapsed >= 0.25, f"the loop has to sleep between retries, not spin ({elapsed:.3f}s)"


@pytest.mark.asyncio
async def test_loop_stops_immediately_when_the_queue_closes():
    engine = _engine(_Spider())
    stub = _StubScheduler(closed=True)
    engine.scheduler = stub

    await engine.crawl()

    assert stub.dequeues == 1, "a closed queue is terminal, no retry"
