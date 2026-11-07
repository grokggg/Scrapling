"""Scheduler is a shell now: it must forward and add nothing."""

import inspect

import pytest

from scrapling.spiders.checkpoint import CheckpointData
from scrapling.spiders.queue_errors import QueueEmpty
from scrapling.spiders.queues.memory import MemoryQueue
from scrapling.spiders.request import Request
from scrapling.spiders.scheduler import Scheduler


def test_backend_is_a_memory_queue():
    scheduler = Scheduler()

    assert isinstance(scheduler._backend, MemoryQueue)


def test_invalid_strategy_still_raises_from_the_backend():
    with pytest.raises(ValueError, match="crawl_strategy must be 'bfs' or 'dfs'"):
        Scheduler(crawl_strategy="random")


@pytest.mark.asyncio
async def test_enqueue_and_dequeue_forward():
    scheduler = Scheduler()
    request = Request("https://example.com/a")

    assert await scheduler.enqueue(request) is True
    assert await scheduler.enqueue(Request("https://example.com/a")) is False
    assert await scheduler.dequeue() is request


@pytest.mark.asyncio
async def test_complete_forwards():
    scheduler = Scheduler()
    await scheduler.enqueue(Request("https://example.com/a"))
    request = await scheduler.dequeue()

    await scheduler.complete(request)

    assert scheduler._backend._inflight == {}
    assert (await scheduler.snapshot())[0] == []


def test_is_empty_reads_the_backend():
    scheduler = Scheduler()

    assert scheduler.is_empty is True
    assert scheduler.is_empty is scheduler._backend.is_empty


@pytest.mark.asyncio
async def test_snapshot_and_restore_forward():
    scheduler = Scheduler()
    await scheduler.enqueue(Request("https://example.com/a", priority=3))
    await scheduler.enqueue(Request("https://example.com/b", priority=1))

    pending, seen = await scheduler.snapshot()
    assert pending == (await scheduler._backend.snapshot())[0]
    assert seen == (await scheduler._backend.snapshot())[1]

    restored = Scheduler()
    await restored.restore(CheckpointData(requests=pending, seen=seen))

    assert [request.url for request in (await restored.snapshot())[0]] == [request.url for request in pending]


def test_len_reads_the_backend():
    scheduler = Scheduler()

    assert len(scheduler) == len(scheduler._backend) == 0


class RecordingBackend:
    """Records every call, and never touches the in-memory implementation."""

    def __init__(self):
        self.calls = []

    async def enqueue(self, request):
        self.calls.append(("enqueue", request.url))
        return True

    async def dequeue(self):
        self.calls.append(("dequeue", None))
        return Request("https://example.com/from-backend")

    async def complete(self, request):
        self.calls.append(("complete", request.url))

    @property
    def is_empty(self):
        return True

    async def snapshot(self):
        self.calls.append(("snapshot", None))
        return ([], set())

    async def restore(self, data):
        self.calls.append(("restore", data))


@pytest.mark.asyncio
async def test_every_method_reaches_an_injected_backend():
    backend = RecordingBackend()
    scheduler = Scheduler(backend=backend)
    request = Request("https://example.com/a")

    await scheduler.enqueue(request)
    await scheduler.dequeue()
    await scheduler.complete(request)
    await scheduler.snapshot()
    await scheduler.restore("sentinel")

    assert backend.calls == [
        ("enqueue", "https://example.com/a"),
        ("dequeue", None),
        ("complete", "https://example.com/a"),
        ("snapshot", None),
        ("restore", "sentinel"),
    ]
    assert not isinstance(scheduler._backend, MemoryQueue), "an injected backend replaces the default"


@pytest.mark.asyncio
async def test_complete_is_a_coroutine_and_hits_the_backend_once():
    backend = RecordingBackend()
    scheduler = Scheduler(backend=backend)

    assert inspect.iscoroutinefunction(Scheduler.complete)
    await scheduler.complete(Request("https://example.com/a"))

    assert backend.calls == [("complete", "https://example.com/a")]


@pytest.mark.asyncio
async def test_dequeue_forwards_queue_empty():
    scheduler = Scheduler(backend=MemoryQueue(dequeue_timeout=0.02))

    with pytest.raises(QueueEmpty):
        await scheduler.dequeue()
