"""Scheduler is a shell now: it must forward and add nothing."""

import pytest

from scrapling.spiders.checkpoint import CheckpointData
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

    scheduler.complete(request)

    assert scheduler._backend._inflight == {}
    assert scheduler.snapshot()[0] == []


def test_is_empty_reads_the_backend():
    scheduler = Scheduler()

    assert scheduler.is_empty is True
    assert scheduler.is_empty is scheduler._backend.is_empty


@pytest.mark.asyncio
async def test_snapshot_and_restore_forward():
    scheduler = Scheduler()
    await scheduler.enqueue(Request("https://example.com/a", priority=3))
    await scheduler.enqueue(Request("https://example.com/b", priority=1))

    pending, seen = scheduler.snapshot()
    assert pending == scheduler._backend.snapshot()[0]
    assert seen == scheduler._backend.snapshot()[1]

    restored = Scheduler()
    restored.restore(CheckpointData(requests=pending, seen=seen))

    assert [request.url for request in restored.snapshot()[0]] == [request.url for request in pending]


def test_len_reads_the_backend():
    scheduler = Scheduler()

    assert len(scheduler) == len(scheduler._backend) == 0
