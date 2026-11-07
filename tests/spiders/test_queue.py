"""Queue contract: dedup, priority, in-flight bookkeeping keyed by fingerprint."""

import inspect

import pytest

from scrapling.spiders.checkpoint import CheckpointData
from scrapling.spiders.queue import Queue
from scrapling.spiders.queues.memory import MemoryQueue
from scrapling.spiders.request import Request
from scrapling.spiders.scheduler import Scheduler


def _request(url: str, priority: int = 0) -> Request:
    return Request(url, priority=priority)


@pytest.mark.asyncio
async def test_enqueue_new_request_returns_true():
    queue = MemoryQueue()

    assert await queue.enqueue(_request("https://example.com/a")) is True
    assert len(queue) == 1


@pytest.mark.asyncio
async def test_duplicate_fingerprint_is_rejected():
    queue = MemoryQueue()

    assert await queue.enqueue(_request("https://example.com/a")) is True
    assert await queue.enqueue(_request("https://example.com/a")) is False
    assert len(queue) == 1


@pytest.mark.asyncio
async def test_dequeue_empties_the_queue():
    queue = MemoryQueue()
    await queue.enqueue(_request("https://example.com/a"))

    await queue.dequeue()

    assert queue.is_empty is True
    assert len(queue) == 0


@pytest.mark.asyncio
async def test_inflight_is_tracked_under_the_fingerprint():
    queue = MemoryQueue()
    request = _request("https://example.com/a")
    await queue.enqueue(request)

    dequeued = await queue.dequeue()

    fingerprint = dequeued.update_fingerprint()
    assert list(queue._inflight) == [fingerprint]
    # in-flight work is not "pending to hand out", but it is still in the checkpoint
    assert queue.is_empty is True
    assert (await queue.snapshot())[0] == [request]


@pytest.mark.asyncio
async def test_complete_clears_the_inflight_slot():
    queue = MemoryQueue()
    await queue.enqueue(_request("https://example.com/a"))
    request = await queue.dequeue()

    await queue.complete(request)

    assert queue._inflight == {}
    assert (await queue.snapshot())[0] == []


@pytest.mark.asyncio
async def test_higher_priority_comes_out_first():
    queue = MemoryQueue()
    await queue.enqueue(_request("https://example.com/low", priority=1))
    await queue.enqueue(_request("https://example.com/high", priority=5))

    first = await queue.dequeue()

    assert first.url == "https://example.com/high"


@pytest.mark.asyncio
async def test_equal_priority_is_fifo():
    queue = MemoryQueue()
    for index in range(3):
        await queue.enqueue(_request(f"https://example.com/{index}", priority=5))

    urls = [(await queue.dequeue()).url for _ in range(3)]

    assert urls == [f"https://example.com/{index}" for index in range(3)]


@pytest.mark.asyncio
async def test_snapshot_restore_round_trip():
    queue = MemoryQueue()
    urls = [f"https://example.com/{index}" for index in range(4)]
    for index, url in enumerate(urls):
        await queue.enqueue(_request(url, priority=index))

    pending, seen = await queue.snapshot()
    restored = MemoryQueue()
    await restored.restore(CheckpointData(requests=pending, seen=seen))

    assert [request.url for request in (await restored.snapshot())[0]] == [request.url for request in pending]
    assert (await restored.snapshot())[1] == seen
    assert await restored.enqueue(_request(urls[0])) is False, "seen set survives the round trip"


@pytest.mark.asyncio
async def test_complete_works_with_a_different_object_of_the_same_url():
    """The in-flight key is the fingerprint, so another process may hand back its own Request."""
    queue = MemoryQueue()
    await queue.enqueue(_request("https://example.com/a"))
    await queue.dequeue()

    stranger = _request("https://example.com/a")
    await queue.complete(stranger)

    assert queue._inflight == {}
    assert (await queue.snapshot())[0] == []


@pytest.mark.asyncio
async def test_memory_queue_satisfies_the_protocol_and_matches_the_shell():
    queue = MemoryQueue()
    shell = Scheduler()

    assert isinstance(queue, Queue)

    for target in (queue, shell):
        assert await target.enqueue(_request("https://example.com/x")) is True
        assert await target.enqueue(_request("https://example.com/x")) is False
        assert await target.enqueue(_request("https://example.com/y", priority=9)) is True

    assert (await queue.dequeue()).url == (await shell.dequeue()).url == "https://example.com/y"
    assert (await queue.dequeue()).url == (await shell.dequeue()).url == "https://example.com/x"
    assert queue.is_empty is shell.is_empty is True


def test_async_methods_are_coroutine_functions():
    """The contract is async end to end, so a networked backend can await inside it."""
    for name in ("enqueue", "dequeue", "complete", "snapshot", "restore"):
        assert inspect.iscoroutinefunction(getattr(MemoryQueue, name)), name


@pytest.mark.asyncio
async def test_awaited_lifecycle_matches_the_synchronous_semantics():
    queue = MemoryQueue()
    await queue.enqueue(_request("https://example.com/a"))
    request = await queue.dequeue()

    await queue.complete(request)
    assert queue._inflight == {}

    await queue.enqueue(_request("https://example.com/b"))
    pending, seen = await queue.snapshot()
    assert [r.url for r in pending] == ["https://example.com/b"]
    assert len(seen) == 2

    restored = MemoryQueue()
    await restored.restore(CheckpointData(requests=pending, seen=seen))
    assert [r.url for r in (await restored.snapshot())[0]] == ["https://example.com/b"]
