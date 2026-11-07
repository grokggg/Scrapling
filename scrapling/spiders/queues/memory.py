"""In-process queue: ``asyncio.PriorityQueue`` plus a fingerprint set.

This is the logic that used to live in ``Scheduler``, moved verbatim except for one thing:
in-flight slots are tracked by fingerprint rather than by ``id(request)``, so the bookkeeping
survives being described to another process.

State that checkpointing relies on is unchanged: ``snapshot()`` still returns
``(pending_requests, seen_set)`` with the queue order preserved, and ``restore()`` still
rebuilds from a ``CheckpointData``.
"""

import asyncio
from itertools import count

from scrapling.core.utils import log
from scrapling.spiders.request import Request
from scrapling.core._types import List, Set, Tuple, Literal, TYPE_CHECKING

if TYPE_CHECKING:
    from scrapling.spiders.checkpoint import CheckpointData

__all__ = ["MemoryQueue"]


class MemoryQueue:
    """Priority queue with URL deduplication, for a single process.

    Higher priority requests come out first; equal priorities are FIFO for ``bfs`` and LIFO for
    ``dfs``. Duplicate URLs are dropped unless ``dont_filter`` is set.
    """

    def __init__(
        self,
        include_kwargs: bool = False,
        include_headers: bool = False,
        keep_fragments: bool = False,
        crawl_strategy: Literal["bfs", "dfs"] = "bfs",
    ):
        if crawl_strategy not in ("bfs", "dfs"):
            raise ValueError("crawl_strategy must be 'bfs' or 'dfs'")
        self._queue: asyncio.PriorityQueue[tuple[int, int, Request]] = asyncio.PriorityQueue()
        self._seen: set[bytes] = set()
        self._lifo = crawl_strategy == "dfs"
        self._counter = count(step=-1 if self._lifo else 1)
        # Mirror dict for snapshot without draining queue
        self._pending: dict[int, tuple[int, int, Request]] = {}
        # fingerprint -> counters, so a completion from another process can find the record
        self._inflight: dict[bytes, list[int]] = {}
        self._include_kwargs = include_kwargs
        self._include_headers = include_headers
        self._keep_fragments = keep_fragments

    def _fingerprint(self, request: Request) -> bytes:
        """Cached inside the request: computing it again after a mutation returns the same bytes."""
        return request.update_fingerprint(self._include_kwargs, self._include_headers, self._keep_fragments)

    async def enqueue(self, request: Request) -> bool:
        """Add a request to the queue."""
        fingerprint = self._fingerprint(request)

        if not request.dont_filter and fingerprint in self._seen:
            log.debug("Dropped duplicate request: %s", request)
            return False

        self._seen.add(fingerprint)

        # Negative priority so higher priority = dequeued first
        counter = next(self._counter)
        item = (-request.priority, counter, request)
        self._pending[counter] = item
        await self._queue.put(item)
        return True

    async def dequeue(self) -> Request:
        """Get the next request to process (stays tracked until complete())."""
        _, counter, request = await self._queue.get()
        self._inflight.setdefault(self._fingerprint(request), []).append(counter)
        return request

    async def complete(self, request: Request) -> None:
        """Mark a request as finished so it stops being tracked for checkpoints."""
        fingerprint = self._fingerprint(request)
        counters = self._inflight.get(fingerprint)
        if not counters:
            return
        counter = counters.pop()
        if not counters:
            del self._inflight[fingerprint]
        self._pending.pop(counter, None)

    def __len__(self) -> int:
        return self._queue.qsize()

    @property
    def is_empty(self) -> bool:
        return self._queue.empty()

    async def snapshot(self) -> Tuple[List[Request], Set[bytes]]:
        """Create a snapshot of the current state for checkpoints."""
        sorted_items = sorted(self._pending.values(), key=lambda x: (x[0], x[1]))  # Maintain queue order
        requests = [item[2] for item in sorted_items]
        return requests, self._seen.copy()

    async def restore(self, data: "CheckpointData") -> None:
        """Restore scheduler state from checkpoint data.

        :param data: CheckpointData containing requests and seen set
        """
        self._seen = data.seen.copy()

        for request in reversed(data.requests) if self._lifo else data.requests:
            counter = next(self._counter)
            item = (-request.priority, counter, request)
            self._pending[counter] = item
            self._queue.put_nowait(item)

        log.info(f"Scheduler restored: {len(data.requests)} requests, {len(data.seen)} seen")
