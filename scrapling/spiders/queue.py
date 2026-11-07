"""Pluggable request queue.

``Scheduler`` used to own both the queue and the crawl bookkeeping. This module splits the
queue out so a backend can be swapped the same way ``Store`` is: pick an implementation, hand
it to the engine, and the crawl loop does not learn a single new name.

Every method that can touch a backend is ``async`` — including ``complete``, ``snapshot`` and
``restore``, which the engine awaits (``engine.py:695``, ``:719``, ``:747``). A PostgreSQL queue
can therefore write its ack and read its checkpoint inside the same contract, with no further
change to the engine.

``is_empty`` stays synchronous and means "nothing left to hand out right now", i.e. the queue
itself is empty. It deliberately does **not** include in-flight work: the crawl loop pairs it
with its own ``_active_tasks`` counter (``engine.py:847`` → ``:860``), and a queue reporting
``is_empty`` as False while a worker was still busy would send the loop into a blocking
``dequeue()`` on an empty queue.

In-flight bookkeeping is keyed by ``request.fingerprint``, never ``id(request)``: the same URL
seen from another process must resolve to the same record, and ``Request.update_fingerprint``
caches its result, so the key stays stable even after the engine mutates session kwargs.
"""

from typing import Any, Protocol, runtime_checkable

from scrapling.spiders.request import Request

__all__ = ["Queue"]


@runtime_checkable
class Queue(Protocol):
    """What the engine needs from a queue. ``MemoryQueue`` and a future PostgreSQL one agree here."""

    async def enqueue(self, request: Request) -> bool:
        """Queue a request. True when new, False when the fingerprint was already seen."""
        ...

    async def dequeue(self) -> Request:
        """Take the next request.

        Raises ``QueueEmpty`` when there is nothing to hand out right now and ``QueueClosed``
        when no further work can ever arrive. An implementation may wait for a while before
        giving up (``MemoryQueue`` waits ``dequeue_timeout``) or answer immediately
        (``PostgresQueue`` raises as soon as the claim returns no row).
        """
        ...

    async def complete(self, request: Request) -> None:
        """Release the in-flight slot for this request, keyed by its fingerprint."""
        ...

    @property
    def is_empty(self) -> bool:
        """True when the queue holds nothing to hand out (in-flight work is not counted)."""
        ...

    async def snapshot(self) -> Any:
        """Serialise the pending requests and the seen set, for checkpoints."""
        ...

    async def restore(self, data: Any) -> None:
        """Rebuild state from a snapshot. The snapshot format is the backend's business."""
        ...
