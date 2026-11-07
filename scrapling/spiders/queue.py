"""Pluggable request queue.

``Scheduler`` used to own both the queue and the crawl bookkeeping. This module splits the
queue out so a backend can be swapped the same way ``Store`` is: pick an implementation, hand
it to the engine, and the crawl loop does not learn a single new name.

Two things about this contract are deliberately *not* the textbook shape, because the engine
already calls them that way and P11-A must not touch ``engine.py``:

* ``complete``, ``snapshot`` and ``restore`` are **synchronous** — ``CrawlerEngine`` calls them
  without ``await`` (``engine.py:695``, ``:719``, ``:747``).
* ``is_empty`` means "nothing left to hand out right now", i.e. the queue itself is empty. It
  does **not** include in-flight work: the crawl loop pairs it with its own ``_active_tasks``
  counter (``engine.py:847`` → ``:860``), and a queue that reported ``is_empty`` as False while
  a worker was still busy would send the loop into a blocking ``dequeue()`` on an empty queue.

A queue backed by the network therefore cannot implement this Protocol by awaiting a round trip
inside ``complete`` or ``snapshot``. Either it buffers those writes locally, or the engine grows
three awaits in P11-C. That is a decision for that batch, not a hidden detail of this one.

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
        """Take the next request. Blocks while the queue is empty; the backend decides."""
        ...

    def complete(self, request: Request) -> None:
        """Release the in-flight slot for this request, keyed by its fingerprint."""
        ...

    @property
    def is_empty(self) -> bool:
        """True when the queue holds nothing to hand out (in-flight work is not counted)."""
        ...

    def snapshot(self) -> Any:
        """Serialise the pending requests and the seen set, for checkpoints."""
        ...

    def restore(self, data: Any) -> None:
        """Rebuild state from a snapshot. The snapshot format is the backend's business."""
        ...
