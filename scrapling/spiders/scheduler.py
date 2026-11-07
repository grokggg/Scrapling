"""The crawl queue the engine talks to.

The implementation moved to :class:`scrapling.spiders.queues.memory.MemoryQueue` in P11-A so a
distributed backend can slot in without the engine noticing. This class stays because the engine
constructs it by name and because every existing import of ``Scheduler`` should keep working;
it forwards, it does not decide anything.
"""

from typing import Any, Literal, Optional

from scrapling.spiders.queues.memory import MemoryQueue
from scrapling.spiders.request import Request
from scrapling.core._types import TYPE_CHECKING

if TYPE_CHECKING:
    from scrapling.spiders.checkpoint import CheckpointData

__all__ = ["Scheduler"]


class Scheduler:
    """Thin shell over a queue backend. Defaults to the in-process one."""

    def __init__(
        self,
        backend: Optional[Any] = None,
        include_kwargs: bool = False,
        include_headers: bool = False,
        keep_fragments: bool = False,
        crawl_strategy: Literal["bfs", "dfs"] = "bfs",
    ):
        self._backend: Any = backend if backend is not None else MemoryQueue(
            include_kwargs=include_kwargs,
            include_headers=include_headers,
            keep_fragments=keep_fragments,
            crawl_strategy=crawl_strategy,
        )

    async def enqueue(self, request: Request) -> bool:
        return await self._backend.enqueue(request)

    async def dequeue(self) -> Request:
        return await self._backend.dequeue()

    async def complete(self, request: Request) -> None:
        return await self._backend.complete(request)

    def __len__(self) -> int:
        return len(self._backend)

    @property
    def is_empty(self) -> bool:
        return self._backend.is_empty

    async def snapshot(self) -> Any:
        return await self._backend.snapshot()

    async def restore(self, data: "CheckpointData") -> None:
        return await self._backend.restore(data)
