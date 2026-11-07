"""Backends for :mod:`scrapling.spiders.queue`.

Pick by where the queue should live:

    MemoryQueue    — process-local, the default (and the only one so far)
"""

from scrapling.spiders.queues.memory import MemoryQueue

__all__ = ["MemoryQueue"]
