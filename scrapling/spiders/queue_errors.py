"""Queue-side conditions the crawl loop has to tell apart.

Both are ordinary exceptions: a queue signalling "nothing right now" is a normal event, not a
``BaseException`` that should slip past ``except Exception`` handlers.
"""

__all__ = ["QueueEmpty", "QueueClosed"]


class QueueEmpty(Exception):
    """Nothing to hand out right now.

    The crawl loop decides what to do: stop when nothing is in flight either, wait and retry when
    another worker may still be producing work.
    """


class QueueClosed(Exception):
    """The queue has been shut down; no further work will ever arrive."""
