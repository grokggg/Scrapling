"""Backends for :mod:`scrapling.spiders.store`.

Pick by where the results should outlive the process:

    MemoryStore    — a dict, gone with the run
    SQLiteStore    — one file, indexed on content_hash
    PostgresStore  — shared across machines (needs ``pip install scrapling[pg]``)
"""

from scrapling.spiders.stores.memory import MemoryStore
from scrapling.spiders.stores.postgres import PostgresStore
from scrapling.spiders.stores.sqlite import SQLiteStore

__all__ = ["MemoryStore", "PostgresStore", "SQLiteStore"]
