"""scrapling_ops — multi-worker crawl operations layer on top of Scrapling.

Scope note: everything here is for **authorised collection only**. The guard module
is not optional decoration — it is the contract that keeps a run inside the scope
you were allowed to touch. There is deliberately no auth-bypass, credential, or
intrusion code in this package.
"""

from importlib.metadata import PackageNotFoundError, version as _dist_version

from .dedupe import NearDupIndex, content_hash, hamming64, simhash64
from .frontier import Frontier, FrontierStats, UrlRecord
from .guard import Decision, ScopeGuard
from .identity import Identity, IdentityPool
from .telemetry import AuditLog, Metrics, redact

try:
    __version__ = _dist_version("scrapling-ops")
except PackageNotFoundError:  # running from a source checkout
    __version__ = "0.1.0"

__all__ = [
    "AuditLog",
    "Decision",
    "Frontier",
    "FrontierStats",
    "Identity",
    "IdentityPool",
    "Metrics",
    "NearDupIndex",
    "ScopeGuard",
    "UrlRecord",
    "content_hash",
    "hamming64",
    "redact",
    "simhash64",
    "__version__",
]
