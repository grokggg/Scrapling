"""Scope guard: the only thing standing between "a crawl" and "an incident".

A crawler that can go anywhere will eventually go somewhere it must not: an
account page, an admin path, a `?delete=` endpoint, or a domain outside the
engagement. The guard makes scope an explicit, testable object instead of a
convention, and it is consulted *before* every fetch — not after a 403.

Policies enforced here:
* domain allow-list (suffix match, `*.example.com` style)
* path deny-list (regex, checked on the raw path + query)
* maximum depth from the seed
* per-domain token bucket (politeness ceiling, independent of `AutoThrottle`)
* per-domain daily request budget (stops runaway crawls overnight)
* optional robots.txt hook (callable returning bool)
"""

from __future__ import annotations

import re
import threading
import time
from dataclasses import dataclass
from typing import Callable, Iterable, Optional
from urllib.parse import urlsplit


@dataclass(frozen=True)
class Decision:
    allowed: bool
    reason: str = "ok"
    wait: float = 0.0

    def __bool__(self) -> bool:
        return self.allowed


class _Bucket:
    """Token bucket, refilled by elapsed time."""

    __slots__ = ("rate", "burst", "tokens", "stamp")

    def __init__(self, rate: float, burst: float):
        self.rate = rate
        self.burst = burst
        self.tokens = burst
        self.stamp = time.monotonic()

    def take(self, amount: float = 1.0) -> float:
        now = time.monotonic()
        self.tokens = min(self.burst, self.tokens + (now - self.stamp) * self.rate)
        self.stamp = now
        if self.tokens >= amount:
            self.tokens -= amount
            return 0.0
        return (amount - self.tokens) / self.rate


class ScopeGuard:
    def __init__(
        self,
        allow_domains: Iterable[str],
        *,
        deny_paths: Iterable[str] = (),
        max_depth: int = 5,
        per_domain_qps: float = 1.0,
        per_domain_burst: float = 3.0,
        daily_budget: Optional[int] = None,
        robots_allowed: Optional[Callable[[str], bool]] = None,
        require_https: bool = False,
    ):
        self.allow_domains = tuple(d.lower().lstrip(".") for d in allow_domains)
        if not self.allow_domains:
            raise ValueError("ScopeGuard needs at least one allowed domain — an empty scope means no crawling")
        self.deny = tuple(re.compile(p, re.I) for p in deny_paths)
        self.max_depth = max_depth
        self.per_domain_qps = per_domain_qps
        self.per_domain_burst = per_domain_burst
        self.daily_budget = daily_budget
        self.robots_allowed = robots_allowed
        self.require_https = require_https
        self._buckets: dict[str, _Bucket] = {}
        self._counts: dict[str, int] = {}
        self._lock = threading.Lock()
        self._checks = 0
        self._denied = 0

    # ------------------------------------------------------------------ checks
    def in_scope_host(self, host: str) -> bool:
        host = (host or "").lower()
        for allowed in self.allow_domains:
            if host == allowed or host.endswith("." + allowed):
                return True
        return False

    def check(self, url: str, *, depth: int = 0) -> Decision:
        self._checks += 1
        parts = urlsplit(url)

        if parts.scheme not in ("http", "https"):
            return self._deny("scheme")
        if self.require_https and parts.scheme != "https":
            return self._deny("https-required")
        if not self.in_scope_host(parts.hostname or ""):
            return self._deny("off-domain")

        target = f"{parts.path}?{parts.query}" if parts.query else (parts.path or "/")
        for pattern in self.deny:
            if pattern.search(target):
                return self._deny("deny-pattern")

        if depth > self.max_depth:
            return self._deny("max-depth")

        if self.robots_allowed is not None and not self.robots_allowed(url):
            return self._deny("robots")

        host = (parts.hostname or "").lower()
        with self._lock:
            if self.daily_budget is not None and self._counts.get(host, 0) >= self.daily_budget:
                return self._deny("daily-budget")
            bucket = self._buckets.get(host)
            if bucket is None:
                bucket = self._buckets[host] = _Bucket(self.per_domain_qps, self.per_domain_burst)
        wait = bucket.take()
        if wait > 0:
            return Decision(False, "rate-limit", wait)
        with self._lock:
            self._counts[host] = self._counts.get(host, 0) + 1
        return Decision(True, "ok", 0.0)

    def _deny(self, reason: str) -> Decision:
        self._denied += 1
        return Decision(False, reason, 0.0)

    # ------------------------------------------------------------------- stats
    def stats(self) -> dict:
        with self._lock:
            return {
                "checks": self._checks,
                "denied": self._denied,
                "domains": dict(self._counts),
            }


DEFAULT_DENY_PATTERNS = (
    r"/admin(/|$)",
    r"/wp-admin(/|$)",
    r"/login(/|$)",
    r"/signin(/|$)",
    r"/logout(/|$)",
    r"/account(/|$)",
    r"/checkout(/|$)",
    r"[?&](delete|remove|destroy|logout)=",
    r"/api/v\d+/(token|keys?|secrets?)(/|$)",
    r"\.(env|pem|key|p12|sql|bak)$",
)
