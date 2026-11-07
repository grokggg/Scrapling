"""The runner: N worker threads pulling from the shared frontier.

Design notes that matter in production:

* **The frontier is the only shared mutable state.** Workers hold no queue of
  their own, so a worker dying does not strand work — its lease expires and the
  reaper puts the URL back.
* **Scope is checked before the request**, per URL, every time. A redirect that
  lands off-domain is caught on the *next* hop, not trusted from the seed.
* **Duplicates are detected on content**, not just on URL: five URLs serving the
  same article produce one item and four `dup` counters.
* **Every decision is audited** (allow / deny / fetch / retry) with a hash chain.
"""

from __future__ import annotations

import json
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Iterable, Optional, Protocol

from .dedupe import NearDupIndex, content_hash, simhash64
from .frontier import Frontier
from .guard import ScopeGuard
from .identity import Identity, IdentityPool, NoIdentityAvailable
from .telemetry import AuditLog, Metrics

# (url, identity) -> FetchResult
Fetcher = Callable[[str, Identity], "FetchResult"]
# (body, url) -> (item, links)
Extractor = Callable[[str, str], "tuple[dict, list[str]]"]
# item -> None
Store = Callable[[dict], None]


@dataclass
class FetchResult:
    ok: bool
    status: int = 0
    body: str = ""
    url: str = ""
    elapsed: float = 0.0
    error: Optional[str] = None


@dataclass
class RunReport:
    pages: int = 0
    items: int = 0
    duplicates: int = 0
    denied: int = 0
    errors: int = 0
    retried: int = 0
    elapsed: float = 0.0
    per_domain: dict[str, int] = field(default_factory=dict)
    stop_reason: str = ""

    def as_dict(self) -> dict:
        return {
            "pages": self.pages,
            "items": self.items,
            "duplicates": self.duplicates,
            "denied": self.denied,
            "errors": self.errors,
            "retried": self.retried,
            "elapsed_s": round(self.elapsed, 2),
            "per_domain": self.per_domain,
            "stop_reason": self.stop_reason,
        }


class JsonlStore:
    """Minimal append-only item store; swap for your DB of choice."""

    def __init__(self, path: str | Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()

    def __call__(self, item: dict) -> None:
        line = json.dumps(item, ensure_ascii=False) + "\n"
        with self._lock, self.path.open("a", encoding="utf-8") as handle:
            handle.write(line)


class OpsRunner:
    def __init__(
        self,
        frontier: Frontier,
        guard: ScopeGuard,
        pool: IdentityPool,
        fetch: Fetcher,
        extract: Extractor,
        store: Store,
        *,
        workers: int = 8,
        max_pages: int = 1000,
        lease: float = 120.0,
        near_dup_threshold: int = 6,
        metrics: Optional[Metrics] = None,
        audit: Optional[AuditLog] = None,
        reap_interval: float = 30.0,
    ):
        self.frontier = frontier
        self.guard = guard
        self.pool = pool
        self.fetch = fetch
        self.extract = extract
        self.store = store
        self.workers = max(1, workers)
        self.max_pages = max_pages
        self.lease = lease
        self.metrics = metrics or Metrics()
        self.audit = audit
        self.reap_interval = reap_interval
        self._near_dup = NearDupIndex(threshold=near_dup_threshold)
        self._report = RunReport()
        self._lock = threading.Lock()
        self._last_reap = 0.0
        for name, help_text in (
            ("pages_total", "Pages fetched"),
            ("items_total", "Items stored"),
            ("duplicates_total", "Content duplicates dropped"),
            ("denied_total", "URLs denied by scope guard"),
            ("errors_total", "Fetch/extract errors"),
            ("fetch_seconds", "Fetch latency in seconds"),
        ):
            self.metrics.describe(name, help_text)

    # ------------------------------------------------------------------ public
    def seed(self, urls: Iterable[str], *, priority: int = 10, depth: int = 0) -> int:
        return self.frontier.extend(urls, priority=priority, depth=depth)

    def run(self, seeds: Optional[Iterable[str]] = None, *, max_pages: Optional[int] = None) -> RunReport:
        if seeds:
            self.seed(seeds)
        limit = max_pages if max_pages is not None else self.max_pages
        started = time.time()
        threads = [
            threading.Thread(target=self._worker, args=(f"w{i}", limit), name=f"ops-w{i}", daemon=True)
            for i in range(self.workers)
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self._report.elapsed = time.time() - started
        self._report.stop_reason = self._report.stop_reason or ("max-pages" if self._report.pages >= limit else "frontier-empty")
        self.metrics.set("pages", self._report.pages)
        self.metrics.set("items", self._report.items)
        if self.audit:
            self.audit.write("run.end", **self._report.as_dict(), stats=self.frontier.stats().__dict__)
        return self._report

    # ------------------------------------------------------------------ worker
    def _worker(self, name: str, limit: int) -> None:
        while True:
            with self._lock:
                if self._report.pages >= limit:
                    return
            self._maybe_reap()
            claimed = self.frontier.claim(name, limit=1, lease=self.lease)
            if not claimed:
                # Nothing ready: either the run is done or peers hold leases.
                if self.frontier.is_empty():
                    return
                time.sleep(0.05)
                continue
            record = claimed[0]
            try:
                self._process(name, record, limit)
            except Exception as exc:  # never let one URL kill a worker
                self.metrics.inc("worker_crash_total")
                if self.audit:
                    self.audit.write("error", url=record.url, error=f"worker: {exc}", worker=name)
                try:
                    self.frontier.complete(record.url_hash, ok=False, error=f"worker: {exc}")
                except Exception:
                    self.frontier.release(record.url_hash)

    def _process(self, worker: str, record, limit: int) -> None:
        decision = self.guard.check(record.url, depth=record.depth)
        if not decision.allowed:
            if decision.reason == "rate-limit":
                self.frontier.release(record.url_hash)
                time.sleep(max(decision.wait, 0.01))
                return
            self.frontier.skip(record.url_hash, decision.reason)
            with self._lock:
                self._report.denied += 1
            self.metrics.inc("denied_total", reason=decision.reason)
            if self.audit:
                self.audit.write("deny", url=record.url, reason=decision.reason, worker=worker)
            return

        if self.audit:
            self.audit.write("fetch", url=record.url, worker=worker, depth=record.depth)
        try:
            identity = self.pool.pick(domain=record.domain)
        except NoIdentityAvailable as exc:
            self.frontier.release(record.url_hash)
            self.metrics.inc("errors_total", kind="no-identity")
            if self.audit:
                self.audit.write("error", url=record.url, error=str(exc), worker=worker)
            time.sleep(0.5)
            return

        result = self.fetch(record.url, identity)
        self.pool.report(identity, ok=result.ok, status=result.status, body=result.body if not result.ok else "")
        self.metrics.observe("fetch_seconds", result.elapsed or 0.0)

        if not result.ok:
            state = self.frontier.complete(
                record.url_hash,
                ok=False,
                error=result.error or f"http {result.status}",
                retry_after=_retry_after(result),
            )
            with self._lock:
                self._report.errors += 1
                if state == "pending":
                    self._report.retried += 1
            self.metrics.inc("errors_total", kind=f"http_{result.status or 'x'}")
            if self.audit:
                self.audit.write("retry" if state == "pending" else "fail", url=record.url, status=result.status, worker=worker)
            return

        try:
            item, links = self.extract(result.body, result.url or record.url)
        except Exception as exc:  # extractor bugs must not poison the queue
            self.frontier.complete(record.url_hash, ok=False, error=f"extract: {exc}")
            self.metrics.inc("errors_total", kind="extract")
            if self.audit:
                self.audit.write("error", url=record.url, error=f"extract: {exc}", worker=worker)
            with self._lock:
                self._report.errors += 1
            return

        text = item.get("text") or result.body
        digest = simhash64(text)
        dup = self._near_dup.add(item.get("url") or record.url, digest) if digest else None
        stored = 0
        if dup is not None:
            with self._lock:
                self._report.duplicates += 1
            self.metrics.inc("duplicates_total")
            if self.audit:
                self.audit.write("duplicate", url=record.url, of=dup.key, distance=dup.distance)
        else:
            item.setdefault("url", result.url or record.url)
            item["fetched_at"] = int(time.time())
            try:
                self.store(item)
                stored = 1
            except Exception as exc:
                self.metrics.inc("errors_total", kind="store")
                if self.audit:
                    self.audit.write("error", url=record.url, error=f"store: {exc}", worker=worker)

        with self._lock:
            self._report.pages += 1
            self._report.items += stored
            self._report.per_domain[record.domain] = self._report.per_domain.get(record.domain, 0) + 1
        self.metrics.inc("pages_total")
        if stored:
            self.metrics.inc("items_total")

        self.frontier.complete(record.url_hash, ok=True, simhash=digest, content_hash=content_hash(text))

        added = 0
        for link in links:
            if not link.lower().startswith(("http://", "https://")):
                continue
            # Every discovered URL enters the frontier; the guard — not this loop —
            # decides what is fetchable, so out-of-scope links leave an audit trail
            # instead of vanishing silently.
            if self.frontier.add(link, priority=max(0, record.priority - 1), depth=record.depth + 1, parent=record.url):
                added += 1
        if self.audit:
            self.audit.write("done", url=record.url, status=result.status, stored=stored, links_added=added, worker=worker)

    # ------------------------------------------------------------------ helper
    def _maybe_reap(self) -> None:
        now = time.time()
        if now - self._last_reap < self.reap_interval:
            return
        with self._lock:
            if now - self._last_reap < self.reap_interval:
                return
            self._last_reap = now
        recovered = self.frontier.reap()
        if recovered:
            self.metrics.inc("reaped_total", value=recovered)
            if self.audit:
                self.audit.write("reap", recovered=recovered)


def _host(url: str) -> str:
    from urllib.parse import urlsplit

    return urlsplit(url).hostname or ""


def _retry_after(result: FetchResult) -> Optional[float]:
    if result.status == 429:
        return 60.0
    return None
