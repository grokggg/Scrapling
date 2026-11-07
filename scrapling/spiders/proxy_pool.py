"""Proxy pool: scored selection, domain affinity, cooling and health checks.

Pure logic — no network, no IO of its own. `health_check_all()` only calls the checker
you inject, so the pool can be exercised without a proxy in sight.
"""

from __future__ import annotations

import inspect
from dataclasses import dataclass, field
from enum import Enum, auto
from random import choices as random_choices
from threading import RLock
from time import monotonic
from typing import Callable, Dict, Iterable, List, Optional

import anyio

__all__ = ["ProxyEntry", "ProxyPool", "ProxyPoolConfig", "ProxyState"]


class ProxyState(Enum):
    HEALTHY = auto()  # 可正常使用
    DEGRADED = auto()  # 连续失败 1-2 次，降权
    COOLING = auto()  # 连续失败 3 次，冷却中
    DEAD = auto()  # 连续失败 5 次，永不复用


@dataclass
class ProxyEntry:
    url: str  # "http://user:pass@host:port"
    state: ProxyState = ProxyState.HEALTHY
    success_count: int = 0
    fail_count: int = 0
    consecutive_fail: int = 0
    last_used_at: float = 0.0
    last_checked_at: float = 0.0
    score: float = 100.0  # 0-100
    avg_latency_ms: float = 0.0
    cooling_until: float = 0.0
    domain_stats: dict = field(default_factory=dict)  # domain -> {success, fail}


@dataclass(frozen=True)
class ProxyPoolConfig:
    health_check_interval: float = 60.0  # 秒，探活间隔
    cooling_seconds: float = 300.0  # 冷却时长
    max_consecutive_fail: int = 3  # 到这个数进冷却
    dead_after_fails: int = 5  # 到这个数判死
    min_score: float = 20.0  # 低于这个分不进选池
    prefer_domain_history: bool = True  # 优先用在该域名上历史好的代理
    health_check_url: str = "http://httpbin.org/ip"
    health_check_timeout: float = 10.0


def _domain_bonus(entry: ProxyEntry, domain: str) -> float:
    """+0.5 for a proxy that owns this host, -0.5 for one that keeps failing on it."""
    if not domain:
        return 0.0
    seen = entry.domain_stats.get(domain)
    if not seen:
        return 0.0
    total = seen.get("success", 0) + seen.get("fail", 0)
    if not total:
        return 0.0
    ratio = seen.get("success", 0) / total
    if ratio > 0.8:
        return 0.5
    if ratio < 0.3:
        return -0.5
    return 0.0


class ProxyPool:
    """A scored pool of proxies with per-domain affinity."""

    def __init__(
        self,
        config: Optional[ProxyPoolConfig] = None,
        *,
        health_checker: Optional[Callable] = None,
        clock: Optional[Callable[[], float]] = None,
    ) -> None:
        self.config = config or ProxyPoolConfig()
        self._health_checker = health_checker
        self._clock = clock or monotonic
        self._entries: Dict[str, ProxyEntry] = {}
        self._lock = RLock()

    # ---------------------------------------------------------------- inventory

    def add(self, url: str) -> None:
        """Add a proxy. Re-adding an existing URL resets nothing."""
        if not url:
            raise ValueError("proxy url must not be empty")
        with self._lock:
            self._entries.setdefault(url, ProxyEntry(url))

    def add_many(self, urls: Iterable[str]) -> None:
        for url in urls:
            self.add(url)

    def remove(self, url: str) -> None:
        with self._lock:
            self._entries.pop(url, None)

    def __len__(self) -> int:
        with self._lock:
            return len(self._entries)

    def __contains__(self, url: str) -> bool:
        with self._lock:
            return url in self._entries

    def get(self, url: str) -> Optional[ProxyEntry]:
        with self._lock:
            return self._entries.get(url)

    # ---------------------------------------------------------------- selection

    def acquire(self, domain: str = "") -> Optional[str]:
        """按评分选一个可用代理返回 url，无可用返回 None。加锁。"""
        with self._lock:
            eligible = [entry for entry in self._entries.values() if self._usable(entry)]
            if not eligible:
                return None
            picked = self._pick_by_score(eligible, domain)
            picked.last_used_at = self._clock()
            return picked.url

    def _usable(self, entry: ProxyEntry) -> bool:
        """Also the single place where a cooling period is allowed to expire."""
        if entry.state is ProxyState.DEAD:
            return False

        if entry.state is ProxyState.COOLING:
            if self._clock() < entry.cooling_until:
                return False
            entry.state = ProxyState.DEGRADED
            entry.score = max(self.config.min_score, entry.score)
            entry.consecutive_fail = 0

        return entry.score >= self.config.min_score

    def _pick_by_score(self, candidates: List[ProxyEntry], domain: str) -> ProxyEntry:
        """加权随机：score * (1 + domain_bonus)，domain_bonus 来自域名历史。"""
        prefer = self.config.prefer_domain_history
        weights = [
            max(0.1, entry.score * (1 + (_domain_bonus(entry, domain) if prefer else 0.0)))
            for entry in candidates
        ]
        return random_choices(candidates, weights=weights, k=1)[0]

    # ------------------------------------------------------------------ feedback

    def release(self, url: str, *, success: bool, latency_ms: float = 0.0, domain: str = "") -> None:
        """报告使用结果，更新评分、状态、冷却、域名历史。加锁。"""
        with self._lock:
            entry = self._entries.get(url)
            if entry is None:
                return

            if success:
                entry.success_count += 1
                entry.consecutive_fail = 0
                entry.score = min(100.0, entry.score + 5.0)
                if entry.state in (ProxyState.DEGRADED, ProxyState.COOLING):
                    entry.state = ProxyState.HEALTHY
                    entry.cooling_until = 0.0
                entry.avg_latency_ms = (
                    latency_ms if entry.avg_latency_ms == 0.0 else 0.7 * entry.avg_latency_ms + 0.3 * latency_ms
                )
            else:
                entry.fail_count += 1
                entry.consecutive_fail += 1
                entry.score = max(0.0, entry.score - 15.0)
                if entry.fail_count >= self.config.dead_after_fails:
                    entry.state = ProxyState.DEAD
                elif entry.consecutive_fail >= self.config.max_consecutive_fail:
                    entry.state = ProxyState.COOLING
                    entry.cooling_until = self._clock() + self.config.cooling_seconds
                else:
                    entry.state = ProxyState.DEGRADED

            if domain:
                seen = entry.domain_stats.setdefault(domain, {"success": 0, "fail": 0})
                seen["success" if success else "fail"] += 1

    # ------------------------------------------------------------------- probing

    async def health_check_all(self) -> Dict[str, bool]:
        """并发探活所有代理，更新状态。返回 {url: ok}。

        Without a checker there is nothing to probe: returns {} and touches no state.
        """
        if self._health_checker is None:
            return {}

        with self._lock:
            urls = [url for url, entry in self._entries.items() if entry.state is not ProxyState.DEAD]

        results: Dict[str, bool] = {}

        async def probe(url: str) -> None:
            with anyio.fail_after(self.config.health_check_timeout):
                outcome = self._health_checker(url)
                if inspect.isawaitable(outcome):
                    outcome = await outcome
            results[url] = bool(outcome)

        async with anyio.create_task_group() as group:
            for url in urls:
                group.start_soon(probe, url)

        now = self._clock()
        with self._lock:
            for url, ok in results.items():
                entry = self._entries.get(url)
                if entry is None:
                    continue
                entry.last_checked_at = now
                self._apply_probe(entry, ok)
        return results

    def _apply_probe(self, entry: ProxyEntry, ok: bool) -> None:
        if ok:
            entry.consecutive_fail = 0
            entry.score = min(100.0, entry.score + 5.0)
            entry.state = ProxyState.HEALTHY
            entry.cooling_until = 0.0
            return

        entry.fail_count += 1
        entry.consecutive_fail += 1
        entry.score = max(0.0, entry.score - 15.0)
        if entry.fail_count >= self.config.dead_after_fails:
            entry.state = ProxyState.DEAD
        elif entry.consecutive_fail >= self.config.max_consecutive_fail:
            entry.state = ProxyState.COOLING
            entry.cooling_until = self._clock() + self.config.cooling_seconds
        else:
            entry.state = ProxyState.DEGRADED

    # ---------------------------------------------------------------------- view

    def stats(self) -> dict:
        """返回池统计：total / healthy / degraded / cooling / dead / avg_score。"""
        with self._lock:
            entries = list(self._entries.values())
            counts = {state: 0 for state in ProxyState}
            for entry in entries:
                counts[entry.state] += 1
            return {
                "total": len(entries),
                "healthy": counts[ProxyState.HEALTHY],
                "degraded": counts[ProxyState.DEGRADED],
                "cooling": counts[ProxyState.COOLING],
                "dead": counts[ProxyState.DEAD],
                "avg_score": (sum(entry.score for entry in entries) / len(entries)) if entries else 0.0,
            }
