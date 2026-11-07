"""Domain-level adaptive escalation: how hard are we allowed to push this host?

The state machine is deliberately boring and explicit — a risk score accumulates from
observed anti-bot signals, one upgrade per threshold breach, a cooling window after
each upgrade, and a slow, streak-gated descent back down. Two knobs make the same code
serve a cautious production crawl (`presets.ENGINEERING_PRESET`) and an aggressive one
(`presets.APT_PRESET`); nothing here knows about HTTP, so it is all unit-testable.

Levels are ordered by cost, and PROXY is conditional: escalating into a proxy tier
with no proxy pool is a downgrade in disguise, so it collapses back to TLS.
"""

from __future__ import annotations

import threading
import time
from collections import defaultdict
from dataclasses import dataclass, field
from enum import IntEnum
from typing import Callable, Dict, Optional, Tuple

from scrapling.spiders.signals import BlockSignal

__all__ = ["AdaptiveConfig", "AdaptiveEngine", "DomainProfile", "FetchLevel"]


class FetchLevel(IntEnum):
    """Escalation ladder, cheapest first."""

    DIRECT = 0  # 裸请求
    HEADERS = 1  # + 完整 headers
    TLS = 2  # + curl_cffi impersonate
    PROXY = 3  # + 代理
    BROWSER = 4  # Playwright


@dataclass
class DomainProfile:
    """Everything we know about one host. Mutated under `AdaptiveEngine._lock` only."""

    domain: str
    level: FetchLevel = FetchLevel.DIRECT
    risk: float = 0.0  # 0-100，风险累积
    cooling_until: float = 0.0  # 冷却到期时间戳
    success_streak: int = 0  # 连续成功次数
    signal_counts: Dict[str, int] = field(default_factory=lambda: defaultdict(int))
    reset_pending: bool = False  # BROWSER 满级后仍被打：换会话，不再升级
    reset_count: int = 0  # 该域名累计重置次数（跨越 profile 生命周期）


@dataclass(frozen=True)
class AdaptiveConfig:
    """Thresholds. Swap the whole object; never mutate a running engine's knobs."""

    # 信号 → 风险增量
    risk_delta: Dict[str, float] = field(
        default_factory=lambda: {
            "CAPTCHA": 60.0,
            "LOGIN_WALL": 40.0,
            "RATELIMIT": 50.0,
            "FORBIDDEN": 55.0,
            "CHALLENGE": 70.0,
            "EMPTY": 20.0,
        }
    )
    # 升级阈值
    upgrade_at_risk: float = 30.0  # risk >= 30 → 升 1 级
    max_risk: float = 100.0
    # 降级条件
    downgrade_after_success: int = 10  # 连续成功 N 次降 1 级
    # 冷却期（秒）
    cooldown_seconds: float = 60.0
    # 每级 delay（秒）
    level_delays: Tuple[float, ...] = (0.0, 0.1, 0.3, 1.0, 2.0)
    # 每级并发上限
    level_concurrency: Tuple[int, ...] = (10, 8, 6, 4, 2)

    def __post_init__(self) -> None:
        levels = len(FetchLevel)
        if len(self.level_delays) != levels or len(self.level_concurrency) != levels:
            raise ValueError(f"level_delays/level_concurrency must have {levels} entries")
        if self.upgrade_at_risk <= 0 or self.max_risk <= 0:
            raise ValueError("upgrade_at_risk and max_risk must be positive")
        if self.downgrade_after_success < 1:
            raise ValueError("downgrade_after_success must be >= 1")
        if self.cooldown_seconds < 0:
            raise ValueError("cooldown_seconds must be >= 0")


class AdaptiveEngine:
    """Per-domain escalation state, safe to call from worker threads.

    :param config: thresholds; defaults to `AdaptiveConfig()`.
    :param proxy_available: called with the lock held; returning False collapses the
        PROXY tier back to TLS. `None` means "assume a proxy pool exists".
    :param clock: monotonic seconds, injectable so cooling can be tested without sleeping.
    """

    def __init__(
        self,
        config: Optional[AdaptiveConfig] = None,
        *,
        proxy_available: Optional[Callable[[], bool]] = None,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.config = config or AdaptiveConfig()
        self._profiles: Dict[str, DomainProfile] = {}
        # Survives profile deletion: reset counts are domain history, not session state.
        self._reset_history: Dict[str, int] = {}
        self._lock = threading.Lock()
        self._proxy_available = proxy_available
        self._clock = clock

    def plan(self, domain: str) -> Tuple[FetchLevel, float, int]:
        """返回 (当前等级, 该等级 delay, 该等级并发上限)。

        Read-only: the PROXY→TLS collapse is reflected in the answer, not in the stored
        profile, so the level comes back automatically once a proxy reappears.
        """
        with self._lock:
            profile = self._profile(domain)
            level = profile.level
            if level == FetchLevel.PROXY and not self._has_proxy():
                level = FetchLevel.TLS
            return level, self.config.level_delays[level], self.config.level_concurrency[level]

    def observe(self, domain: str, signal: BlockSignal, confidence: float) -> None:
        """收到信号后更新状态：累积风险、升级、设冷却。"""
        if signal is BlockSignal.NONE:
            return

        with self._lock:
            profile = self._profile(domain)
            profile.signal_counts[signal.name] += 1

            delta = self.config.risk_delta.get(signal.name, 0.0)
            profile.risk = min(self.config.max_risk, profile.risk + delta * max(0.4, confidence))

            if profile.risk >= self.config.upgrade_at_risk and profile.level < FetchLevel.BROWSER:
                profile.level = FetchLevel(profile.level + 1)
                profile.cooling_until = self._clock() + self.config.cooldown_seconds
                # Halve instead of reset: a second signal still escalates, at a slower pace.
                profile.risk *= 0.5
                if profile.level == FetchLevel.PROXY and not self._has_proxy():
                    profile.level = FetchLevel.TLS

            # Full ladder and still saturated: no more escalation exists, so the next
            # move is a fresh session rather than a higher tier.
            if profile.level == FetchLevel.BROWSER and profile.risk >= self.config.max_risk:
                profile.reset_pending = True

    def observe_success(self, domain: str) -> None:
        """成功响应后调用：success_streak+1，冷却期外够 N 次则降级。"""
        with self._lock:
            profile = self._profile(domain)
            profile.success_streak += 1

            if (
                self._clock() > profile.cooling_until
                and profile.success_streak >= self.config.downgrade_after_success
                and profile.level > FetchLevel.DIRECT
            ):
                profile.level = FetchLevel(profile.level - 1)
                profile.success_streak = 0

    def should_reset(self, domain: str) -> bool:
        """返回该域名是否需要重置会话（BROWSER 满级且 risk 仍满时）"""
        with self._lock:
            profile = self._profiles.get(domain)
            return bool(profile.reset_pending) if profile is not None else False

    def reset_session(self, domain: str) -> None:
        """会话重置后调用：清空该域名的 profile，reset_count+1，回落到 DIRECT"""
        with self._lock:
            self._profiles.pop(domain, None)
            self._reset_history[domain] = self._reset_history.get(domain, 0) + 1

    def stats(self) -> Dict[str, Dict[str, object]]:
        """返回所有域名的当前状态，用于可观测。"""
        with self._lock:
            return {
                domain: {
                    "level": int(profile.level),
                    "level_name": profile.level.name,
                    "risk": profile.risk,
                    "cooling_until": profile.cooling_until,
                    "success_streak": profile.success_streak,
                    "signal_counts": dict(profile.signal_counts),
                    "reset_pending": profile.reset_pending,
                    "reset_count": profile.reset_count,
                }
                for domain, profile in self._profiles.items()
            }

    def _profile(self, domain: str) -> DomainProfile:
        profile = self._profiles.get(domain)
        if profile is None:
            # A profile born after a reset still reports the domain's reset history.
            profile = self._profiles[domain] = DomainProfile(domain, reset_count=self._reset_history.get(domain, 0))
        return profile

    def _has_proxy(self) -> bool:
        return True if self._proxy_available is None else bool(self._proxy_available())
