"""Request pacing: human-shaped intervals, not just a fixed delay.

The static rungs have no mouse or keyboard to simulate, so the only believable signal we
can emit is *timing*: an interval distribution instead of a constant, short bursts
followed by a pause, hour-of-day speed changes, and a session rotation every N requests
so cookies stop accumulating on one identity.

Everything here is pure logic — no sockets, no clock sleeps. The engine asks for a number
and sleeps it.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from math import exp, log
from random import Random
from threading import RLock
from typing import Callable, Dict, Optional, Tuple

__all__ = ["PacingConfig", "PacingController"]

_MIN_DELAY = 0.1


@dataclass(frozen=True)
class PacingConfig:
    # 基础间隔分布
    base_delay: float = 1.0  # 平均间隔秒
    jitter_ratio: float = 0.5  # 抖动比例 0-1
    distribution: str = "lognormal"  # lognormal / uniform / pareto / fixed
    distribution_params: dict = field(default_factory=lambda: {"sigma": 0.5})

    # 突发模式：连续 N 次请求后停顿
    burst_size: int = 0  # 0 = 禁用
    burst_pause_min: float = 2.0
    burst_pause_max: float = 8.0

    # 时段模式：某些时段降速或暂停
    hour_multipliers: Tuple[float, ...] = (1.0,) * 24  # 每小时的倍率
    minute_jitter: float = 0.3  # 分钟内额外抖动

    # 会话轮换：每 N 次请求后重置 session（不是 reset_domain 的语义升级，只是刷新状态）
    rotate_session_every: int = 0  # 0 = 禁用
    rotate_pause: float = 3.0

    # per-domain 覆盖
    domain_overrides: dict = field(default_factory=dict)  # domain -> PacingConfig

    def for_domain(self, domain: str) -> "PacingConfig":
        """该域名生效的配置（覆盖优先）。"""
        if not self.domain_overrides:
            return self
        return self.domain_overrides.get(domain) or self


@dataclass
class _DomainState:
    count: int = 0
    burst_progress: int = 0  # 距离上次停顿已经发出的间隔数
    rotate_count: int = 0
    in_burst_pause: bool = False
    last_delay: float = 0.0


class PacingController:
    """Per-domain pacing state. Pure: `next_delay` returns seconds, the caller sleeps."""

    def __init__(
        self,
        config: PacingConfig,
        *,
        rng: Optional[Random] = None,
        clock: Optional[Callable[[], datetime]] = None,
    ) -> None:
        self._config = config
        self._rng = rng or Random()
        self._clock = clock or datetime.now
        self._states: Dict[str, _DomainState] = {}
        self._lock = RLock()

    @property
    def config(self) -> PacingConfig:
        return self._config

    def _state(self, domain: str) -> _DomainState:
        state = self._states.get(domain)
        if state is None:
            state = self._states[domain] = _DomainState()
        return state

    # ── 分布 ────────────────────────────────────────────────────────────────
    def _sample(self, cfg: PacingConfig) -> float:
        base, jitter = cfg.base_delay, cfg.jitter_ratio
        kind = cfg.distribution

        if kind == "fixed":
            # Constant when jitter is 0; a symmetric wobble otherwise (never negative).
            return base * (1.0 + self._rng.uniform(-jitter, jitter))
        if kind == "uniform":
            return self._rng.uniform(base * (1.0 - jitter), base * (1.0 + jitter))
        if kind == "lognormal":
            sigma = float(cfg.distribution_params.get("sigma", 0.5))
            value = exp(self._rng.gauss(log(base), sigma))
            return min(max(value, _MIN_DELAY), base * 10.0)
        if kind == "pareto":
            alpha = float(cfg.distribution_params.get("alpha", 1.5))
            # Long tail on purpose: most hits near the base, a few multi-second gaps.
            value = base * max(1.0, self._rng.paretovariate(alpha))
            return min(value, base * 20.0)
        raise ValueError(f"unknown pacing distribution: {kind}")

    def _hour_multiplier(self, cfg: PacingConfig) -> float:
        multipliers = cfg.hour_multipliers or (1.0,) * 24
        hour = self._clock().hour
        if not 0 <= hour < len(multipliers):
            return 1.0
        return multipliers[hour]

    # ── 对外 ────────────────────────────────────────────────────────────────
    def next_delay(self, domain: str = "") -> float:
        """返回下一次请求前应等待的秒数。纯函数，无 IO。

        Counts towards the burst budget: the N+1st call after a burst returns the pause.
        """
        cfg = self._config.for_domain(domain)
        with self._lock:
            state = self._state(domain)

            if cfg.burst_size > 0 and state.burst_progress >= cfg.burst_size:
                span = max(0.0, cfg.burst_pause_max - cfg.burst_pause_min)
                delay = cfg.burst_pause_min + self._rng.uniform(0.0, span)
                state.burst_progress = 0
                state.in_burst_pause = True
                state.last_delay = delay
                return delay

            state.burst_progress += 1
            state.in_burst_pause = False
            delay = self._sample(cfg) * self._hour_multiplier(cfg)
            if cfg.minute_jitter:
                delay *= 1.0 + self._rng.uniform(-cfg.minute_jitter, cfg.minute_jitter)
            delay = max(0.0, delay)
            state.last_delay = delay
            return delay

    def record_request(self, domain: str) -> None:
        """每次请求完成后调用，更新计数（轮换判定与 stats 用）。"""
        with self._lock:
            state = self._state(domain)
            state.count += 1
            state.rotate_count += 1

    def should_rotate(self, domain: str) -> bool:
        """是否应该轮换该域名的 session。"""
        cfg = self._config.for_domain(domain)
        if cfg.rotate_session_every <= 0:
            return False
        with self._lock:
            return self._state(domain).rotate_count >= cfg.rotate_session_every

    def mark_rotated(self, domain: str) -> None:
        """session 轮换后调用，重置计数。"""
        with self._lock:
            state = self._state(domain)
            state.rotate_count = 0
            state.burst_progress = 0
            state.in_burst_pause = False

    def stats(self) -> dict:
        """返回当前节奏状态：每域名的请求计数、下次延迟估计、是否在突发停顿中。"""
        with self._lock:
            return {
                domain: {
                    "count": state.count,
                    "next_delay_estimate": state.last_delay or self._config.for_domain(domain).base_delay,
                    "in_burst_pause": state.in_burst_pause,
                }
                for domain, state in self._states.items()
            }
