"""Two ready-made `AdaptiveConfig` profiles.

`ENGINEERING_PRESET` assumes you would rather miss pages than get flagged: it needs two
signals before escalating, waits two minutes after each upgrade, and crawls slowly.

`APT_PRESET` assumes the opposite trade — escalate on the first signal, cool for five
minutes, and spend concurrency freely once past headers. Slow to come back down (30 clean
pages) so a single good response cannot drag you back into a wall you already hit.
"""

from __future__ import annotations

from scrapling.spiders.adaptive import AdaptiveConfig

__all__ = ["APT_PRESET", "ENGINEERING_PRESET"]

# 工程保守档
ENGINEERING_PRESET = AdaptiveConfig(
    upgrade_at_risk=80.0,
    downgrade_after_success=15,
    cooldown_seconds=120.0,
    level_delays=(0.0, 0.2, 0.5, 1.5, 3.0),
    level_concurrency=(8, 6, 4, 2, 1),
)

# 攻击档（APT 级）
APT_PRESET = AdaptiveConfig(
    upgrade_at_risk=20.0,  # 一次信号就升级
    downgrade_after_success=30,  # 极慢降级
    cooldown_seconds=300.0,  # 5 分钟冷却
    level_delays=(0.0, 0.05, 0.2, 0.5, 1.0),
    level_concurrency=(50, 30, 20, 10, 5),
)
