"""Two ready-made `AdaptiveConfig` profiles.

`ENGINEERING_PRESET` assumes you would rather miss pages than get flagged: it needs two
signals before escalating, waits two minutes after each upgrade, and crawls slowly.

`APT_PRESET` assumes the opposite trade — escalate on the first signal, cool for five
minutes, and spend concurrency freely once past headers. Slow to come back down (30 clean
pages) so a single good response cannot drag you back into a wall you already hit.
"""

from __future__ import annotations

from scrapling.spiders.adaptive import AdaptiveConfig
from scrapling.spiders.pacing import PacingConfig

__all__ = [
    "APT_PACING",
    "APT_PRESET",
    "ENGINEERING_PACING",
    "ENGINEERING_PRESET",
    "make_apt_spider",
]

# 工程保守档
ENGINEERING_PRESET = AdaptiveConfig(
    upgrade_at_risk=80.0,
    downgrade_after_success=15,
    cooldown_seconds=120.0,
    level_delays=(0.0, 0.2, 0.5, 1.5, 3.0),
    level_concurrency=(8, 6, 4, 2, 1),
)

# 工程档节奏：白天快一点、后半夜降速，不设突发停顿，间隔是"匀速但带抖动"的量级
ENGINEERING_PACING = PacingConfig(
    base_delay=2.0,
    jitter_ratio=0.4,
    distribution="uniform",
    burst_size=0,
    hour_multipliers=(
        0.5, 0.5, 0.5, 0.5, 0.5, 0.8,
        1.2, 1.5, 1.5, 1.5, 1.5, 1.2,
        1.2, 1.0, 1.2, 1.5, 1.5, 1.5,
        1.2, 1.0, 0.8, 0.6, 0.5, 0.5,
    ),
)

# 攻击档（APT 级）
# 攻击档节奏：间隔短、长尾抖动、20 次一顿、24 小时不降速、每 500 次换身份
APT_PACING = PacingConfig(
    base_delay=0.3,
    jitter_ratio=0.6,
    distribution="lognormal",
    distribution_params={"sigma": 0.7},
    burst_size=20,
    burst_pause_min=2.0,
    burst_pause_max=5.0,
    hour_multipliers=(1.0,) * 24,
    rotate_session_every=500,
    rotate_pause=2.0,
)

APT_PRESET = AdaptiveConfig(
    upgrade_at_risk=20.0,  # 一次信号就升级
    downgrade_after_success=30,  # 极慢降级
    cooldown_seconds=300.0,  # 5 分钟冷却
    level_delays=(0.0, 0.05, 0.2, 0.5, 1.0),
    level_concurrency=(50, 30, 20, 10, 5),
)


def make_apt_spider(base_class):
    """生成一个 APT 档的 Spider 子类。

    用法：
        from scrapling.spiders.spider import Spider
        from scrapling.spiders.presets import make_apt_spider
        APTSpider = make_apt_spider(Spider)

        class MySpider(APTSpider):
            name = "my_apt_crawl"
            start_urls = ["https://target.example"]
    """
    from scrapling.spiders.adaptive import AdaptiveConfig
    from scrapling.spiders.fingerprint import APT_FINGERPRINTS, FingerprintPool
    from scrapling.spiders.proxy_pool import ProxyPoolConfig
    from scrapling.spiders.retry import RetryPolicy

    class _APTSpider(base_class):
        # 并发拉满
        concurrent_requests = 200
        concurrent_requests_per_domain = 20
        download_delay = 0.05
        max_blocked_retries = 5

        # 自适应激进档
        adaptive_config = APT_PRESET

        # 指纹池：5 个浏览器 + 3 个 macOS 变体，同一域名粘一个，reset 后才换
        fingerprint_pool = FingerprintPool(APT_FINGERPRINTS)

        # 节奏：0.3s 基准 + 20 次一顿 + 500 次换 session
        pacing_config = APT_PACING

        # BROWSER 档渲染池：16 个并发 tab
        browser_max_pages = 16
        browser_disable_resources = True  # 速度优先，去图/字体/媒体（P7 第二批会分档）
        browser_block_ads = True  # 3526 个广告/追踪域名
        browser_retries = 2  # 比默认 3 少，因为失败时 P7 第三批会降级
        browser_retry_delay = 0.5
        browser_resource_profile = "speed"  # 速度档，不再一刀切
        browser_block_analytics = True

        # 代理池按需注入（proxy_pool 仍为 None），但池策略更激进
        proxy_pool_config = ProxyPoolConfig(
            health_check_interval=30.0,  # 比默认 60s 短一半
            cooling_seconds=180.0,  # 比默认 300s 短
            max_consecutive_fail=2,  # 比默认 3 更严格
            dead_after_fails=5,
            min_score=15.0,  # 比默认 20 更宽松
            prefer_domain_history=True,
        )

        # 重试激进档
        retry_policy = RetryPolicy(
            max_attempts=8,
            base_delay=0.3,
            max_delay=300.0,
            jitter=0.6,
            retry_on_status=frozenset({408, 425, 429, 500, 502, 503, 504, 520, 521, 522, 523, 524, 525, 526, 527, 530}),
            no_retry_on_status=frozenset({404, 410}),
            respect_retry_after=True,
            max_retry_after=600.0,
        )

    return _APTSpider
