"""Two ready-made `AdaptiveConfig` profiles.

`ENGINEERING_PRESET` assumes you would rather miss pages than get flagged: it needs two
signals before escalating, waits two minutes after each upgrade, and crawls slowly.

`APT_PRESET` assumes the opposite trade — escalate on the first signal, cool for five
minutes, and spend concurrency freely once past headers. Slow to come back down (30 clean
pages) so a single good response cannot drag you back into a wall you already hit.
"""

from __future__ import annotations

from scrapling.spiders.adaptive import AdaptiveConfig

__all__ = ["APT_PRESET", "ENGINEERING_PRESET", "make_apt_spider"]

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
    from scrapling.spiders.adaptive import AdaptiveConfig  # noqa: F401  (documented import path)
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

        # 指纹池加大
        fingerprint_pool = [
            "chrome",
            "chrome110",
            "chrome116",
            "chrome120",
            "safari17_0",
            "safari17_2_ios",
            "edge101",
            "firefox133",
            "firefox130",
            "chrome124",
            "chrome126",
            "chrome128",
            "safari18_0",
            "safari18_1_ios",
        ]

    return _APTSpider
