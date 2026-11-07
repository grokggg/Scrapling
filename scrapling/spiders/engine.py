import json
import logging
import pprint
from pathlib import Path
from urllib.parse import urlparse

import anyio
from anyio import Path as AsyncPath
from anyio import create_task_group, CapacityLimiter, create_memory_object_stream, EndOfStream

from scrapling.core.utils import log
from scrapling.spiders.scheduler import Scheduler
from scrapling.spiders.session import SessionManager
from scrapling.spiders.request import Request, Response
from scrapling.spiders.robotstxt import RobotsTxtManager
from scrapling.spiders.result import CrawlStats, ItemList
from scrapling.spiders.throttle import AutoThrottle, parse_retry_after
from scrapling.spiders.retry import RetryDecision, RetryEngine, RetryPolicy, attempt_of, make_retry
from scrapling.spiders.adaptive import AdaptiveEngine, FetchLevel
from scrapling.spiders.presets import ENGINEERING_PRESET
from scrapling.spiders.signals import BlockSignal, detect_signal
from scrapling.spiders.session import STATIC_DEFAULT_SID, FINGERPRINT_POOL
from scrapling.spiders.pacing import PacingController
from scrapling.spiders.proxy_pool import ProxyPool, ProxyPoolConfig
from scrapling.spiders.cache import ResponseCacheManager
from scrapling.spiders.checkpoint import CheckpointManager, CheckpointData
from scrapling.spiders.dedup import ContentDeduplicator
from scrapling.spiders.store import StoredItem
from scrapling.spiders.telemetry import MetricsRegistry, StructuredLogFormatter, new_trace_id, register_defaults, trace_context
from scrapling.core._types import Dict, Union, Optional, TYPE_CHECKING, Any, AsyncGenerator

if TYPE_CHECKING:
    from scrapling.spiders.spider import Spider


def _dump(obj: Dict) -> str:
    return json.dumps(obj, indent=4)


class CrawlerEngine:
    """Orchestrates the crawling process."""

    def __init__(
        self,
        spider: "Spider",
        session_manager: SessionManager,
        crawldir: Optional[Union[str, Path, AsyncPath]] = None,
        interval: float = 300.0,
    ):
        self.spider = spider
        self.session_manager = session_manager
        self.scheduler = Scheduler(
            include_kwargs=spider.fp_include_kwargs,
            include_headers=spider.fp_include_headers,
            keep_fragments=spider.fp_keep_fragments,
            crawl_strategy=getattr(spider, "crawl_strategy", "bfs"),
        )
        self.stats = CrawlStats()

        # P10: telemetry is a switch, not a tax. Off means no registry, no exporter, and
        # _process_request skips the trace context wrapper entirely.
        self._telemetry = bool(getattr(self.spider, "telemetry_enabled", False))
        self._metrics: Optional[MetricsRegistry] = None
        self._exporter: Any = None
        if self._telemetry:
            self._metrics = MetricsRegistry()
            register_defaults(self._metrics)
            self._install_json_logs()
            port = int(getattr(self.spider, "telemetry_export_port", 0) or 0)
            if port:
                from scrapling.spiders.telemetry_export import TelemetryExporter

                self._exporter = TelemetryExporter(
                    self._metrics,
                    host=getattr(self.spider, "telemetry_host", "127.0.0.1"),
                    port=port,
                )

        # One adaptive ladder per crawl: level per domain decides which session a request uses.
        self._adaptive_engine = AdaptiveEngine(getattr(self.spider, "adaptive_config", None) or ENGINEERING_PRESET)
        # Per-domain isolation only makes sense while the framework's own static default is
        # in charge. A spider that registers its own sessions owns sid dispatch, and its
        # "first session added is the default" contract stays untouched.
        self._per_domain_sessions = self.session_manager.default_session_id == STATIC_DEFAULT_SID
        # The ladder may escalate the fingerprint (TLS/PROXY rungs); the pool is per-spider.
        self.session_manager.fingerprint_pool = getattr(self.spider, "fingerprint_pool", FINGERPRINT_POOL)
        # Proxy pool is opt-in: without it the PROXY rung falls back to TLS.
        self._proxy_pool: Optional[ProxyPool] = getattr(self.spider, "proxy_pool", None)
        # P9: persistence is opt-in, and so is dedup (dedup without a store has nothing to
        # consult after its window rolls over).
        store = getattr(self.spider, "store", None)
        self._dedup: Optional[ContentDeduplicator] = (
            ContentDeduplicator(store, window=getattr(self.spider, "dedup_window", 10000))
            if store is not None and getattr(self.spider, "dedup_enabled", False)
            else None
        )

        # Pacing is opt-in: a spider that never asked for it pays nothing, not even a draw.
        pacing_config = getattr(self.spider, "pacing_config", None)
        self._pacing: Optional[PacingController] = (
            PacingController(pacing_config) if pacing_config is not None else None
        )
        # A manager built by hand (`SessionManager()`) has no spider; adopt ours so the
        # BROWSER rung still reads this spider's tab count instead of the defaults.
        if getattr(self.session_manager, "_spider", None) is None:
            self.session_manager._spider = self.spider

        if self.spider.robots_txt_obey:

            async def _fetch_robots(url: str, sid: str) -> Response:
                return await self.session_manager.fetch(Request(url, sid=sid))

            self._robots_manager: Optional[RobotsTxtManager] = RobotsTxtManager(_fetch_robots)
        else:
            self._robots_manager = None

        if self.spider.development_mode:
            cache_dir = self.spider.development_cache_dir or f".scrapling_cache/{self.spider.name}"
            self._cache_manager: Optional[ResponseCacheManager] = ResponseCacheManager(cache_dir)
            log.warning("Development mode enabled -- responses will be cached to disk and replayed on subsequent runs")
        else:
            self._cache_manager = None

        if self.spider.autothrottle_enabled:
            self._autothrottle: Optional[AutoThrottle] = AutoThrottle(
                start_delay=self.spider.autothrottle_start_delay,
                max_delay=self.spider.autothrottle_max_delay,
                target_concurrency=(
                    self.spider.autothrottle_target_concurrency or self.spider.concurrent_requests_per_domain or 1.0
                ),
                block_backoff=self.spider.autothrottle_block_backoff,
            )
        else:
            self._autothrottle = None

        self._global_limiter = CapacityLimiter(spider.concurrent_requests)
        self._domain_limiters: dict[str, CapacityLimiter] = {}
        self._allowed_domains: set[str] = spider.allowed_domains or set()

        if self.spider.robots_txt_obey:
            self._domain_delays: dict[str, float] = {}

        self._active_tasks: int = 0
        self._running: bool = False
        self._items: ItemList = ItemList()
        self._item_stream: Any = None

        self._checkpoint_system_enabled = bool(crawldir)
        self._checkpoint_manager = CheckpointManager(crawldir or "", interval)
        self._last_checkpoint_time: float = 0.0
        self._pause_requested: bool = False
        self._force_stop: bool = False
        self.paused: bool = False

    def _is_domain_allowed(self, request: Request) -> bool:
        """Check if the request's domain is in allowed_domains."""
        if not self._allowed_domains:
            return True

        domain = request.domain
        for allowed in self._allowed_domains:
            if domain == allowed or domain.endswith("." + allowed):
                return True
        return False

    async def _get_domain_delay(self, request: Request) -> float:
        """Resolve the effective download delay for a domain.

        Takes the max of the spider's configured delay and any robots.txt
        directives (Crawl-delay / Request-rate). Result is cached per domain.
        """
        robots_manager = self._robots_manager
        if robots_manager is None:
            return self.spider.download_delay

        domain = request.domain

        if domain in self._domain_delays:
            return self._domain_delays[domain]

        # For domains covered by _prefetch_robots_txt this is a local parser read.
        # Domains discovered mid-crawl (not in start_urls) will fetch here.
        c_delay, r_rate = await robots_manager.get_delay_directives(request.url, request.sid)

        delay = self.spider.download_delay

        if r_rate:
            req_count, period = r_rate
            if req_count > 0:
                delay = max(delay, period / req_count)

        if c_delay is not None:
            delay = max(delay, c_delay)

        self._domain_delays[domain] = delay
        return delay

    def _rate_limiter(self, domain: str) -> CapacityLimiter:
        """Get or create a per-domain concurrency limiter if enabled, otherwise use the global limiter."""
        if self.spider.concurrent_requests_per_domain:
            self._domain_limiters.setdefault(domain, CapacityLimiter(self.spider.concurrent_requests_per_domain))
            return self._domain_limiters[domain]
        return self._global_limiter

    def _normalize_request(self, request: Request) -> None:
        """Normalize request fields before enqueueing.

        A request without a sid is pinned to its own domain's static session, so two
        domains never share a cookie jar. Hostless requests fall back to the registered
        default session. `_process_request` re-resolves the sid from the adaptive ladder
        right before fetching; this is the enqueue-time floor.
        """
        if not request.sid:
            domain = request.domain or ""
            # Initial rung is DIRECT (int=0); `_process_request` overrides it with the
            # domain's current level right before fetching.
            if domain and self._per_domain_sessions:
                request.sid = f"static:0:{domain}"
            else:
                request.sid = self.session_manager.default_session_id

    @staticmethod
    def _is_auto_sid(sid: str, domain: str) -> bool:
        """True for the sids this module derives (`{static|browser}:{level}:{domain}`)."""
        prefix, _, rest = sid.partition(":")
        if prefix not in ("static", "browser") or not domain:
            return False
        level, _, host = rest.partition(":")
        return host == domain and level.isdigit()

    async def _run_callbacks(self, request: Request, response: Response) -> None:
        """Dispatch response to the request's callback and process yielded items/requests."""
        callback = request.callback if request.callback else self.spider.parse
        try:
            async for result in callback(response):
                if isinstance(result, Request):
                    if self._is_domain_allowed(result):
                        self._normalize_request(result)
                        await self.scheduler.enqueue(result)
                    else:
                        self.stats.offsite_requests_count += 1
                        log.debug(f"Filtered offsite request to: {result.url}")
                elif isinstance(result, dict):
                    processed_result = await self.spider.on_scraped_item(result)
                    if processed_result:
                        self.stats.items_scraped += 1
                        log.debug(f"Scraped from {str(response)}\n{pprint.pformat(processed_result)}")
                        if self._item_stream:
                            await self._item_stream.send(processed_result)
                        else:
                            self._items.append(processed_result)
                    else:
                        self.stats.items_dropped += 1
                        log.warning(f"Dropped from {str(response)}\n{processed_result}")
                elif result is not None:
                    log.error(f"Spider must return Request, dict or None, got '{type(result)}' in {request}")
        except Exception as e:
            msg = f"Spider error processing {request}:\n {e}"
            log.error(msg, exc_info=e)
            await self.spider.on_error(request, e)

    def _get_policy(self, request: Request) -> RetryPolicy:
        """Retry policy that governs `request`.

        `getattr` on purpose: third-party spiders and test doubles predate this layer,
        so a spider without `get_retry_policy` simply gets the default policy instead
        of an AttributeError in the middle of a crawl.
        """
        getter = getattr(self.spider, "get_retry_policy", None)
        if getter is None:
            return RetryPolicy()
        try:
            return getter(request)
        except Exception:
            return RetryPolicy()

    async def _schedule_retry(self, request: Request, decision: RetryDecision, delay: float) -> None:
        """Wait out `delay`, then re-enqueue a retry copy of `request`."""
        if delay > 0:
            await anyio.sleep(delay)

        retry = make_retry(request, decision)

        bumper = getattr(self.spider, "_bump_retry", None)
        if bumper is not None:
            bumper(retry)
        else:
            retry._retry_count = getattr(retry, "_retry_count", 0) + 1

        self._normalize_request(retry)
        await self.scheduler.enqueue(retry)
        log.info("Scheduled retry (%s, attempt %d): %s", decision.name, attempt_of(retry), request.url)

    async def _process_request(self, request: Request) -> None:
        """Download and process a single request, under its own trace id."""
        if self._metrics is None:
            return await self._process_request_inner(request)
        with trace_context(new_trace_id()):
            return await self._process_request_inner(request)

    async def _process_request_inner(self, request: Request) -> None:
        if self._robots_manager:
            can_fetch = await self._robots_manager.can_fetch(request.url, request.sid)
            if not can_fetch:
                self.stats.robots_disallowed_count += 1
                log.info(f"Request disallowed by robots.txt: {request.url}")
                return
            floor = await self._get_domain_delay(request)
        else:
            floor = self.spider.download_delay

        delay = floor

        if self._cache_manager and request._fp is not None:
            cached = await self._cache_manager.get(request._fp)
            if cached is not None:
                cached.request = request
                # Cached responses are rebuilt without meta, so merge the request's in as the live path does
                cached.meta = {**request.meta, **cached.meta}
                self.stats.cache_hits += 1
                self.stats.increment_requests_count(request.sid or self.session_manager.default_session_id)
                self.stats.increment_response_bytes(request.domain, len(cached.body))
                self.stats.increment_status(cached.status)
                log.debug(f"Cache hit: {request.url}")
                await self._run_callbacks(request, cached)
                return

        retry_plan = None

        # Resolve the session for this domain at its current ladder level. Done before the
        # rate limiter so the session exists by the time the worker gets a slot.
        # An explicit `sid` is the caller's choice and is never overridden; only auto sids
        # (the ones `_normalize_request` derives from the domain) follow the ladder.
        domain = request.domain or ""
        proxy_url = None
        level = FetchLevel.DIRECT
        if self._per_domain_sessions and (not request.sid or self._is_auto_sid(request.sid, domain)):
            # A dead ladder (BROWSER at max risk) starts over: drop every session of this
            # host, forget the profile, and let this very request run on the fresh DIRECT
            # rung — the request that tripped the reset is not thrown away.
            if self._adaptive_engine is not None and self._adaptive_engine.should_reset(domain):
                await self.session_manager.reset_domain(domain)
                self._adaptive_engine.reset_session(domain)
                log.info("session reset: domain=%s", domain)

            level = self._adaptive_engine.plan(domain)[0] if self._adaptive_engine else FetchLevel.DIRECT
            request.sid = self.session_manager.ensure_domain_session(domain, level)

            if level is FetchLevel.PROXY:
                proxy_url = self._proxy_pool.acquire(domain) if self._proxy_pool is not None else None
                if proxy_url is None:
                    # No pool, or a dry one: the TLS rung is the strongest thing left.
                    level = FetchLevel.TLS
                    request.sid = self.session_manager.ensure_domain_session(domain, level)
                else:
                    request._session_kwargs["proxy"] = proxy_url
            elif level is FetchLevel.BROWSER and self._proxy_pool is not None:
                # A browser session works on its own; a proxy is a bonus, never a fallback trigger.
                proxy_url = self._proxy_pool.acquire(domain)
                if proxy_url is not None:
                    request._session_kwargs["proxy"] = proxy_url

        if self._metrics is not None:
            self._metrics.gauge("scrapling_ladder_level").set(float(getattr(level, "value", level)), domain=domain)

        # P8: ask the origin only for what changed. Conditional headers lose to any the
        # caller already set, and a request with a body is never a cache lookup.
        cache = getattr(self.spider, "incremental_cache", None)
        cache_key = request.url if cache is not None and request._session_kwargs.get("method", "GET") == "GET" else None
        if cache_key is not None:
            conditional = cache.conditional_headers(cache_key)
            if conditional:
                request._session_kwargs["headers"] = {
                    **conditional,
                    **request._session_kwargs.get("headers", {}),
                }

        async with self._rate_limiter(request.domain):
            if self._autothrottle:
                delay = self._autothrottle.delay_for(request.domain, floor)

            if self._pacing is not None:
                # Both numbers are "at least this long", so the more conservative one wins.
                delay = max(delay, self._pacing.next_delay(request.domain or ""))
                if self._metrics is not None:
                    self._metrics.histogram("scrapling_pacing_delay_seconds").observe(delay, domain=domain)

            if delay:
                await anyio.sleep(delay)

            if request._session_kwargs.get("proxy"):
                self.stats.proxies.append(request._session_kwargs["proxy"])
            if request._session_kwargs.get("proxies"):
                self.stats.proxies.append(dict(request._session_kwargs["proxies"]))
            try:
                started_at = anyio.current_time()
                response = await self.session_manager.fetch(request)
                latency = anyio.current_time() - started_at

                # Before signals, stats or callbacks: a 304 carries no body, so letting it
                # through would read as an EMPTY signal and escalate the ladder on every hit.
                if cache_key is not None:
                    response = self._apply_incremental(cache, cache_key, request, response)

                if self._metrics is not None:
                    self._metrics.counter("scrapling_requests_total").inc(status=str(response.status))
                    self._metrics.histogram("scrapling_request_duration_seconds").observe(
                        latency, level=level.name
                    )

                if proxy_url is not None:
                    proxy_ok = 200 <= response.status < 400
                    self._proxy_pool.release(
                        proxy_url,
                        success=proxy_ok,
                        latency_ms=latency * 1000,
                        domain=domain,
                    )
                    if self._metrics is not None:
                        self._metrics.counter("scrapling_proxy_success_total").inc(
                            result="success" if proxy_ok else "fail"
                        )

                # Every response is evidence: a signal feeds the ladder, a clean one buys
                # a step down. Done before stats so the ladder never depends on counters.
                if self._adaptive_engine is not None:
                    signal = detect_signal(response)
                    if signal.signal is not BlockSignal.NONE:
                        self._adaptive_engine.observe(domain, signal.signal, signal.confidence)
                        if self._metrics is not None:
                            self._metrics.counter("scrapling_signals_total").inc(signal=signal.signal.name)
                        log.debug(
                            "signal %s on %s (conf=%.2f): %s",
                            signal.signal.name,
                            domain,
                            signal.confidence,
                            signal.reason,
                        )
                    else:
                        self._adaptive_engine.observe_success(domain)

                self.stats.increment_requests_count(request.sid or self.session_manager.default_session_id)
                self.stats.increment_response_bytes(request.domain, len(response.body))
                self.stats.increment_status(response.status)

                # Rhythm bookkeeping: a completed request may be the one that retires the
                # session. The reset only drops sessions — the adaptive ladder keeps its
                # risk, so the next request resumes at the same rung on a fresh identity.
                if self._pacing is not None:
                    self._pacing.record_request(domain)
                    if self._pacing.should_rotate(domain):
                        await self.session_manager.reset_domain(domain)
                        self._pacing.mark_rotated(domain)
                        pause = self._pacing.config.for_domain(domain).rotate_pause
                        log.info("session rotated: domain=%s", domain)
                        # A new identity should not open with a burst: let the old one go quiet.
                        if pause > 0:
                            await anyio.sleep(pause)

            except Exception as e:
                if self._metrics is not None:
                    self._metrics.counter("scrapling_requests_total").inc(status="error")
                if proxy_url is not None:
                    self._proxy_pool.release(proxy_url, success=False, domain=domain)
                    if self._metrics is not None:
                        self._metrics.counter("scrapling_proxy_success_total").inc(result="fail")
                # An exception is not proof of anti-bot, but a run of them is a risk of its
                # own. Low weight on purpose: it accumulates without triggering on its own.
                if self._adaptive_engine is not None:
                    self._adaptive_engine.observe(domain, BlockSignal.EMPTY, 0.3)

                # The browser is the last rung, so its failure used to mean a lost URL. Retry
                # the same request once at TLS strength instead: same object, same queue slot,
                # no re-scheduling. One hop only — TLS failing goes back to the retry engine.
                fell_back = False
                if level is FetchLevel.BROWSER and getattr(self.spider, "browser_fallback_to_static", True):
                    if self._metrics is not None:
                        self._metrics.counter("scrapling_browser_failures_total").inc(exc=type(e).__name__)
                    # The browser's proxy was just released as a failure; the TLS session
                    # carries its own identity and must not inherit a dead lease.
                    request._session_kwargs.pop("proxy", None)
                    request._session_kwargs.pop("proxies", None)
                    proxy_url = None
                    request.sid = self.session_manager.ensure_domain_session(domain, FetchLevel.TLS)
                    try:
                        started_at = anyio.current_time()
                        response = await self.session_manager.fetch(request)
                        latency = anyio.current_time() - started_at
                        fell_back = True
                    except Exception as fallback_error:
                        e = fallback_error
                        log.warning(
                            "TLS fallback failed too: url=%s exc=%s", request.url, type(fallback_error).__name__
                        )

                if fell_back:
                    log.info("browser fell back to TLS: url=%s exc=%s", request.url, type(e).__name__)
                    self.stats.increment_requests_count(
                        request.sid or self.session_manager.default_session_id
                    )
                    self.stats.increment_response_bytes(request.domain, len(response.body))
                    self.stats.increment_status(response.status)
                    if self._metrics is not None:
                        self._metrics.counter("scrapling_requests_total").inc(status=str(response.status))
                        self._metrics.histogram("scrapling_request_duration_seconds").observe(
                            latency, level=FetchLevel.TLS.name
                        )
                    if self._adaptive_engine is not None:
                        signal = detect_signal(response)
                        if signal.signal is not BlockSignal.NONE:
                            self._adaptive_engine.observe(domain, signal.signal, signal.confidence)
                        else:
                            self._adaptive_engine.observe_success(domain)
                else:
                    decision, delay = RetryEngine(self._get_policy(request)).decide(request, exc=e)
                    if self._metrics is not None:
                        self._metrics.counter("scrapling_retries_total").inc(decision=decision.name)
                    if decision is RetryDecision.GIVE_UP:
                        log.warning(
                            "retry give up: url=%s exc=%s attempt=%d",
                            request.url,
                            type(e).__name__,
                            attempt_of(request),
                        )
                        self.stats.failed_requests_count += 1
                        await self.spider.on_error(request, e)
                        return
                    retry_plan = (decision, delay)

        # Sleep outside the rate limiter: a backing-off worker must not hold a slot.
        if retry_plan is not None:
            await self._schedule_retry(request, *retry_plan)
            return

        if self._cache_manager and request._fp is not None:
            self.stats.cache_misses += 1

        blocked = await self.spider.is_blocked(response)
        if self._autothrottle:
            ok = 200 <= response.status < 300 and not blocked
            retry_after = None if ok else parse_retry_after(response.headers)
            self._autothrottle.record(request.domain, latency, ok, floor, retry_after)

        if blocked:
            self.stats.blocked_requests_count += 1
            if request._retry_count < self.spider.max_blocked_retries:
                retry_request = request.copy()
                retry_request._retry_count += 1
                retry_request.priority -= 1  # Don't retry immediately
                retry_request.dont_filter = True
                retry_request._session_kwargs.pop("proxy", None)
                retry_request._session_kwargs.pop("proxies", None)

                new_request = await self.spider.retry_blocked_request(retry_request, response)
                self._normalize_request(new_request)
                await self.scheduler.enqueue(new_request)
                log.info(
                    f"Scheduled blocked request for retry ({retry_request._retry_count}/{self.spider.max_blocked_retries}): {request.url}"
                )
            else:
                log.warning(f"Max retries exceeded for blocked request: {request.url}")
            return

        # Transient failures (429 / 5xx): the one policy table decides what happens.
        # Blocked responses returned above, so the two retry paths never stack.
        policy = self._get_policy(request)
        decision, delay = RetryEngine(policy).decide(request, response=response)
        if decision is not RetryDecision.GIVE_UP:
            await self._schedule_retry(request, decision, delay)
            return
        if response.status in policy.retry_on_status:
            log.warning(
                "retry exhausted: url=%s status=%d attempt=%d",
                request.url,
                response.status,
                attempt_of(request),
            )

        if self._cache_manager and request._fp is not None:
            await self._cache_manager.put(request._fp, response, request._session_kwargs.get("method", "GET"))

        await self._store_response(request, response)

        await self._run_callbacks(request, response)

    async def _store_response(self, request: Request, response: Response) -> None:
        """Hand the row to the spider's store. A store failure never kills the crawl."""
        store = getattr(self.spider, "store", None)
        if store is None or not 200 <= response.status < 300:
            return
        try:
            item = self._extract_item(request, response)
            if item is None:
                return
            # Hashes are left blank on purpose: StoredItem derives them the same way the
            # store keys rows, so get(url) and the upsert agree.
            stored = StoredItem(
                url=request.url,
                item=item,
                source_sid=request.sid or "",
                meta=dict(getattr(request, "meta", None) or {}),
            )
            if self._dedup is not None and self._dedup.is_duplicate(stored):
                self.stats.dedup_skipped += 1
                log.debug("dedup skip: %s", request.url)
                return
            is_new = store.put(stored)
            if self._dedup is not None:
                self._dedup.mark_seen(stored)
            if is_new:
                self.stats.stored_new += 1
            else:
                self.stats.stored_updated += 1
            if self._metrics is not None:
                self._metrics.counter("scrapling_store_ops_total").inc(op="new" if is_new else "update")
        except Exception as exc:
            log.warning("store failed for %s: %s: %s", request.url, type(exc).__name__, exc)

    def _extract_item(self, request: Request, response: Response) -> Optional[dict]:
        """The spider's ``extract_item`` hook if it has one, else a conservative preview.

        The default is deliberately dumb — url, status, headers, first 4KB — because the
        real schema belongs to the spider, which already owns ``parse``.
        """
        hook = getattr(self.spider, "extract_item", None)
        if callable(hook):
            return hook(request, response)
        body = getattr(response, "body", None)
        if not body:
            return None
        return {
            "url": request.url,
            "status": response.status,
            "headers": dict(response.headers or {}),
            "body_preview": body[:4096].decode("utf-8", errors="replace"),
        }

    def _apply_incremental(self, cache: Any, url: str, request: Request, response: Response) -> Response:
        """Serve a 304 / byte-identical 200 from the cache; refresh the entry on a real 200.

        Returns the response the rest of the pipeline should see. Whatever happens here, the
        caller keeps its normal flow — a cache hit is a success, not a shortcut.
        """
        status = getattr(response, "status", None)
        if status == 304 or (status == 200 and cache.is_not_modified(response)):
            cached = cache.build_response_from_cache(url)
            if cached is None:
                # Expired between the conditional request and the answer: fall back to the
                # server's response rather than inventing a body.
                return response
            cached.request = request
            self.stats.incremental_hits += 1
            if self._metrics is not None:
                self._metrics.counter("scrapling_incremental_hits_total").inc()
            log.debug("incremental: %s served from cache (status %s)", url, status)
            return cached

        if status == 200:
            cache.put(url, response)
            self.stats.incremental_misses += 1
        return response

    def _install_json_logs(self) -> None:
        """Swap this spider's logger onto the JSON formatter, leaving everyone else alone."""
        if getattr(self.spider, "telemetry_log_format", "text") != "json":
            return
        logger = logging.getLogger(f"scrapling.spiders.{self.spider.name}")
        handler = logging.StreamHandler()
        handler.setFormatter(
            StructuredLogFormatter(
                extra_fields=tuple(getattr(self.spider, "telemetry_extra_fields", ("domain", "url", "sid")))
            )
        )
        handler.set_name("scrapling-telemetry")
        # Idempotent: a second call replaces our handler instead of stacking a duplicate.
        logger.handlers = [h for h in logger.handlers if h.get_name() != "scrapling-telemetry"] + [handler]
        logger.propagate = False

    async def _task_wrapper(self, request: Request) -> None:
        """Wrapper to track active task count."""
        try:
            await self._process_request(request)
        finally:
            self.scheduler.complete(request)
            self._active_tasks -= 1

    def request_pause(self) -> None:
        """Request a graceful pause of the crawl.

        First call: requests graceful pause (waits for active tasks).
        Second call: forces immediate stop.
        """
        if self._force_stop:
            return  # Already forcing stop

        if self._pause_requested:
            # Second Ctrl+C - force stop
            self._force_stop = True
            log.warning("Force stop requested, cancelling immediately...")
        else:
            self._pause_requested = True
            log.info(
                "Pause requested, waiting for in-flight requests to complete (press Ctrl+C again to force stop)..."
            )

    async def _save_checkpoint(self) -> None:
        """Save current state to checkpoint files."""
        requests, seen = self.scheduler.snapshot()
        data = CheckpointData(requests=requests, seen=seen)
        await self._checkpoint_manager.save(data)
        self._last_checkpoint_time = anyio.current_time()

    def _is_checkpoint_time(self) -> bool:
        """Check if it's time for the periodic checkpoint."""
        if not self._checkpoint_system_enabled:
            return False

        if self._checkpoint_manager.interval == 0:
            return False

        current_time = anyio.current_time()
        return (current_time - self._last_checkpoint_time) >= self._checkpoint_manager.interval

    async def _restore_from_checkpoint(self) -> bool:
        """Attempt to restore state from checkpoint.

        Returns True if successfully restored, False otherwise.
        """
        if not self._checkpoint_system_enabled:
            return False

        data = await self._checkpoint_manager.load()
        if data is None:
            return False

        self.scheduler.restore(data)

        # Restore callbacks from spider after scheduler restore
        for request in data.requests:
            request._restore_callback(self.spider)

        return True

    async def _prefetch_robots_txt(self) -> None:
        """Pre-warm the robots.txt cache before the crawl loop starts.

        Extracts unique domains from start_urls, preserving the original scheme.
        """
        if not self._robots_manager or not self.spider.start_urls:
            return

        # Deduplicate by netloc, preserving the scheme from the first URL per domain
        seen: set[str] = set()
        seed_urls: list[str] = []
        for url in self.spider.start_urls:
            parsed = urlparse(url)
            if parsed.netloc not in seen:
                seen.add(parsed.netloc)
                seed_urls.append(f"{parsed.scheme}://{parsed.netloc}/")

        await self._robots_manager.prefetch(seed_urls, self.session_manager.default_session_id)

    async def crawl(self) -> CrawlStats:
        """Run the spider with the metrics endpoint up for exactly as long as the crawl."""
        if self._exporter is None:
            return await self._crawl()
        self._exporter.start()
        log.info("metrics endpoint: %s", self._exporter.url())
        try:
            return await self._crawl()
        finally:
            self._exporter.stop()

    async def _crawl(self) -> CrawlStats:
        self._running = True
        self._items.clear()
        self.paused = False
        self._pause_requested = False
        self._force_stop = False
        self.stats = CrawlStats(start_time=anyio.current_time())
        self._domain_limiters.clear()
        if self._robots_manager:
            self._domain_delays.clear()
        if self._autothrottle:
            self._autothrottle.reset()

        # Check for existing checkpoint
        resuming = (await self._restore_from_checkpoint()) if self._checkpoint_system_enabled else False
        self._last_checkpoint_time = anyio.current_time()

        async with self.session_manager:
            self.stats.concurrent_requests = self.spider.concurrent_requests
            self.stats.concurrent_requests_per_domain = self.spider.concurrent_requests_per_domain
            self.stats.download_delay = self.spider.download_delay
            self.stats.autothrottle_enabled = self.spider.autothrottle_enabled
            await self.spider.on_start(resuming=resuming)

            await self._prefetch_robots_txt()

            finished = False
            try:
                if not resuming:
                    async for request in self.spider.start_requests():
                        self._normalize_request(request)
                        await self.scheduler.enqueue(request)
                else:
                    log.info("Resuming from checkpoint, skipping start_requests()")

                # Process queue
                async with create_task_group() as tg:
                    while self._running:
                        if self._pause_requested:
                            if self._active_tasks == 0 or self._force_stop:
                                # Save checkpoint before canceling to avoid data loss
                                if self._checkpoint_system_enabled:
                                    await self._save_checkpoint()
                                    self.paused = True
                                    log.info("Spider paused, checkpoint saved")
                                else:
                                    log.info("Spider stopped gracefully")

                                if self._force_stop:
                                    log.warning(f"Force stopping with {self._active_tasks} active tasks")
                                    tg.cancel_scope.cancel()

                                self._running = False
                                break

                            # Wait briefly and check again
                            await anyio.sleep(0.05)
                            continue

                        if self._checkpoint_system_enabled and self._is_checkpoint_time():
                            await self._save_checkpoint()

                        if self.scheduler.is_empty:
                            # Empty queue + no active tasks = done
                            if self._active_tasks == 0:
                                self._running = False
                                log.debug("Spider idle")
                                break

                            # Brief wait for callbacks to enqueue new requests
                            await anyio.sleep(0.05)
                            continue

                        # Only spawn tasks up to concurrent_requests limit
                        # This prevents spawning thousands of waiting tasks
                        if self._active_tasks >= self.spider.concurrent_requests:
                            await anyio.sleep(0.01)
                            continue

                        request = await self.scheduler.dequeue()
                        self._active_tasks += 1
                        tg.start_soon(self._task_wrapper, request)

                finished = True
            finally:
                await self.spider.on_close()
                # Clean up checkpoint files only on successful completion (not paused, not crashed)
                if finished and not self.paused and self._checkpoint_system_enabled:
                    await self._checkpoint_manager.cleanup()

        if self._autothrottle:
            self.stats.autothrottle_delays = dict(self._autothrottle.delays)

        self.stats.log_levels_counter = self.spider._log_counter.get_counts()
        self.stats.end_time = anyio.current_time()
        log.info(_dump(self.stats.to_dict()))
        return self.stats

    @property
    def items(self) -> ItemList:
        """Access scraped items."""
        return self._items

    def __aiter__(self) -> AsyncGenerator[dict, None]:
        return self._stream()

    async def _stream(self) -> AsyncGenerator[dict, None]:
        """Async generator that runs crawl and yields items."""
        send, recv = create_memory_object_stream[dict](100)
        self._item_stream = send

        async def run():
            try:
                await self.crawl()
            finally:
                await send.aclose()

        async with create_task_group() as tg:
            tg.start_soon(run)
            try:
                async for item in recv:
                    yield item
            except EndOfStream:
                pass
