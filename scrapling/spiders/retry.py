"""Status-code driven retry policy for the spider engine.

Pure by construction: no IO, no sleeping, no threads, no logging. `decide()` is a
function of (request, response|exception) and the engine owns waiting/enqueueing,
so every branch is unit-testable and the policy stays out of the crawl loop.

Facts about `Request` this module lives with (verified in `request.py`, unchanged):

* the attempt counter is the **private** `_retry_count` (0 = first attempt),
  preserved by `Request.copy()` and by pickle/checkpoint state;
* proxies are not an attribute — they travel inside `_session_kwargs` as
  `proxy=` / `proxies=` and are handed to the fetch layer verbatim;
* the fingerprint ignores `_retry_count` and `dont_filter`, so a retry keeps the
  same fingerprint and **must** be re-enqueued with `dont_filter=True` or the
  scheduler silently drops it.

Attempt budget is shared: the blocked-response path in `engine.py` already does
`retry_request._retry_count += 1`, so `decide()` sees the *total* attempt count
(blocked retries included) and `max_attempts` is a budget for the whole request,
not per failure kind. Incrementing is the spider's job (`Spider._bump_retry`);
this module never writes `_retry_count`.

Decision table (evaluation order):

    attempt >= max_attempts                     -> GIVE_UP
    exception not in retry_on_exception         -> GIVE_UP
    exception in retry_on_exception             -> RETRY_SAME_PRIORITY
    response is None                            -> GIVE_UP
    status in no_retry_on_status                -> GIVE_UP   (wins over retry_on_status)
    status not in retry_on_status               -> GIVE_UP
    5xx (500..599), attempt == 0                -> RETRY_HIGH_PRIORITY
    5xx (500..599), attempt > 0                 -> RETRY_SAME_PRIORITY
    429, no proxy                               -> RETRY_HIGH_PRIORITY
    429, with proxy                             -> RETRY_WITH_NEW_PROXY
    other retryable status (408, 425, ...)      -> RETRY_SAME_PRIORITY
"""

from __future__ import annotations

import random
import time
from dataclasses import dataclass
from email.utils import parsedate_to_datetime
from enum import Enum, auto
from typing import TYPE_CHECKING, Any, Optional

if TYPE_CHECKING:  # pragma: no cover
    from scrapling.spiders.request import Request

__all__ = [
    "RetryDecision",
    "RetryPolicy",
    "RetryEngine",
    "attempt_of",
    "has_proxy",
    "strip_proxy",
    "make_retry",
]


class RetryDecision(Enum):
    """What the engine should do with a failed attempt."""

    RETRY_SAME_PRIORITY = auto()  # re-enqueue at the same priority
    RETRY_HIGH_PRIORITY = auto()  # re-enqueue one priority step earlier (-1)
    RETRY_WITH_NEW_PROXY = auto()  # drop the proxy, then re-enqueue
    GIVE_UP = auto()  # dead letter: never re-enqueue


@dataclass(frozen=True)
class RetryPolicy:
    """Retry knobs. Immutable so one policy can be shared across requests."""

    max_attempts: int = 5
    base_delay: float = 0.5
    max_delay: float = 120.0
    jitter: float = 0.4  # ratio in [0, 1]
    retry_on_status: frozenset[int] = frozenset({408, 425, 429, 500, 502, 503, 504, 522, 524})
    no_retry_on_status: frozenset[int] = frozenset({400, 401, 403, 404, 405, 410, 422})
    retry_on_exception: tuple[type[BaseException], ...] = (TimeoutError, ConnectionError, OSError)
    respect_retry_after: bool = True
    max_retry_after: float = 300.0

    def __post_init__(self) -> None:
        if self.max_attempts < 0:
            raise ValueError("max_attempts must be >= 0")
        if self.base_delay <= 0:
            raise ValueError("base_delay must be > 0")
        if self.max_delay <= 0:
            raise ValueError("max_delay must be > 0")
        if not 0.0 <= self.jitter <= 1.0:
            raise ValueError("jitter must be within [0, 1]")
        if self.max_retry_after < 0:
            raise ValueError("max_retry_after must be >= 0")


# --------------------------------------------------------------------------- #
# Request field adapters (read-only with respect to the attempt counter)
# --------------------------------------------------------------------------- #
def attempt_of(request: Any) -> int:
    """Attempts already made for this request (0 = first try).

    `_retry_count` is the field `Request` actually defines; there is no public
    `retry_count` alias, and inventing one silently loses the value on `copy()`.
    """
    return int(getattr(request, "_retry_count", 0) or 0)


def _session_kwargs(request: Any) -> dict:
    kwargs = getattr(request, "_session_kwargs", None)
    return kwargs if isinstance(kwargs, dict) else {}


def has_proxy(request: Any) -> bool:
    """True when the request would reach the network through a proxy."""
    if getattr(request, "proxy", None):
        return True
    kwargs = _session_kwargs(request)
    return bool(kwargs.get("proxy") or kwargs.get("proxies"))


def strip_proxy(request: Any) -> int:
    """Remove every proxy carrier from the request. Returns how many were removed."""
    removed = 0
    kwargs = _session_kwargs(request)
    for key in ("proxy", "proxies"):
        if kwargs.pop(key, None) is not None:
            removed += 1
    if getattr(request, "proxy", None) is not None:
        request.proxy = None
        removed += 1
    return removed


def make_retry(request: Any, decision: RetryDecision) -> Any:
    """Return a copy of `request` shaped by `decision`, ready to re-enqueue.

    Deliberately does **not** touch `_retry_count`: the attempt budget is bumped
    by `Spider._bump_retry()` at the single place that owns it. `dont_filter=True`
    is mandatory because the scheduler dedupes by fingerprint, which a retry keeps.
    """
    if decision is RetryDecision.GIVE_UP:
        raise ValueError("GIVE_UP is not a retry decision")

    retry = request.copy()
    if decision is RetryDecision.RETRY_HIGH_PRIORITY:
        retry.priority -= 1
    elif decision is RetryDecision.RETRY_WITH_NEW_PROXY:
        strip_proxy(retry)
    retry.dont_filter = True
    return retry


# --------------------------------------------------------------------------- #
# Engine
# --------------------------------------------------------------------------- #
class RetryEngine:
    """Evaluates failures against a `RetryPolicy` and computes the next delay."""

    __slots__ = ("policy", "_rng")

    def __init__(self, policy: Optional[RetryPolicy] = None, *, rng: Optional[random.Random] = None):
        self.policy = policy if policy is not None else RetryPolicy()
        # Injectable RNG: production uses the module RNG, tests can seed it.
        self._rng = rng if rng is not None else random.Random()

    # ------------------------------------------------------------------ delay
    def next_delay(self, attempt: int, retry_after: Optional[float] = None) -> float:
        """Seconds to wait before the next attempt.

        A sane `Retry-After` (non-negative and <= `max_retry_after`) wins, capped at
        `max_delay`, plus a small spread so a hundred clients told "wait 5s" do not
        stampede at 5.000s.
        Otherwise exponential backoff: `base * 2**attempt`, scaled by
        `1 + U(-jitter, +jitter)`, then clamped to `[0.1, max_delay]` — the cap is
        applied **after** jitter so it is a real ceiling.
        """
        policy = self.policy

        if retry_after is not None and policy.respect_retry_after and 0 <= retry_after <= policy.max_retry_after:
            # Server hint is honoured, but never beyond max_delay: returning later than
            # the origin asked is safer than hammering it earlier. The spread keeps a
            # hundred clients told "wait 5s" from stampeding at 5.000s.
            hint = min(float(retry_after), policy.max_delay)
            return max(0.1, hint + self._rng.uniform(0.0, 0.25))

        step = max(0, int(attempt))
        raw = policy.base_delay * (2**step)
        jittered = raw * (1.0 + self._rng.uniform(-policy.jitter, policy.jitter))
        return min(max(jittered, 0.1), policy.max_delay)

    # ----------------------------------------------------------------- decide
    def decide(
        self,
        request: "Request",
        response: Any = None,
        exc: Optional[BaseException] = None,
    ) -> tuple[RetryDecision, float]:
        """Return `(decision, delay_seconds)`. `delay` is 0.0 when giving up."""
        policy = self.policy
        attempt = attempt_of(request)

        # The budget covers every kind of attempt, blocked retries included.
        if attempt >= policy.max_attempts:
            return RetryDecision.GIVE_UP, 0.0

        if exc is not None:
            if not isinstance(exc, policy.retry_on_exception):
                return RetryDecision.GIVE_UP, 0.0
            return RetryDecision.RETRY_SAME_PRIORITY, self.next_delay(attempt, _retry_after_hint(exc))

        if response is None:
            return RetryDecision.GIVE_UP, 0.0

        status = _status_of(response)
        if status in policy.no_retry_on_status:
            return RetryDecision.GIVE_UP, 0.0
        if status not in policy.retry_on_status:
            return RetryDecision.GIVE_UP, 0.0

        delay = self.next_delay(attempt, _retry_after_hint(response))

        if 500 <= status < 600:
            # First look at a broken origin deserves a fresh slot; later ones queue.
            if attempt == 0:
                return RetryDecision.RETRY_HIGH_PRIORITY, delay
            return RetryDecision.RETRY_SAME_PRIORITY, delay

        if status == 429:
            if has_proxy(request):
                return RetryDecision.RETRY_WITH_NEW_PROXY, delay
            return RetryDecision.RETRY_HIGH_PRIORITY, delay

        return RetryDecision.RETRY_SAME_PRIORITY, delay


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
def _status_of(response: Any) -> int:
    status = getattr(response, "status", None)
    if status is None:
        status = getattr(response, "status_code", 0)
    try:
        return int(status or 0)
    except (TypeError, ValueError):
        return 0


def _retry_after_hint(source: Any) -> Optional[float]:
    """Read `Retry-After` from a response's headers or from an exception attribute."""
    if source is None:
        return None

    headers = getattr(source, "headers", None)
    if headers is None:
        value = getattr(source, "retry_after", None)
        try:
            return float(value) if value is not None else None
        except (TypeError, ValueError):
            return None

    try:
        raw = headers.get("Retry-After") or headers.get("retry-after")
    except AttributeError:
        return None
    if raw is None:
        return None

    text = str(raw).strip()
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        pass
    try:  # HTTP-date form, e.g. "Wed, 21 Oct 2026 07:28:00 GMT"
        delta = parsedate_to_datetime(text).timestamp() - time.time()
        return max(0.0, delta)
    except (TypeError, ValueError, OverflowError):
        return None
