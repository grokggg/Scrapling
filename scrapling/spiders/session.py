from asyncio import Lock
from threading import Lock as ThreadLock

from scrapling.spiders.request import Request
from scrapling.spiders.adaptive import FetchLevel
from scrapling.engines.static import _ASyncSessionLogic
from scrapling.engines.toolbelt.convertor import Response
from scrapling.core._types import Set, cast, SUPPORTED_HTTP_METHODS
from scrapling.fetchers import AsyncStealthySession, FetcherSession
from scrapling.spiders.fingerprint import DEFAULT_FINGERPRINTS, BrowserFingerprint, FingerprintPool

Session = FetcherSession | AsyncStealthySession

# Requests without a host (rare: `Request("")`) still need a registry key.
NO_DOMAIN = "__default__"
# The sid `Spider.configure_sessions()` registers by default. Its presence means the
# framework owns sid dispatch, which is what enables per-domain sessions.
STATIC_DEFAULT_SID = "static:0:__default__"

# Profiles, not bare impersonate strings: the TLS handshake and the HTTP headers now come
# from the same real browser. Kept under the old name because `CrawlerEngine` seeds
# `SessionManager.fingerprint_pool` with it; the setter below accepts a pool, a list of
# names, or None.
FINGERPRINT_POOL = FingerprintPool(DEFAULT_FINGERPRINTS)


def _build_session(
    level: FetchLevel,
    fingerprint_pool: "FingerprintPool | None" = None,
    domain: str = "",
    salt: int = 0,
) -> Session:
    """Create the session that `level` maps to. Never started here — registration is lazy.

    The static rungs differ in what actually goes on the wire, not just in name:
    DIRECT sends curl_cffi's own headers, HEADERS adds browser-shaped ones, and TLS/PROXY
    pin a whole browser profile so the handshake and the headers cannot disagree.
    """
    if level == FetchLevel.BROWSER:
        return AsyncStealthySession(headless=True, real_chrome=False, hide_canvas=True, allow_webgl=True)
    if level == FetchLevel.DIRECT:
        return FetcherSession(impersonate="chrome", stealthy_headers=False)
    if level == FetchLevel.HEADERS:
        return FetcherSession(impersonate="chrome", stealthy_headers=True)
    # TLS and PROXY: same wire profile, only the proxy differs (per-request `_session_kwargs`).
    profile = (fingerprint_pool or FINGERPRINT_POOL).pick_for_domain(domain, "sticky", salt=salt)
    return FetcherSession(
        impersonate=profile.impersonate,
        stealthy_headers=True,
        headers=profile.to_headers(),
    )


def session_sid(domain: str, level: FetchLevel) -> str:
    """`static:{0..3}:example.com` up to PROXY, `browser:4:example.com` at BROWSER.

    The level is part of the key on purpose: an upgrade must be a new session, not a
    re-parameterisation of the old one, or the ladder would be a no-op.
    """
    prefix = "browser" if level == FetchLevel.BROWSER else "static"
    return f"{prefix}:{int(level)}:{domain or NO_DOMAIN}"


class SessionManager:
    """Manages pre-configured session instances."""

    def __init__(self, fingerprint_pool: "FingerprintPool | None" = None) -> None:
        self._sessions: dict[str, Session] = {}
        self._default_session_id: str | None = None
        self._started: bool = False
        self._lazy_sessions: Set[str] = set()
        self._lazy_lock = Lock()
        # Registry mutations happen from sync code (`ensure_domain_session`), so the guard
        # is a thread lock; `_lazy_lock` still serialises the async `__aenter__` calls.
        self._registry_lock = ThreadLock()
        # Overridable fingerprint source; `CrawlerEngine` seeds it from `spider.fingerprint_pool`.
        self._fingerprint_pool: FingerprintPool = fingerprint_pool or FINGERPRINT_POOL
        # Bumped per reset so a rebuilt session does not get the burnt profile back.
        self._fingerprint_rotation: dict[str, int] = {}

    @property
    def fingerprint_pool(self) -> FingerprintPool:
        return self._fingerprint_pool

    @fingerprint_pool.setter
    def fingerprint_pool(self, value: "FingerprintPool | list | None") -> None:
        """Accept a pool, profile objects, or profile names — None means the default set."""
        if value is None:
            self._fingerprint_pool = FINGERPRINT_POOL
        elif isinstance(value, FingerprintPool):
            self._fingerprint_pool = value
        else:
            self._fingerprint_pool = FingerprintPool(
                item if isinstance(item, BrowserFingerprint) else FINGERPRINT_POOL.get(item) for item in value
            )

    def add(self, session_id: str, session: Session, *, default: bool = False, lazy: bool = False) -> "SessionManager":
        """Register a session instance.

        :param session_id: Name to reference this session in requests
        :param session: Your pre-configured session instance
        :param default: If True, this becomes the default session
        :param lazy: If True, the session will be started only when a request uses its ID.
        """
        if session_id in self._sessions:
            raise ValueError(f"Session '{session_id}' already registered")

        self._sessions[session_id] = session

        if default or self._default_session_id is None:
            self._default_session_id = session_id

        if lazy:
            self._lazy_sessions.add(session_id)

        return self

    def ensure_domain_session(self, domain: str, level: FetchLevel) -> str:
        """按 domain + level 确保对应 session 存在，返回 sid。

        level == BROWSER → browser:4:{domain}（懒启动 AsyncStealthySession）
        其它 level → static:{int(level)}:{domain}（懒启动 FetcherSession，带 level 对应参数）

        返回的 sid 可直接赋给 request.sid
        """
        sid = session_sid(domain, level)
        with self._registry_lock:
            if sid not in self._sessions:
                salt = self._fingerprint_rotation.get(domain or NO_DOMAIN, 0)
                previous_default = self._default_session_id
                self.add(sid, _build_session(level, self._fingerprint_pool, domain, salt), lazy=True)
                # A lazily ensured session must never steal the default slot: `reset_domain`
                # spares the default, so hijacking it would leave a burnt session alive.
                if previous_default is None:
                    self._default_session_id = None
        return sid

    async def reset_domain(self, domain: str) -> None:
        """关闭并移除该域名的所有 session，下次 ensure 会重新创建。

        Matches every level of one host (the last colon-separated field is the domain),
        never the framework default.
        """
        d = domain or NO_DOMAIN
        with self._registry_lock:
            doomed = [
                sid
                for sid in self._sessions
                if sid != self._default_session_id and sid.rsplit(":", 1)[-1] == d
            ]
            sessions = [self._sessions.pop(sid) for sid in doomed]
            for sid in doomed:
                self._lazy_sessions.discard(sid)
            # Next build for this host picks a different profile than the one that just died.
            self._fingerprint_rotation[d] = self._fingerprint_rotation.get(d, 0) + 1

        for session in sessions:
            # Only live sessions can be exited; a registered-but-never-used one is dropped as is.
            if getattr(session, "_is_alive", False):
                await session.__aexit__(None, None, None)

    def remove(self, session_id: str) -> None:
        """Removes a session.

        :param session_id: ID of session to remove
        """
        _ = self.pop(session_id)

    def pop(self, session_id: str) -> Session:
        """Remove and returns a session.

        :param session_id: ID of session to remove
        """
        if session_id not in self._sessions:
            raise KeyError(f"Session '{session_id}' not found")

        session = self._sessions.pop(session_id)
        if session_id in self._lazy_sessions:
            self._lazy_sessions.remove(session_id)

        if session and self._default_session_id == session_id:
            self._default_session_id = next(iter(self._sessions), None)

        return session

    @property
    def default_session_id(self) -> str:
        if self._default_session_id is None:
            raise RuntimeError("No sessions registered")
        return self._default_session_id

    @property
    def session_ids(self) -> list[str]:
        return list(self._sessions.keys())

    def get(self, session_id: str) -> Session:
        if session_id not in self._sessions:
            available = ", ".join(self._sessions.keys())
            raise KeyError(f"Session '{session_id}' not found. Available: {available}")
        return self._sessions[session_id]

    async def start(self) -> None:
        """Start all sessions that aren't already alive."""
        if self._started:
            return

        for sid, session in self._sessions.items():
            if sid not in self._lazy_sessions and not session._is_alive:
                await session.__aenter__()

        self._started = True

    async def close(self) -> None:
        """Close all registered sessions."""
        for sid, session in self._sessions.items():
            if sid in self._lazy_sessions and not session._is_alive:
                continue
            _ = await session.__aexit__(None, None, None)

        self._started = False

    async def fetch(self, request: Request) -> Response:
        sid = request.sid if request.sid else self.default_session_id
        session = self.get(sid)

        if session:
            if sid in self._lazy_sessions and not session._is_alive:
                async with self._lazy_lock:
                    if not session._is_alive:
                        await session.__aenter__()

            if isinstance(session, FetcherSession):
                client = session._client

                if isinstance(client, _ASyncSessionLogic):
                    kwargs = request._session_kwargs.copy()
                    method = cast(SUPPORTED_HTTP_METHODS, kwargs.pop("method", "GET"))
                    if "stealthy_headers" in kwargs:
                        # Same mapping as the public session methods (`get`, `post`, ...), so it doesn't reach curl_cffi
                        kwargs["stealth"] = kwargs.pop("stealthy_headers")
                    response = await client._make_request(
                        method=method,
                        url=request.url,
                        **kwargs,
                    )
                else:
                    # Sync session or other types - shouldn't happen in async context
                    raise TypeError(f"Session type {type(client)} not supported for async fetch")
            else:
                response = await session.fetch(url=request.url, **request._session_kwargs)

            response.request = request
            # Merge request meta into response meta (response meta takes priority)
            response.meta = {**request.meta, **response.meta}
            return response
        raise RuntimeError("No session found with the request session id")

    async def __aenter__(self) -> "SessionManager":
        await self.start()
        return self

    async def __aexit__(self, *exc) -> None:
        await self.close()

    def __contains__(self, session_id: str) -> bool:
        """Check if a session ID is registered."""
        return session_id in self._sessions

    def __len__(self) -> int:
        """Number of registered sessions."""
        return len(self._sessions)
