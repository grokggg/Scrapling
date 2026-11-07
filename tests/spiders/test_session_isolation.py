"""Per-domain session isolation: level-encoded sids, lazy creation, fingerprints, reset."""

import threading

import pytest

from scrapling.fetchers import AsyncStealthySession, FetcherSession
from scrapling.spiders import session as session_module
from scrapling.spiders.adaptive import AdaptiveEngine, FetchLevel
from scrapling.spiders.presets import APT_PRESET
from scrapling.spiders.fingerprint import DEFAULT_FINGERPRINTS
from scrapling.spiders.session import FINGERPRINT_POOL, SessionManager, session_sid

# impersonate → profile（edge101 是 curl_cffi 里 Edge 的唯一入口，映射唯一）
FINGERPRINT_LOOKUP = {fp.impersonate: fp for fp in DEFAULT_FINGERPRINTS}
from scrapling.spiders.signals import BlockSignal

DOMAIN = "example.com"
OTHER = "other.com"
DEFAULT = "static:0:__default__"


@pytest.fixture
def manager() -> SessionManager:
    return SessionManager().add(DEFAULT, FetcherSession(), default=True)


class _FakeSession:
    """A session stand-in that records its own shutdown."""

    def __init__(self) -> None:
        self._is_alive = True
        self.exits = 0

    async def __aexit__(self, *exc) -> None:
        self.exits += 1
        self._is_alive = False


class TestSidNaming:
    def test_static_levels_encode_the_level(self):
        levels = [level for level in FetchLevel if level != FetchLevel.BROWSER]
        assert [session_sid(DOMAIN, level) for level in levels] == [
            f"static:0:{DOMAIN}",
            f"static:1:{DOMAIN}",
            f"static:2:{DOMAIN}",
            f"static:3:{DOMAIN}",
        ]

    def test_browser_level_encodes_four(self):
        assert session_sid(DOMAIN, FetchLevel.BROWSER) == f"browser:4:{DOMAIN}"

    def test_hostless_domain_uses_the_reserved_key(self):
        assert session_sid("", FetchLevel.DIRECT) == DEFAULT
        assert session_sid("", FetchLevel.BROWSER) == "browser:4:__default__"

    def test_pool_has_enough_fingerprints(self):
        assert len(FINGERPRINT_POOL) == 5
        assert {"chrome120", "safari17_2_ios", "firefox133"} <= set(FINGERPRINT_POOL.all_names())


class TestEnsure:
    def test_two_domains_get_different_sids(self, manager):
        a = manager.ensure_domain_session(DOMAIN, FetchLevel.DIRECT)
        b = manager.ensure_domain_session(OTHER, FetchLevel.DIRECT)

        assert (a, b) == (f"static:0:{DOMAIN}", f"static:0:{OTHER}")
        assert manager.get(a) is not manager.get(b)

    def test_same_domain_and_level_reuses_the_sid_and_the_session(self, manager):
        sid = manager.ensure_domain_session(DOMAIN, FetchLevel.DIRECT)
        session = manager.get(sid)

        assert manager.ensure_domain_session(DOMAIN, FetchLevel.DIRECT) == sid
        assert manager.get(sid) is session
        assert len(manager) == 2  # default + this domain

    def test_same_domain_at_browser_gets_its_own_sid(self, manager):
        assert manager.ensure_domain_session(DOMAIN, FetchLevel.DIRECT) == f"static:0:{DOMAIN}"
        assert manager.ensure_domain_session(DOMAIN, FetchLevel.BROWSER) == f"browser:4:{DOMAIN}"

    def test_browser_level_builds_a_stealth_session(self, manager):
        sid = manager.ensure_domain_session(DOMAIN, FetchLevel.BROWSER)
        session = manager.get(sid)

        assert isinstance(session, AsyncStealthySession)
        assert session._is_alive is False  # registered lazily, never started here

    def test_static_levels_actually_differ_on_the_wire(self, manager):
        direct = manager.get(manager.ensure_domain_session(DOMAIN, FetchLevel.DIRECT))
        headers = manager.get(manager.ensure_domain_session(DOMAIN, FetchLevel.HEADERS))

        assert direct._stealth is False
        assert headers._stealth is True
        assert direct is not headers

    def test_proxy_level_carries_no_proxy_of_its_own(self, manager):
        session = manager.get(manager.ensure_domain_session(DOMAIN, FetchLevel.PROXY))

        assert isinstance(session, FetcherSession)
        # A proxy is per-request (`request._session_kwargs`), never baked into the session.
        assert session._default_proxy is None
        assert session._default_proxies == {}
        assert session._proxy_rotator is None

    def test_domain_sessions_are_lazy(self, manager):
        sid = manager.ensure_domain_session(DOMAIN, FetchLevel.DIRECT)

        assert sid in manager._lazy_sessions
        assert manager.get(sid)._is_alive is False

    def test_ten_concurrent_ensures_create_one_session(self):
        manager = SessionManager().add(DEFAULT, FetcherSession(), default=True)
        sids = []

        def hammer():
            sids.append(manager.ensure_domain_session(DOMAIN, FetchLevel.DIRECT))

        threads = [threading.Thread(target=hammer) for _ in range(10)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        assert len(set(sids)) == 1
        assert len(manager) == 2


class TestUpgradeAllocatesANewSession:
    def test_upgrading_to_headers_yields_a_new_sid_and_keeps_the_old(self, manager):
        direct = manager.ensure_domain_session(DOMAIN, FetchLevel.DIRECT)
        direct_session = manager.get(direct)

        headers = manager.ensure_domain_session(DOMAIN, FetchLevel.HEADERS)

        assert (direct, headers) == (f"static:0:{DOMAIN}", f"static:1:{DOMAIN}")
        # The DIRECT session is left in place for whatever retry is still in flight.
        assert manager.get(direct) is direct_session
        assert manager.get(headers) is not direct_session

    def test_ladder_walks_five_distinct_sids(self):
        engine = AdaptiveEngine(APT_PRESET)
        sids = []

        for _ in range(4):
            engine.observe(DOMAIN, BlockSignal.CAPTCHA, 0.9)
            sids.append(session_sid(DOMAIN, engine.plan(DOMAIN)[0]))

        assert sids == [
            f"static:1:{DOMAIN}",
            f"static:2:{DOMAIN}",
            f"static:3:{DOMAIN}",
            f"browser:4:{DOMAIN}",
        ]


class TestFingerprintRotation:
    def test_tls_rung_pins_one_whole_profile_per_domain(self):
        session = session_module._build_session(FetchLevel.TLS, FINGERPRINT_POOL, DOMAIN)

        profile = FINGERPRINT_LOOKUP[session._default_impersonate]
        assert session._default_headers == profile.to_headers()
        assert session._stealth is True

    def test_rebuilding_the_same_domain_is_deterministic(self):
        first = session_module._build_session(FetchLevel.TLS, FINGERPRINT_POOL, DOMAIN)
        again = session_module._build_session(FetchLevel.TLS, FINGERPRINT_POOL, DOMAIN)

        assert first._default_impersonate == again._default_impersonate
        assert first._default_headers == again._default_headers

    def test_the_salt_walks_to_another_profile(self):
        base = session_module._build_session(FetchLevel.TLS, FINGERPRINT_POOL, DOMAIN)
        rotated = session_module._build_session(FetchLevel.TLS, FINGERPRINT_POOL, DOMAIN, salt=1)

        assert rotated._default_impersonate != base._default_impersonate

    def test_tls_and_proxy_share_the_profile(self):
        tls = session_module._build_session(FetchLevel.TLS, FINGERPRINT_POOL, DOMAIN)
        proxy = session_module._build_session(FetchLevel.PROXY, FINGERPRINT_POOL, DOMAIN)

        assert tls._default_impersonate == proxy._default_impersonate
        assert tls._default_headers == proxy._default_headers
        assert tls._stealth is proxy._stealth is True

    def test_manager_accepts_a_named_pool(self):
        manager = SessionManager()
        manager.fingerprint_pool = [FINGERPRINT_LOOKUP["firefox133"]]

        session = manager.get(manager.ensure_domain_session(DOMAIN, FetchLevel.TLS))

        assert session._default_impersonate == "firefox133"
        assert "Firefox/133.0" in session._default_headers["User-Agent"]


class TestReset:
    @pytest.mark.asyncio
    async def test_reset_closes_and_removes_every_level_of_the_domain(self, manager):
        fake = _FakeSession()
        manager.add(f"static:0:{DOMAIN}", fake, lazy=True)
        manager.ensure_domain_session(DOMAIN, FetchLevel.HEADERS)
        manager.ensure_domain_session(DOMAIN, FetchLevel.BROWSER)

        await manager.reset_domain(DOMAIN)

        assert fake.exits == 1
        assert [sid for sid in manager.session_ids if sid.endswith(DOMAIN)] == []

    @pytest.mark.asyncio
    async def test_reset_rebuilds_a_fresh_session_on_the_next_ensure(self, manager):
        sid = manager.ensure_domain_session(DOMAIN, FetchLevel.DIRECT)
        first = manager.get(sid)

        await manager.reset_domain(DOMAIN)
        again = manager.ensure_domain_session(DOMAIN, FetchLevel.DIRECT)

        assert again == sid
        assert manager.get(again) is not first

    @pytest.mark.asyncio
    async def test_reset_leaves_other_domains_alone(self, manager):
        manager.ensure_domain_session(DOMAIN, FetchLevel.DIRECT)
        manager.ensure_domain_session(OTHER, FetchLevel.HEADERS)
        other_session = manager.get(f"static:1:{OTHER}")

        await manager.reset_domain(DOMAIN)

        assert f"static:0:{DOMAIN}" not in manager
        assert manager.get(f"static:1:{OTHER}") is other_session

    @pytest.mark.asyncio
    async def test_reset_never_removes_the_default(self, manager):
        await manager.reset_domain("__default__")

        assert manager.default_session_id == DEFAULT
        assert DEFAULT in manager

    @pytest.mark.asyncio
    async def test_reset_of_unknown_domain_is_a_no_op(self, manager):
        await manager.reset_domain("never-seen.com")

        assert manager.session_ids == [DEFAULT]

    @pytest.mark.asyncio
    async def test_reset_skips_exiting_sessions_that_never_started(self, manager):
        sid = manager.ensure_domain_session(DOMAIN, FetchLevel.DIRECT)
        assert manager.get(sid)._is_alive is False

        await manager.reset_domain(DOMAIN)  # must not raise "Cannot exit invalid session"

        assert sid not in manager


class TestFingerprintBinding:
    """The TLS/PROXY rungs must pin a whole browser, not just a TLS string."""

    def test_rebuilds_after_reset_rotate_the_profile(self):
        manager = SessionManager()
        seen = []

        async def rebuild():
            sid = manager.ensure_domain_session(DOMAIN, FetchLevel.TLS)
            seen.append(manager.get(sid)._default_impersonate)
            await manager.reset_domain(DOMAIN)

        import anyio

        for _ in range(10):
            anyio.run(rebuild)

        assert len(seen) == 10
        assert len(set(seen)) >= 2

    def test_same_domain_without_reset_keeps_the_profile(self):
        manager = SessionManager()
        first = manager.get(manager.ensure_domain_session(DOMAIN, FetchLevel.TLS))._default_impersonate

        for _ in range(5):
            sid = manager.ensure_domain_session(DOMAIN, FetchLevel.TLS)
            assert manager.get(sid)._default_impersonate == first

    def test_direct_sets_no_extra_headers(self):
        manager = SessionManager()
        session = manager.get(manager.ensure_domain_session(DOMAIN, FetchLevel.DIRECT))

        assert session._stealth is False
        assert not session._default_headers

    def test_headers_rung_shapes_headers_but_keeps_the_default_ua(self):
        manager = SessionManager()
        session = manager.get(manager.ensure_domain_session(DOMAIN, FetchLevel.HEADERS))

        assert session._stealth is True
        assert not session._default_headers

    def test_tls_rung_pins_ua_language_and_client_hints(self):
        manager = SessionManager()
        session = manager.get(manager.ensure_domain_session(DOMAIN, FetchLevel.TLS))
        headers = session._default_headers

        assert session._stealth is True
        assert headers["User-Agent"].startswith("Mozilla/5.0")
        assert "Accept-Language" in headers
        assert "Accept-Encoding" in headers

    def test_tls_profile_matches_its_impersonate(self):
        manager = SessionManager()
        session = manager.get(manager.ensure_domain_session(DOMAIN, FetchLevel.TLS))
        profile = FINGERPRINT_LOOKUP[session._default_impersonate]

        assert session._default_headers == profile.to_headers()

    def test_proxy_rung_uses_the_same_profiles_as_tls(self):
        manager = SessionManager()
        tls = manager.get(manager.ensure_domain_session(DOMAIN, FetchLevel.TLS))
        proxy = manager.get(manager.ensure_domain_session(DOMAIN, FetchLevel.PROXY))

        assert proxy._default_impersonate == tls._default_impersonate
        assert proxy._default_headers == tls._default_headers

    def test_spider_pool_reaches_the_manager(self):
        from scrapling.spiders.fingerprint import APT_FINGERPRINTS, FingerprintPool

        manager = SessionManager()
        manager.fingerprint_pool = FingerprintPool(APT_FINGERPRINTS)
        session = manager.get(manager.ensure_domain_session(DOMAIN, FetchLevel.TLS))

        assert session._default_impersonate in {fp.impersonate for fp in APT_FINGERPRINTS}

    def test_setting_none_restores_the_default_pool(self):
        manager = SessionManager()
        manager.fingerprint_pool = None

        assert manager.fingerprint_pool.all_names() == [
            "chrome120",
            "chrome131",
            "safari17_2_ios",
            "firefox133",
            "edge131",
        ]
