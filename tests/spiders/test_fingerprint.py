"""Whole-browser profiles: every field belongs to the same browser, no mixing."""

import pytest

from scrapling.spiders.fingerprint import (
    APT_FINGERPRINTS,
    CHROME_120,
    CHROME_131,
    DEFAULT_FINGERPRINTS,
    EDGE_131,
    FIREFOX_133,
    SAFARI_17_2_IOS,
    BrowserFingerprint,
    FingerprintPool,
)

CHROME_NAMES = ("chrome120", "chrome131", "edge131")


@pytest.fixture
def pool() -> FingerprintPool:
    return FingerprintPool()


class TestProfiles:
    def test_every_profile_carries_the_required_headers(self):
        for profile in DEFAULT_FINGERPRINTS:
            headers = profile.to_headers()
            assert headers["User-Agent"] == profile.user_agent
            assert headers["Accept"] == profile.accept
            assert headers["Accept-Language"] == profile.accept_language
            assert headers["Accept-Encoding"] == profile.accept_encoding

    def test_chromium_profiles_send_client_hints(self):
        for profile in (CHROME_120, CHROME_131, EDGE_131):
            headers = profile.to_headers()
            assert headers["sec-ch-ua"] == profile.sec_ch_ua
            assert headers["sec-ch-ua-mobile"] == "?0"
            assert headers["sec-ch-ua-platform"].startswith('"')

    def test_safari_and_firefox_send_no_client_hints(self):
        for profile in (SAFARI_17_2_IOS, FIREFOX_133):
            headers = profile.to_headers()
            assert profile.sec_ch_ua is None
            assert not any(name.startswith("sec-ch-ua") for name in headers)

    def test_chrome_131_is_not_a_chrome_120_relabel(self):
        assert CHROME_131.user_agent != CHROME_120.user_agent
        assert CHROME_131.sec_ch_ua != CHROME_120.sec_ch_ua
        assert "131" in CHROME_131.user_agent and "131" in CHROME_131.sec_ch_ua

    def test_edge_reports_edge_and_uses_the_edge_tls_stack(self):
        headers = EDGE_131.to_headers()
        assert EDGE_131.impersonate == "edge101"  # curl_cffi has no edge131
        assert "Edg/131" in headers["User-Agent"]
        assert "Microsoft Edge" in headers["sec-ch-ua"]

    def test_safari_headers_keep_safari_order(self):
        names = list(SAFARI_17_2_IOS.to_headers())
        assert names.index("Accept-Encoding") < names.index("Accept")
        assert names.index("Accept") < names.index("User-Agent")

    def test_firefox_keeps_its_own_language_chain(self):
        assert FIREFOX_133.accept_language.startswith("zh-CN,zh;q=0.8")
        assert "Firefox/133.0" in FIREFOX_133.user_agent
        assert FIREFOX_133.impersonate == "firefox133"

    def test_extra_headers_are_merged(self):
        profile = BrowserFingerprint(
            name="custom",
            impersonate="chrome120",
            user_agent="UA",
            accept="*/*",
            accept_language="en",
            accept_encoding="gzip",
            extra_headers={"X-Test": "1"},
        )
        assert profile.to_headers()["X-Test"] == "1"

    def test_profiles_are_frozen(self):
        with pytest.raises(Exception):
            CHROME_120.user_agent = "tampered"

    def test_impersonate_targets_exist_in_curl_cffi(self):
        from curl_cffi.requests import BrowserType

        for profile in DEFAULT_FINGERPRINTS:
            assert profile.impersonate in {item.value for item in BrowserType}


class TestApply:
    def test_apply_to_session_kwargs_sets_both_halves(self):
        kwargs = {"timeout": 30}

        returned = CHROME_120.apply_to_session_kwargs(kwargs)

        assert returned is kwargs
        assert kwargs["impersonate"] == "chrome120"
        assert kwargs["headers"]["User-Agent"] == CHROME_120.user_agent
        assert kwargs["timeout"] == 30

    def test_apply_overwrites_a_previous_profile(self):
        kwargs = CHROME_120.apply_to_session_kwargs({})

        SAFARI_17_2_IOS.apply_to_session_kwargs(kwargs)

        assert kwargs["impersonate"] == "safari17_2_ios"
        assert "sec-ch-ua" not in kwargs["headers"]


class TestPool:
    def test_all_names_lists_the_default_set(self, pool):
        assert pool.all_names() == ["chrome120", "chrome131", "safari17_2_ios", "firefox133", "edge131"]
        assert len(pool) == 5

    def test_random_pick_visits_several_profiles(self, pool):
        picked = {pool.pick("random").name for _ in range(100)}
        assert len(picked) >= 3

    def test_round_robin_walks_the_ring(self, pool):
        names = [pool.pick("round_robin").name for _ in range(len(pool) * 2)]
        assert names[: len(pool)] == pool.all_names()
        assert names[len(pool):] == pool.all_names()

    def test_sticky_domain_is_stable(self, pool):
        first = pool.pick_for_domain("example.com", "sticky")
        for _ in range(10):
            assert pool.pick_for_domain("example.com", "sticky").name == first.name

    def test_different_domains_spread_over_the_pool(self, pool):
        domains = [f"host{i}.example.com" for i in range(40)]
        assert len({pool.pick_for_domain(d).name for d in domains}) >= 2

    def test_salt_rotates_the_choice(self, pool):
        base = pool.pick_for_domain("example.com", "sticky")
        # salts 1..n-1 walk the ring once; salt n would land back on the base profile
        rotated = {pool.pick_for_domain("example.com", "sticky", salt=salt).name for salt in range(1, len(pool))}
        assert base.name not in rotated
        assert len(rotated) == len(pool) - 1

    def test_get_returns_the_named_profile(self, pool):
        assert pool.get("safari17_2_ios") is SAFARI_17_2_IOS

    def test_unknown_name_lists_the_alternatives(self, pool):
        with pytest.raises(KeyError) as excinfo:
            pool.get("netscape4")
        assert "chrome120" in str(excinfo.value)

    def test_non_sticky_strategy_delegates(self, pool):
        picked = {pool.pick_for_domain("example.com", "round_robin").name for _ in range(len(pool))}
        assert picked == set(pool.all_names())

    def test_duplicate_names_are_rejected(self):
        with pytest.raises(ValueError):
            FingerprintPool([CHROME_120, CHROME_120])

    def test_an_empty_pool_is_rejected(self):
        with pytest.raises(ValueError):
            FingerprintPool([])

    def test_apt_pool_adds_mac_variants(self):
        apt = FingerprintPool(APT_FINGERPRINTS)
        assert len(apt) == 8
        assert {"chrome120_mac", "safari17_2_macos", "firefox133_mac"} <= set(apt.all_names())
        assert apt.get("chrome120_mac").sec_ch_ua_platform == '"macOS"'
        assert len({apt.get(n).impersonate for n in apt.all_names()}) >= 4
