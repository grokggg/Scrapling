"""Signal detection: one response in, one verdict out. No network, no fixtures."""

from dataclasses import FrozenInstanceError

import pytest

from scrapling.engines.toolbelt.custom import Response
from scrapling.spiders.signals import BlockSignal, SignalResult, detect_signal

LONG_BODY = b"<html><body>" + b"ordinary article text " * 60 + b"</body></html>"


def _resp(status: int = 200, body: bytes | str = b"", url: str = "https://example.com/page", **headers) -> Response:
    return Response(url, body, status, "probe", {}, headers, {}, _log=False)


class TestChallenge:
    def test_cf_mitigated_header(self):
        result = detect_signal(_resp(403, b"", **{"cf-mitigated": "challenge"}))
        assert result.signal is BlockSignal.CHALLENGE
        assert result.confidence == 0.9
        assert "cf-mitigated" in result.reason

    def test_cf_chl_bypass_header(self):
        assert detect_signal(_resp(200, LONG_BODY, **{"cf-chl-bypass": "1"})).signal is BlockSignal.CHALLENGE

    def test_cf_chl_opt_body(self):
        body = b"<script>window.__cf_chl_opt={cvId:'3'}</script>"
        result = detect_signal(_resp(200, body))
        assert result.signal is BlockSignal.CHALLENGE
        assert "__cf_chl_opt" in result.reason

    def test_just_a_moment_body(self):
        result = detect_signal(_resp(503, b"<html><title>Just a moment...</title></html>"))
        assert result.signal is BlockSignal.CHALLENGE
        assert result.confidence == 0.9

    def test_checking_your_browser_body(self):
        assert detect_signal(_resp(200, b"Checking your browser before accessing")).signal is BlockSignal.CHALLENGE

    def test_ddos_guard_body(self):
        assert detect_signal(_resp(200, b"<div id='ddos-guard'></div>")).signal is BlockSignal.CHALLENGE

    def test_sgcaptcha_body(self):
        assert detect_signal(_resp(200, b"<script src='/sgcaptcha.js'></script>")).signal is BlockSignal.CHALLENGE


class TestCaptcha:
    @pytest.mark.parametrize("marker", ["recaptcha", "hcaptcha", "g-recaptcha", "cf-turnstile", "captcha"])
    def test_english_markers(self, marker):
        result = detect_signal(_resp(200, f"<div class='{marker}'></div>".encode()))
        assert result.signal is BlockSignal.CAPTCHA
        assert result.confidence == 0.85

    @pytest.mark.parametrize("marker", ["请输入验证码", "请完成验证"])
    def test_chinese_markers(self, marker):
        assert detect_signal(_resp(200, marker.encode())).signal is BlockSignal.CAPTCHA

    @pytest.mark.parametrize("path", ["/captcha", "/verify", "/challenge"])
    def test_url_markers(self, path):
        result = detect_signal(_resp(200, LONG_BODY, url=f"https://example.com{path}?next=1"))
        assert result.signal is BlockSignal.CAPTCHA
        assert path in result.reason


class TestLoginWall:
    @pytest.mark.parametrize("path", ["/login", "/signin", "/auth", "/sso"])
    def test_url_markers(self, path):
        result = detect_signal(_resp(200, LONG_BODY, url=f"https://example.com{path}"))
        assert result.signal is BlockSignal.LOGIN_WALL
        assert result.confidence == 0.8

    def test_body_marker(self):
        assert detect_signal(_resp(200, b"<p>Sign in to continue</p>")).signal is BlockSignal.LOGIN_WALL

    def test_chinese_body_marker(self):
        assert detect_signal(_resp(200, "登录后查看".encode())).signal is BlockSignal.LOGIN_WALL

    def test_redirect_to_login(self):
        result = detect_signal(_resp(302, b"", **{"Location": "https://example.com/signin?next=/a"}))
        assert result.signal is BlockSignal.LOGIN_WALL
        assert "302" in result.reason

    def test_redirect_elsewhere_is_not_login(self):
        assert detect_signal(_resp(302, b"", **{"Location": "https://example.com/a/b"})).signal is BlockSignal.NONE


class TestRateLimit:
    def test_status_429(self):
        result = detect_signal(_resp(429, b"nope"))
        assert result.signal is BlockSignal.RATELIMIT
        assert result.confidence == 0.95

    def test_retry_after_header(self):
        assert detect_signal(_resp(503, b"", **{"Retry-After": "30"})).signal is BlockSignal.RATELIMIT

    def test_ratelimit_remaining_zero(self):
        assert detect_signal(_resp(200, LONG_BODY, **{"X-RateLimit-Remaining": "0"})).signal is BlockSignal.RATELIMIT

    def test_english_body(self):
        assert detect_signal(_resp(200, b"<h1>Too Many Requests</h1>")).signal is BlockSignal.RATELIMIT

    def test_chinese_body(self):
        assert detect_signal(_resp(200, "请求过于频繁，请稍后再试".encode())).signal is BlockSignal.RATELIMIT


class TestForbidden:
    @pytest.mark.parametrize("status", [401, 403, 407, 444])
    def test_hard_block_statuses(self, status):
        result = detect_signal(_resp(status, b"denied"))
        assert result.signal is BlockSignal.FORBIDDEN
        assert result.confidence == 0.9
        assert str(status) in result.reason

    @pytest.mark.parametrize("status", [400, 404, 410, 418, 451, 500, 502])
    def test_other_statuses_are_not_forbidden(self, status):
        assert detect_signal(_resp(status, LONG_BODY)).signal is BlockSignal.NONE


class TestEmptyAndNone:
    def test_short_200_is_empty(self):
        result = detect_signal(_resp(200, b"<html></html>"))
        assert result.signal is BlockSignal.EMPTY
        assert result.confidence == 0.7

    def test_threshold_is_configurable(self):
        body = b"x" * 600
        assert detect_signal(_resp(200, body)).signal is BlockSignal.NONE
        assert detect_signal(_resp(200, body), min_body_bytes=1000).signal is BlockSignal.EMPTY

    def test_short_404_is_not_empty(self):
        assert detect_signal(_resp(404, b"gone")).signal is BlockSignal.NONE

    def test_normal_page_is_none(self):
        result = detect_signal(_resp(200, LONG_BODY))
        assert result.signal is BlockSignal.NONE
        assert result.confidence == 0.0

    def test_result_is_frozen(self):
        result = detect_signal(_resp(200, LONG_BODY))
        assert isinstance(result, SignalResult)
        with pytest.raises(FrozenInstanceError):
            result.signal = BlockSignal.EMPTY


class TestPrecedence:
    def test_challenge_beats_captcha(self):
        body = b"<title>Just a moment...</title><div class='g-recaptcha'></div>"
        assert detect_signal(_resp(200, body)).signal is BlockSignal.CHALLENGE

    def test_captcha_beats_login(self):
        body = b"<div class='hcaptcha'></div><a>Sign in to continue</a>"
        assert detect_signal(_resp(200, body)).signal is BlockSignal.CAPTCHA

    def test_login_beats_ratelimit(self):
        assert detect_signal(_resp(429, b"<p>Sign in to continue</p>")).signal is BlockSignal.LOGIN_WALL

    def test_ratelimit_beats_forbidden(self):
        assert detect_signal(_resp(403, b"", **{"Retry-After": "5"})).signal is BlockSignal.RATELIMIT

    def test_forbidden_beats_empty(self):
        assert detect_signal(_resp(403, b"x")).signal is BlockSignal.FORBIDDEN

    def test_str_body_is_accepted(self):
        assert detect_signal(Response("https://e.com/p", "<h1>请求过于频繁</h1>", 200, "OK", {}, {}, {}, _log=False)).signal is BlockSignal.RATELIMIT
