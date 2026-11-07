"""Route handlers: profile selection, analytics blocking and domain blocking, in both flavours."""

import pytest

from scrapling.engines.toolbelt.navigation import (
    create_async_intercept_handler,
    create_intercept_handler,
)


class _Request:
    def __init__(self, url: str, resource_type: str) -> None:
        self.url = url
        self.resource_type = resource_type


class _Route:
    """Records what the handler decided; Playwright's own behaviour is out of scope here."""

    def __init__(self, url: str, resource_type: str) -> None:
        self.request = _Request(url, resource_type)
        self.aborted = False
        self.continued = False

    def abort(self):
        self.aborted = True

    def continue_(self):
        self.continued = True

    async def aabort(self):
        self.aborted = True

    async def acontinue(self):
        self.continued = True


class _AsyncRoute(_Route):
    async def abort(self):
        self.aborted = True

    async def continue_(self):
        self.continued = True


IMG = "https://example.com/photo.png"
GA = "https://www.google-analytics.com/analytics.js"


class TestSyncHandler:
    def test_speed_profile_aborts_images(self):
        route = _Route(IMG, "image")
        create_intercept_handler(resource_profile="speed")(route)
        assert route.aborted and not route.continued

    def test_fidelity_profile_continues_images(self):
        route = _Route(IMG, "image")
        create_intercept_handler(resource_profile="fidelity")(route)
        assert route.continued and not route.aborted

    def test_fidelity_profile_aborts_beacons(self):
        route = _Route("https://example.com/b", "beacon")
        create_intercept_handler(resource_profile="fidelity")(route)
        assert route.aborted

    def test_none_profile_continues_images(self):
        route = _Route(IMG, "image")
        create_intercept_handler(resource_profile="none")(route)
        assert route.continued

    def test_analytics_blocking_aborts_ga(self):
        route = _Route(GA, "script")
        create_intercept_handler(block_analytics=True)(route)
        assert route.aborted

    def test_disable_resources_alone_means_speed(self):
        route = _Route(IMG, "image")
        create_intercept_handler(disable_resources=True)(route)
        assert route.aborted

    def test_disable_resources_defers_to_an_explicit_profile(self):
        route = _Route(IMG, "image")
        create_intercept_handler(disable_resources=True, resource_profile="fidelity")(route)
        assert route.continued

    def test_blocked_domains_still_abort(self):
        route = _Route("https://tracker.ads.example.net/pixel", "script")
        create_intercept_handler(blocked_domains={"ads.example.net"})(route)
        assert route.aborted

    def test_blocked_domains_do_not_abort_other_hosts(self):
        route = _Route("https://cdn.example.com/app.js", "script")
        create_intercept_handler(blocked_domains={"ads.example.net"})(route)
        assert route.continued

    def test_nothing_configured_just_continues(self):
        route = _Route(IMG, "image")
        create_intercept_handler()(route)
        assert route.continued


class TestAsyncHandler:
    @pytest.mark.asyncio
    async def test_speed_profile_aborts_images(self):
        route = _AsyncRoute(IMG, "image")
        await create_async_intercept_handler(resource_profile="speed")(route)
        assert route.aborted and not route.continued

    @pytest.mark.asyncio
    async def test_fidelity_profile_continues_images(self):
        route = _AsyncRoute(IMG, "image")
        await create_async_intercept_handler(resource_profile="fidelity")(route)
        assert route.continued and not route.aborted

    @pytest.mark.asyncio
    async def test_fidelity_profile_aborts_beacons(self):
        route = _AsyncRoute("https://example.com/b", "beacon")
        await create_async_intercept_handler(resource_profile="fidelity")(route)
        assert route.aborted

    @pytest.mark.asyncio
    async def test_none_profile_continues_images(self):
        route = _AsyncRoute(IMG, "image")
        await create_async_intercept_handler(resource_profile="none")(route)
        assert route.continued

    @pytest.mark.asyncio
    async def test_analytics_blocking_aborts_ga(self):
        route = _AsyncRoute(GA, "script")
        await create_async_intercept_handler(block_analytics=True)(route)
        assert route.aborted

    @pytest.mark.asyncio
    async def test_disable_resources_alone_means_speed(self):
        route = _AsyncRoute(IMG, "image")
        await create_async_intercept_handler(disable_resources=True)(route)
        assert route.aborted

    @pytest.mark.asyncio
    async def test_disable_resources_defers_to_an_explicit_profile(self):
        route = _AsyncRoute(IMG, "image")
        await create_async_intercept_handler(disable_resources=True, resource_profile="fidelity")(route)
        assert route.continued

    @pytest.mark.asyncio
    async def test_blocked_domains_still_abort(self):
        route = _AsyncRoute("https://tracker.ads.example.net/pixel", "script")
        await create_async_intercept_handler(blocked_domains={"ads.example.net"})(route)
        assert route.aborted

    @pytest.mark.asyncio
    async def test_blocked_domains_do_not_abort_other_hosts(self):
        route = _AsyncRoute("https://cdn.example.com/app.js", "script")
        await create_async_intercept_handler(blocked_domains={"ads.example.net"})(route)
        assert route.continued
