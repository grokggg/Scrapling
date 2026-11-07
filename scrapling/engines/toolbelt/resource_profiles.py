"""Resource-blocking profiles and analytics URL recognition.

Three profiles trade payload against layout fidelity:

* ``speed``    - drop everything that does not affect the DOM text: images, fonts, media,
                 stylesheets, websockets. Fastest, layout destroyed.
* ``fidelity`` - drop only pure tracking cargo (beacons, CSP reports, objects, texttracks).
                 Stylesheets and websockets survive, so screenshots stay faithful.
* ``none``     - block nothing; the engine behaves as it did before profiles existed.

Blocking by resource type misses inline and first-party analytics: a page can serve
``/analytics.js`` from its own host, and ``gtag/js`` is routinely proxied. Hence the URL
patterns below, which are matched against the request URL regardless of host.
"""

import re
from typing import FrozenSet

#: Speed profile: no images, no fonts, no media, no CSS, no sockets.
SPEED_PROFILE: FrozenSet[str] = frozenset(
    {
        "font",
        "image",
        "imageset",
        "media",
        "beacon",
        "object",
        "texttrack",
        "websocket",
        "csp_report",
        "stylesheet",
    }
)

#: Fidelity profile: only cargo that carries no visual weight.
FIDELITY_PROFILE: FrozenSet[str] = frozenset(
    {
        "beacon",
        "csp_report",
        "object",
        "imageset",
        "texttrack",
    }
)

RESOURCE_PROFILES: dict[str, FrozenSet[str]] = {
    "speed": SPEED_PROFILE,
    "fidelity": FIDELITY_PROFILE,
    "none": frozenset(),
}

#: Valid values, for validation messages and ``PlaywrightConfig``.
PROFILE_NAMES: tuple[str, ...] = ("none", "speed", "fidelity")

#: Analytics/tracking script paths. Matched case-insensitively against the whole URL, so both
#: third-party hosts and first-party paths (``/gtag/js``, ``/analytics.js``) are caught.
ANALYTICS_URL_PATTERNS: tuple[re.Pattern, ...] = tuple(
    re.compile(p, re.I)
    for p in (
        r"/google-analytics\.com/",
        r"/googletagmanager\.com/",
        r"/gtag/js",
        r"/gtm\.js",
        r"/analytics\.js",
        r"/ga\.js",
        r"/piwik\.js",
        r"/matomo\.js",
        r"/facebook\.net/.*/fbevents\.js",
        r"/connect\.facebook\.net/",
        r"/hotjar\.com/",
        r"/hotjar-",
        r"/mixpanel\.com/",
        r"/mixpanel-",
        r"/segment\.(com|io)/",
        r"/cdn\.segment\.com/",
        r"/amplitude\.com/",
        r"/fullstory\.com/",
        r"/clarity\.ms/",
        r"/yandex\.ru/metrika",
        r"/mc\.yandex\.ru/",
        r"/stat\.counter",
        r"/hm\.baidu\.com/",
        r"/cnzz\.com/",
        r"/umeng\.com/",
        r"/sentry\.io/",
        r"/bugsnag\.com/",
        r"/newrelic\.com/",
    )
)


def is_analytics_url(url: str) -> bool:
    """Whether `url` looks like an analytics/tracking script. Pure, no IO."""
    return any(p.search(url) for p in ANALYTICS_URL_PATTERNS)


def should_block(
    url: str,
    resource_type: str,
    *,
    resource_profile: str = "none",
    block_analytics: bool = False,
) -> bool:
    """Whether a request should be aborted.

    True when its resource type is in the profile, or when analytics blocking is on and the
    URL matches one of the analytics patterns. Unknown profiles block nothing.
    """
    if resource_type in RESOURCE_PROFILES.get(resource_profile, frozenset()):
        return True
    return block_analytics and is_analytics_url(url)
