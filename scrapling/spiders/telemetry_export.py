"""Optional Prometheus scrape endpoint — stdlib only, off unless asked.

The exporter is deliberately tiny: a thread, a handler, and ``registry.prometheus_text()``.
Nothing is scheduled, nothing is pushed; Prometheus pulls when it wants.
"""

from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Thread
from typing import Optional

from scrapling.spiders.telemetry import MetricsRegistry

__all__ = ["TelemetryExporter"]

_CONTENT_TYPE = "text/plain; version=0.0.4; charset=utf-8"


class _Handler(BaseHTTPRequestHandler):
    registry: MetricsRegistry  # set per server instance

    def do_GET(self) -> None:  # noqa: N802  (http.server naming)
        path = self.path.split("?", 1)[0]
        if path not in ("/metrics", "/"):
            self._send(404, b"not found\n")
            return
        if path == "/":
            self._send(200, b"scrapling telemetry: GET /metrics\n")
            return
        self._send(200, self.registry.prometheus_text().encode("utf-8"))

    def _send(self, status: int, body: bytes) -> None:
        self.send_response(status)
        self.send_header("Content-Type", _CONTENT_TYPE)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *args) -> None:  # noqa: A002
        """Silence the default stderr access log; the crawl's own logs are enough."""


class TelemetryExporter:
    """Serves ``/metrics`` on ``host:port`` from a background thread.

    ``port=0`` means "do not listen at all": a spider collecting metrics in-process pays
    neither a socket nor a thread. ``start()`` on such an exporter is a no-op, ``url()``
    stays ``None``, and ``stop()`` is always safe to call.
    """

    def __init__(self, registry: MetricsRegistry, host: str = "127.0.0.1", port: int = 0):
        self.registry = registry
        self.host = host
        self.port = port
        self._server: Optional[ThreadingHTTPServer] = None
        self._thread: Optional[Thread] = None

    @property
    def running(self) -> bool:
        return self._server is not None

    def start(self) -> None:
        if self._server is not None or self.port == 0:
            return
        registry = self.registry

        class Handler(_Handler):
            pass

        Handler.registry = registry
        server = ThreadingHTTPServer((self.host, self.port), Handler)
        server.daemon_threads = True
        self._server = server
        # 0.1s poll: shutdown() returns fast, so a test spider does not pay half a second per stop.
        self._thread = Thread(
            target=server.serve_forever,
            kwargs={"poll_interval": 0.1},
            name="scrapling-telemetry",
            daemon=True,
        )
        self._thread.start()

    def stop(self) -> None:
        server, thread = self._server, self._thread
        self._server = self._thread = None
        if server is None:
            return
        server.shutdown()
        server.server_close()
        if thread is not None:
            thread.join(timeout=5.0)

    def url(self) -> Optional[str]:
        """The scrape URL, or ``None`` when nothing is listening."""
        server = self._server
        if server is None:
            return None
        host, port = server.server_address[:2]
        return f"http://{host}:{port}/metrics"

    def __enter__(self) -> "TelemetryExporter":
        self.start()
        return self

    def __exit__(self, *exc_info) -> None:
        self.stop()
