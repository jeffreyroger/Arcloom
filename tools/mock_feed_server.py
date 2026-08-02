"""Local HTTP server serving static RSS/Atom fixtures for deterministic
ingestion testing (week 1 hardening deliverable).

Exists so pipeline/ingest.py can be exercised at high frequency — conditional
GET, robots.txt enforcement, timeouts, malformed/dead/stale feeds — without
hammering real publishers. stdlib http.server only, no new dependencies.

Endpoints, each backed by a static file in tests/fixtures/feeds/:
  /normal.xml     valid RSS, 20 entries, recent dates
  /atom.xml       valid Atom (not RSS)
  /etag.xml       valid RSS; ETag; 304 on matching If-None-Match     (FR-201)
  /lastmod.xml    valid RSS; Last-Modified; 304 on matching If-Modified-Since (FR-201)
  /fullbody.xml   entry <description> is a 5,000-char full article body (FR-204/NFR-602)
  /empty.xml      valid RSS, zero entries
  /stale.xml      valid RSS, newest entry 400 days old
  /malformed.xml  truncated XML -> feedparser bozo
  /403            HTTP 403
  /404            HTTP 404
  /slow           valid feed, sleeps 3s before responding
  /robots.txt     User-agent: * / Disallow: /blocked.xml            (FR-203)
  /blocked.xml    valid feed, disallowed by the robots.txt above     (FR-203)
  /dupes.xml      same 5 articles as /normal.xml, different URLs with utm_* params
                  (FR-207 canonicalization + FR-208 dedup)
  /wire_source.xml      one article, the "original" wire story
  /wire_syndicated.xml  same story, genuinely different URL (no shared host,
                        path, or tracking params) and a one-word-reworded
                        title -- what canonicalization can't dedupe but
                        simhash's near-duplicate bypass can (FR-301/FR-302)

Fixtures whose dates matter (recency, staleness) are templates: the literal
timestamps are placeholders of the form {{RFC822:-3h}} / {{ISO:-3h}} /
{{RFC822:-400d}}, rendered relative to wall-clock "now" at request time by
_render_fixture. This is what makes /normal.xml's entries genuinely recent
and /stale.xml's entry genuinely ~400 days old no matter when the server is
started, instead of rotting to a fixed calendar date.
"""

import argparse
import re
import sys
import time
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

FIXTURES_DIR = Path(__file__).resolve().parent.parent / "tests" / "fixtures" / "feeds"

ETAG_VALUE = '"fixture-etag-v1"'
LAST_MODIFIED_VALUE = "Wed, 21 Oct 2015 07:28:00 GMT"

_STATIC_XML_ROUTES = {
    "/normal.xml": "normal.xml",
    "/atom.xml": "atom.xml",
    "/fullbody.xml": "fullbody.xml",
    "/empty.xml": "empty.xml",
    "/stale.xml": "stale.xml",
    "/malformed.xml": "malformed.xml",
    "/blocked.xml": "blocked.xml",
    "/dupes.xml": "dupes.xml",
    "/wire_source.xml": "wire_source.xml",
    "/wire_syndicated.xml": "wire_syndicated.xml",
}

# Fixtures with {{RFC822:...}}/{{ISO:...}} date placeholders that must be
# rendered relative to now. empty.xml and malformed.xml carry no dates and
# malformed.xml's truncated XML must reach feedparser untouched, so both are
# served as raw bytes instead.
_TEMPLATED_FIXTURES = {
    "normal.xml",
    "atom.xml",
    "fullbody.xml",
    "stale.xml",
    "blocked.xml",
    "dupes.xml",
    "wire_source.xml",
    "wire_syndicated.xml",
}

_WEEKDAYS = ("Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun")
_MONTHS = ("Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec")

_PLACEHOLDER_RE = re.compile(r"\{\{(RFC822|ISO):([+-]?\d+(?:\.\d+)?)(h|d)\}\}")


def _rfc822(dt: datetime) -> str:
    # Built manually rather than via strftime("%a, %d %b") — %a/%b are
    # locale-dependent and RFC822 requires English weekday/month names.
    return (
        f"{_WEEKDAYS[dt.weekday()]}, {dt.day:02d} {_MONTHS[dt.month - 1]} {dt.year} "
        f"{dt.hour:02d}:{dt.minute:02d}:{dt.second:02d} +0000"
    )


def _iso8601(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def _render_placeholder(match: "re.Match[str]") -> str:
    kind, amount, unit = match.group(1), float(match.group(2)), match.group(3)
    hours = amount * 24 if unit == "d" else amount
    dt = datetime.now(timezone.utc) + timedelta(hours=hours)
    return _rfc822(dt) if kind == "RFC822" else _iso8601(dt)


def _render_fixture(name: str) -> bytes:
    text = (FIXTURES_DIR / name).read_text(encoding="utf-8")
    return _PLACEHOLDER_RE.sub(_render_placeholder, text).encode("utf-8")


def _read_fixture(name: str) -> bytes:
    return (FIXTURES_DIR / name).read_bytes()


def _fixture_bytes(name: str) -> bytes:
    return _render_fixture(name) if name in _TEMPLATED_FIXTURES else _read_fixture(name)


class MockFeedHandler(BaseHTTPRequestHandler):
    server_version = "ArcLoomMockFeedServer/0.1"

    def log_message(self, format, *args):  # noqa: A002 - stdlib signature
        if not self.server.quiet:  # type: ignore[attr-defined]
            super().log_message(format, *args)

    def _send_xml(self, body: bytes, status: int = 200, extra_headers: dict | None = None) -> None:
        self.send_response(status)
        self.send_header("Content-Type", "application/rss+xml; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        for key, value in (extra_headers or {}).items():
            self.send_header(key, value)
        self.end_headers()
        self.wfile.write(body)

    def _send_status_only(self, status: int) -> None:
        body = f"{status}".encode("ascii")
        self.send_response(status)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_text(self, body: bytes, status: int = 200) -> None:
        self.send_response(status)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    # FR-201: conditional GET via If-None-Match / ETag.
    def _handle_etag(self) -> None:
        if self.headers.get("If-None-Match") == ETAG_VALUE:
            self.send_response(304)
            self.send_header("ETag", ETAG_VALUE)
            self.end_headers()
            return
        self._send_xml(_render_fixture("etag.xml"), extra_headers={"ETag": ETAG_VALUE})

    # FR-201: conditional GET via If-Modified-Since / Last-Modified.
    def _handle_lastmod(self) -> None:
        if self.headers.get("If-Modified-Since") == LAST_MODIFIED_VALUE:
            self.send_response(304)
            self.send_header("Last-Modified", LAST_MODIFIED_VALUE)
            self.end_headers()
            return
        self._send_xml(_render_fixture("lastmod.xml"), extra_headers={"Last-Modified": LAST_MODIFIED_VALUE})

    def do_GET(self) -> None:  # noqa: N802 - stdlib method name
        path = self.path

        if path in _STATIC_XML_ROUTES:
            self._send_xml(_fixture_bytes(_STATIC_XML_ROUTES[path]))
        elif path == "/etag.xml":
            self._handle_etag()
        elif path == "/lastmod.xml":
            self._handle_lastmod()
        elif path == "/403":
            self._send_status_only(403)
        elif path == "/404":
            self._send_status_only(404)
        elif path == "/slow":
            time.sleep(3)
            self._send_xml(_render_fixture("normal.xml"))
        elif path == "/robots.txt":
            self._send_text(_read_fixture("robots.txt"))
        else:
            self._send_status_only(404)


def serve(port: int, quiet: bool) -> None:
    server = ThreadingHTTPServer(("127.0.0.1", port), MockFeedHandler)
    server.quiet = quiet  # type: ignore[attr-defined]
    if not quiet:
        print(f"[mock_feed_server] serving {FIXTURES_DIR} on http://127.0.0.1:{port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


def main() -> int:
    parser = argparse.ArgumentParser(description="Serve static RSS/Atom fixtures for ingestion testing")
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument("--quiet", action="store_true", help="Suppress per-request access logging")
    args = parser.parse_args()
    serve(args.port, args.quiet)
    return 0


if __name__ == "__main__":
    sys.exit(main())
