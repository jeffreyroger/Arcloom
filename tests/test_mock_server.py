"""Regression test for tools/mock_feed_server.py's fixture set.

Asserts every endpoint behaves as documented in the server's docstring, so
the fixtures backing pipeline/ingest.py's tests can't silently rot.
"""

import threading
import time

import feedparser
import httpx
import pytest
from http.server import ThreadingHTTPServer

from pipeline.normalize import canonicalize_url
from tools.mock_feed_server import MockFeedHandler


@pytest.fixture(scope="module")
def base_url():
    server = ThreadingHTTPServer(("127.0.0.1", 0), MockFeedHandler)
    server.quiet = True  # type: ignore[attr-defined]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    port = server.server_address[1]
    try:
        yield f"http://127.0.0.1:{port}"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def test_normal_xml_is_valid_rss_with_20_recent_entries(base_url):
    resp = httpx.get(f"{base_url}/normal.xml")
    assert resp.status_code == 200
    parsed = feedparser.parse(resp.content)
    assert not parsed.bozo
    assert len(parsed.entries) == 20

    from datetime import datetime, timezone

    now = datetime.now(timezone.utc)
    for entry in parsed.entries:
        published = datetime(*entry.published_parsed[:6], tzinfo=timezone.utc)
        age_h = (now - published).total_seconds() / 3600
        assert 0 <= age_h < 24, f"{entry.link} published {age_h:.1f}h ago, not recent"


def test_atom_xml_is_valid_atom_not_rss(base_url):
    resp = httpx.get(f"{base_url}/atom.xml")
    assert resp.status_code == 200
    parsed = feedparser.parse(resp.content)
    assert not parsed.bozo
    assert parsed.version.startswith("atom")
    assert len(parsed.entries) == 5


def test_etag_xml_returns_etag_and_304_on_matching_if_none_match(base_url):
    first = httpx.get(f"{base_url}/etag.xml")
    assert first.status_code == 200
    etag = first.headers.get("ETag")
    assert etag

    second = httpx.get(f"{base_url}/etag.xml", headers={"If-None-Match": etag})
    assert second.status_code == 304


def test_lastmod_xml_returns_last_modified_and_304_on_matching_if_modified_since(base_url):
    first = httpx.get(f"{base_url}/lastmod.xml")
    assert first.status_code == 200
    last_modified = first.headers.get("Last-Modified")
    assert last_modified

    second = httpx.get(f"{base_url}/lastmod.xml", headers={"If-Modified-Since": last_modified})
    assert second.status_code == 304


def test_fullbody_xml_description_is_5000_chars(base_url):
    resp = httpx.get(f"{base_url}/fullbody.xml")
    assert resp.status_code == 200
    parsed = feedparser.parse(resp.content)
    assert len(parsed.entries) == 1
    assert len(parsed.entries[0].description) == 5000


def test_empty_xml_is_valid_with_zero_entries(base_url):
    resp = httpx.get(f"{base_url}/empty.xml")
    assert resp.status_code == 200
    parsed = feedparser.parse(resp.content)
    assert not parsed.bozo
    assert len(parsed.entries) == 0


def test_stale_xml_newest_entry_is_400_days_old(base_url):
    resp = httpx.get(f"{base_url}/stale.xml")
    assert resp.status_code == 200
    parsed = feedparser.parse(resp.content)
    assert len(parsed.entries) == 1
    from datetime import datetime, timezone

    published = datetime(*parsed.entries[0].published_parsed[:6], tzinfo=timezone.utc)
    age_days = (datetime.now(timezone.utc) - published).days
    assert age_days >= 390


def test_malformed_xml_triggers_feedparser_bozo(base_url):
    resp = httpx.get(f"{base_url}/malformed.xml")
    assert resp.status_code == 200
    parsed = feedparser.parse(resp.content)
    assert parsed.bozo


def test_403_returns_forbidden(base_url):
    resp = httpx.get(f"{base_url}/403")
    assert resp.status_code == 403


def test_404_returns_not_found(base_url):
    resp = httpx.get(f"{base_url}/404")
    assert resp.status_code == 404


def test_slow_sleeps_before_responding(base_url):
    start = time.monotonic()
    resp = httpx.get(f"{base_url}/slow", timeout=10.0)
    elapsed = time.monotonic() - start
    assert resp.status_code == 200
    assert elapsed >= 3.0


def test_robots_txt_disallows_blocked_xml(base_url):
    resp = httpx.get(f"{base_url}/robots.txt")
    assert resp.status_code == 200
    assert "Disallow: /blocked.xml" in resp.text


def test_blocked_xml_is_a_valid_feed_when_fetched_directly(base_url):
    resp = httpx.get(f"{base_url}/blocked.xml")
    assert resp.status_code == 200
    parsed = feedparser.parse(resp.content)
    assert not parsed.bozo
    assert len(parsed.entries) == 3


def test_dupes_xml_canonicalizes_to_normal_xmls_urls(base_url):
    dupes_resp = httpx.get(f"{base_url}/dupes.xml")
    normal_resp = httpx.get(f"{base_url}/normal.xml")
    dupes = feedparser.parse(dupes_resp.content)
    normal = feedparser.parse(normal_resp.content)

    assert len(dupes.entries) == 5
    for i, entry in enumerate(dupes.entries):
        assert "utm_" in entry.link
        assert canonicalize_url(entry.link) == canonicalize_url(normal.entries[i].link)
