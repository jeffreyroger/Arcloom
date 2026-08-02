"""FR-301/FR-302 validation against tools/mock_feed_server.py.

Two claims, checked in order because the second is only meaningful once the
first is confirmed: (1) /dupes.xml serves the same 5 articles as
/normal.xml under different utm_*-tagged URLs, and canonicalization
(FR-207/FR-208) alone dedupes those -- simhash never even sees them, since
they never reach the article table as separate rows; (2) a genuinely
different URL with a lightly reworded title (/wire_source.xml vs
/wire_syndicated.xml -- no shared host, path, or tracking params, so
canonicalization cannot and must not merge them) is exactly the case
canonicalization can't catch and simhash's near-duplicate bypass can.
"""

import asyncio
import sqlite3
import threading
from http.server import ThreadingHTTPServer

import pytest

import config
import db
from pipeline.ingest import ingest_once, sync_sources
from pipeline.simhash import apply_bypass
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


def _make_conn():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    db.init_schema(conn)
    return conn


def _feed(name, url):
    return {"name": name, "url": url, "packs": ["general-tech"], "lang": "en", "weight": 1.0, "enabled": True}


def _enabled_sources(conn):
    return [dict(row) for row in conn.execute("SELECT id, feed_url, etag, last_modified FROM source WHERE enabled=1")]


def test_dupes_xml_dedupes_by_canonical_url_before_simhash_is_involved(base_url):
    conn = _make_conn()
    sync_sources(conn, [_feed("Normal", f"{base_url}/normal.xml")])
    asyncio.run(ingest_once(conn, _enabled_sources(conn)))
    conn.commit()

    sync_sources(conn, [_feed("Normal", f"{base_url}/normal.xml"), _feed("Dupes", f"{base_url}/dupes.xml")])
    counts, _ = asyncio.run(ingest_once(conn, _enabled_sources(conn)))
    conn.commit()

    # Normal's 20 entries + Dupes' 5 entries, all already-known canonical
    # URLs -- FR-207/FR-208 alone accounts for every one of them.
    assert counts["articles_inserted"] == 0
    assert counts["duplicates_skipped"] == 25


def test_simhash_catches_near_duplicate_canonicalization_cannot(base_url):
    conn = _make_conn()
    sync_sources(
        conn,
        [
            _feed("Wire Source", f"{base_url}/wire_source.xml"),
            _feed("Wire Syndicated", f"{base_url}/wire_syndicated.xml"),
        ],
    )
    counts, _ = asyncio.run(ingest_once(conn, _enabled_sources(conn)))
    conn.commit()

    # Genuinely distinct URLs (different host, different path, no shared
    # tracking params) -- canonicalization must not, and does not, merge
    # these into one row.
    assert counts["articles_inserted"] == 2
    rows = conn.execute("SELECT id, canonical_url, title FROM article ORDER BY id").fetchall()
    assert len(rows) == 2
    assert rows[0]["canonical_url"] != rows[1]["canonical_url"]
    assert rows[0]["title"] != rows[1]["title"]  # near-identical, not identical

    # Simulate the wire source article already having been embedded by an
    # earlier pipeline.embed pass -- this is the scenario apply_bypass
    # exists for.
    conn.execute(
        "UPDATE article SET embedding=?, embed_model=? WHERE id=?",
        (b"wire-source-embedding-bytes", config.EMBED_MODEL_NAME, rows[0]["id"]),
    )
    conn.commit()

    stats = apply_bypass(conn)

    assert stats["bypassed"] == 1
    syndicated = conn.execute("SELECT embedding FROM article WHERE id=?", (rows[1]["id"],)).fetchone()
    assert syndicated["embedding"] == b"wire-source-embedding-bytes"
