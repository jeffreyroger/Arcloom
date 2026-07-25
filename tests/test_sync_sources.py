import sqlite3

import db
from pipeline.ingest import sync_sources


def _make_conn():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    db.init_schema(conn)
    return conn


def _feed(name="Feed A", url="https://example.com/feed.xml", enabled=True):
    return {"name": name, "url": url, "packs": ["ai"], "lang": "en", "weight": 1.0, "enabled": enabled}


def test_feed_removed_from_file_becomes_disabled():
    conn = _make_conn()
    sync_sources(conn, [_feed()])

    sync_sources(conn, [])  # feed no longer in feeds.yaml

    row = conn.execute("SELECT status, enabled FROM source WHERE feed_url=?", (_feed()["url"],)).fetchone()
    assert row["status"] == "disabled"
    assert row["enabled"] == 0


def test_disabled_feed_readded_to_file_becomes_ok_again():
    conn = _make_conn()
    sync_sources(conn, [_feed()])
    sync_sources(conn, [])  # removed

    sync_sources(conn, [_feed()])  # re-added

    row = conn.execute("SELECT status, enabled FROM source WHERE feed_url=?", (_feed()["url"],)).fetchone()
    assert row["status"] == "ok"
    assert row["enabled"] == 1


def test_degraded_feed_still_in_file_keeps_degraded_status():
    conn = _make_conn()
    sync_sources(conn, [_feed()])
    conn.execute("UPDATE source SET status='degraded', fail_streak=25 WHERE feed_url=?", (_feed()["url"],))
    conn.commit()

    sync_sources(conn, [_feed()])  # still present in feeds.yaml

    row = conn.execute("SELECT status FROM source WHERE feed_url=?", (_feed()["url"],)).fetchone()
    assert row["status"] == "degraded"


def test_disabling_a_feed_preserves_its_articles():
    conn = _make_conn()
    sync_sources(conn, [_feed()])
    source_id = conn.execute("SELECT id FROM source WHERE feed_url=?", (_feed()["url"],)).fetchone()["id"]
    conn.execute(
        """
        INSERT INTO article (source_id, canonical_url, title, published_at, fetched_at, timestamp_inferred)
        VALUES (?, 'https://example.com/article-1', 'Title', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z', 0)
        """,
        (source_id,),
    )
    conn.commit()

    sync_sources(conn, [])  # feed removed, source disabled

    article = conn.execute("SELECT id FROM article WHERE source_id=?", (source_id,)).fetchone()
    assert article is not None
