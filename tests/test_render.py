import sqlite3
from datetime import datetime, timedelta, timezone

import config
from pipeline.render import render_index


def _make_conn():
    conn = sqlite3.connect(":memory:")
    conn.execute(
        """
        CREATE TABLE source (
            id   INTEGER PRIMARY KEY,
            name TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE article (
            id            INTEGER PRIMARY KEY,
            source_id     INTEGER NOT NULL,
            canonical_url TEXT NOT NULL,
            title         TEXT NOT NULL,
            published_at  TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE cluster (
            id               INTEGER PRIMARY KEY,
            canonical_title  TEXT NOT NULL,
            created_at       TEXT NOT NULL,
            updated_at       TEXT NOT NULL,
            member_count     INTEGER NOT NULL DEFAULT 0,
            distinct_sources INTEGER NOT NULL DEFAULT 0
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE article_cluster (
            article_id INTEGER NOT NULL,
            cluster_id INTEGER NOT NULL,
            PRIMARY KEY (article_id, cluster_id)
        )
        """
    )
    return conn


def _iso(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def _insert_source(conn, name):
    return conn.execute("INSERT INTO source (name) VALUES (?)", (name,)).lastrowid


def _insert_article(conn, source_id, url, title, published_at):
    return conn.execute(
        "INSERT INTO article (source_id, canonical_url, title, published_at) VALUES (?, ?, ?, ?)",
        (source_id, url, title, _iso(published_at)),
    ).lastrowid


def _insert_cluster(conn, title, created_at, updated_at, member_count, distinct_sources):
    return conn.execute(
        """
        INSERT INTO cluster (canonical_title, created_at, updated_at, member_count, distinct_sources)
        VALUES (?, ?, ?, ?, ?)
        """,
        (title, _iso(created_at), _iso(updated_at), member_count, distinct_sources),
    ).lastrowid


def _link(conn, article_id, cluster_id):
    conn.execute(
        "INSERT INTO article_cluster (article_id, cluster_id) VALUES (?, ?)", (article_id, cluster_id)
    )


# Last-72h window, ordered by recency.


def test_cluster_within_window_appears_in_output(tmp_path):
    conn = _make_conn()
    now = datetime.now(timezone.utc)
    src = _insert_source(conn, "Wire Service")
    cluster_id = _insert_cluster(conn, "Recent event", now, now, 1, 1)
    article_id = _insert_article(conn, src, "https://example.com/a", "Recent event", now)
    _link(conn, article_id, cluster_id)
    conn.commit()

    out = tmp_path / "index.html"
    render_index(conn, output_path=out, now=now)

    html = out.read_text(encoding="utf-8")
    assert "Recent event" in html


def test_cluster_outside_window_is_excluded(tmp_path):
    conn = _make_conn()
    now = datetime.now(timezone.utc)
    old = now - timedelta(hours=config.ACTIVE_WINDOW_H + 1)
    src = _insert_source(conn, "Wire Service")
    cluster_id = _insert_cluster(conn, "Stale event", old, old, 1, 1)
    article_id = _insert_article(conn, src, "https://example.com/old", "Stale event", old)
    _link(conn, article_id, cluster_id)
    conn.commit()

    out = tmp_path / "index.html"
    render_index(conn, output_path=out, now=now)

    html = out.read_text(encoding="utf-8")
    assert "Stale event" not in html


# Principle 4 ("link out, always") and the legal posture: every member
# article's outbound link must be present, not just the canonical one.


def test_every_member_article_link_is_present(tmp_path):
    conn = _make_conn()
    now = datetime.now(timezone.utc)
    src_a = _insert_source(conn, "Outlet A")
    src_b = _insert_source(conn, "Outlet B")
    cluster_id = _insert_cluster(conn, "Shared story", now, now, 2, 2)
    a1 = _insert_article(conn, src_a, "https://a.example.com/story", "Outlet A's headline", now)
    a2 = _insert_article(conn, src_b, "https://b.example.com/story", "Outlet B's headline", now)
    _link(conn, a1, cluster_id)
    _link(conn, a2, cluster_id)
    conn.commit()

    out = tmp_path / "index.html"
    render_index(conn, output_path=out, now=now)

    html = out.read_text(encoding="utf-8")
    assert "https://a.example.com/story" in html
    assert "https://b.example.com/story" in html


def test_distinct_source_count_is_shown(tmp_path):
    conn = _make_conn()
    now = datetime.now(timezone.utc)
    src = _insert_source(conn, "Outlet A")
    cluster_id = _insert_cluster(conn, "Wire story", now, now, 3, 2)
    article_id = _insert_article(conn, src, "https://example.com/wire", "Wire story", now)
    _link(conn, article_id, cluster_id)
    conn.commit()

    out = tmp_path / "index.html"
    render_index(conn, output_path=out, now=now)

    html = out.read_text(encoding="utf-8")
    assert "2" in html  # distinct_sources value rendered somewhere on the card


def test_clusters_ordered_by_recency(tmp_path):
    conn = _make_conn()
    now = datetime.now(timezone.utc)
    older = now - timedelta(hours=10)
    src = _insert_source(conn, "Outlet A")

    older_cluster = _insert_cluster(conn, "Older event", older, older, 1, 1)
    a_old = _insert_article(conn, src, "https://example.com/old-event", "Older event", older)
    _link(conn, a_old, older_cluster)

    newer_cluster = _insert_cluster(conn, "Newer event", now, now, 1, 1)
    a_new = _insert_article(conn, src, "https://example.com/new-event", "Newer event", now)
    _link(conn, a_new, newer_cluster)
    conn.commit()

    out = tmp_path / "index.html"
    render_index(conn, output_path=out, now=now)

    html = out.read_text(encoding="utf-8")
    assert html.index("Newer event") < html.index("Older event")


def test_render_returns_counts(tmp_path):
    conn = _make_conn()
    now = datetime.now(timezone.utc)
    src = _insert_source(conn, "Outlet A")
    cluster_id = _insert_cluster(conn, "Counted event", now, now, 2, 1)
    a1 = _insert_article(conn, src, "https://example.com/1", "Counted event", now)
    a2 = _insert_article(conn, src, "https://example.com/2", "Counted event, cont.", now)
    _link(conn, a1, cluster_id)
    _link(conn, a2, cluster_id)
    conn.commit()

    out = tmp_path / "index.html"
    stats = render_index(conn, output_path=out, now=now)

    assert stats["cluster_count"] == 1
    assert stats["article_count"] == 2
