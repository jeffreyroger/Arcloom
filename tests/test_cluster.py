import sqlite3
import struct
from datetime import datetime, timedelta, timezone

import numpy as np

import config
from pipeline.cluster import assign_pending


def _make_conn():
    conn = sqlite3.connect(":memory:")
    conn.execute(
        """
        CREATE TABLE source (
            id     INTEGER PRIMARY KEY,
            weight REAL NOT NULL DEFAULT 1.0
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE article (
            id           INTEGER PRIMARY KEY,
            source_id    INTEGER NOT NULL,
            title        TEXT NOT NULL,
            published_at TEXT NOT NULL,
            embedding    BLOB,
            embed_model  TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE cluster (
            id               INTEGER PRIMARY KEY,
            canonical_title  TEXT NOT NULL,
            centroid         BLOB,
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


def _vec(*components) -> bytes:
    arr = np.array(components, dtype=np.float32)
    norm = np.linalg.norm(arr)
    if norm > 0:
        arr = arr / norm
    return arr.tobytes()


def _insert_source(conn, weight=1.0):
    return conn.execute("INSERT INTO source (weight) VALUES (?)", (weight,)).lastrowid


def _insert_article(conn, source_id, title, published_at, embedding):
    cur = conn.execute(
        "INSERT INTO article (source_id, title, published_at, embedding, embed_model) VALUES (?, ?, ?, ?, ?)",
        (source_id, title, _iso(published_at), embedding, config.EMBED_MODEL_NAME),
    )
    conn.commit()
    return cur.lastrowid


def _read_centroid(conn, cluster_id):
    blob = conn.execute("SELECT centroid FROM cluster WHERE id = ?", (cluster_id,)).fetchone()[0]
    return np.frombuffer(blob, dtype=np.float32)


# FR-504: best match above TAU_EVENT joins, centroid becomes the running
# mean of member embeddings.
def test_joining_updates_centroid_correctly():
    conn = _make_conn()
    now = datetime.now(timezone.utc)
    src = _insert_source(conn)

    v1 = _vec(1.0, 0.0, 0.0)
    v2 = _vec(0.99, 0.14, 0.0)  # cosine sim with v1 well above TAU_EVENT
    _insert_article(conn, src, "First report", now, v1)
    _insert_article(conn, src, "Second report", now + timedelta(minutes=5), v2)

    assign_pending(conn)

    cluster_id = conn.execute("SELECT cluster_id FROM article_cluster LIMIT 1").fetchone()[0]
    member_count = conn.execute("SELECT member_count FROM cluster WHERE id = ?", (cluster_id,)).fetchone()[0]
    assert member_count == 2

    expected = (np.frombuffer(v1, dtype=np.float32) + np.frombuffer(v2, dtype=np.float32)) / 2
    actual = _read_centroid(conn, cluster_id)
    np.testing.assert_allclose(actual, expected, atol=1e-6)


# FR-504: below TAU_EVENT seeds a new cluster instead of joining.
def test_below_threshold_seeds_new_cluster():
    conn = _make_conn()
    now = datetime.now(timezone.utc)
    src = _insert_source(conn)

    v1 = _vec(1.0, 0.0, 0.0)
    v2 = _vec(0.0, 1.0, 0.0)  # orthogonal -- far below TAU_EVENT
    _insert_article(conn, src, "First report", now, v1)
    _insert_article(conn, src, "Unrelated report", now + timedelta(minutes=5), v2)

    assign_pending(conn)

    cluster_ids = {row[0] for row in conn.execute("SELECT cluster_id FROM article_cluster").fetchall()}
    assert len(cluster_ids) == 2


# FR-503: only clusters active within ACTIVE_WINDOW_H of the new article's
# published_at are candidates -- an old, out-of-window cluster must not
# absorb a new article even with a near-identical embedding.
def test_out_of_window_clusters_are_not_candidates():
    conn = _make_conn()
    old_time = datetime.now(timezone.utc) - timedelta(hours=200)
    src = _insert_source(conn)

    v1 = _vec(1.0, 0.0, 0.0)
    v2 = _vec(1.0, 0.0, 0.0)  # identical embedding
    _insert_article(conn, src, "Old report", old_time, v1)
    _insert_article(
        conn, src, "New report",
        old_time + timedelta(hours=config.ACTIVE_WINDOW_H + 1),
        v2,
    )

    assign_pending(conn)

    cluster_ids = {row[0] for row in conn.execute("SELECT cluster_id FROM article_cluster").fetchall()}
    assert len(cluster_ids) == 2


# FR-506: canonical title comes from the highest-weight source's member,
# regardless of arrival order.
def test_canonical_title_respects_source_weight():
    conn = _make_conn()
    now = datetime.now(timezone.utc)
    low = _insert_source(conn, weight=1.0)
    high = _insert_source(conn, weight=5.0)

    v1 = _vec(1.0, 0.0, 0.0)
    v2 = _vec(0.99, 0.14, 0.0)
    _insert_article(conn, low, "Low-weight headline", now, v1)
    _insert_article(conn, high, "High-weight headline", now + timedelta(minutes=5), v2)

    assign_pending(conn)

    cluster_id = conn.execute("SELECT cluster_id FROM article_cluster LIMIT 1").fetchone()[0]
    title = conn.execute("SELECT canonical_title FROM cluster WHERE id = ?", (cluster_id,)).fetchone()[0]
    assert title == "High-weight headline"


# FR-501/FR-502: re-running must not reprocess already-assigned articles or
# change existing assignments.
def test_rerunning_is_idempotent():
    conn = _make_conn()
    now = datetime.now(timezone.utc)
    src = _insert_source(conn)

    v1 = _vec(1.0, 0.0, 0.0)
    v2 = _vec(0.99, 0.14, 0.0)
    _insert_article(conn, src, "First report", now, v1)
    _insert_article(conn, src, "Second report", now + timedelta(minutes=5), v2)

    assign_pending(conn)
    before = conn.execute("SELECT article_id, cluster_id FROM article_cluster ORDER BY article_id").fetchall()
    before_centroid = _read_centroid(
        conn, conn.execute("SELECT cluster_id FROM article_cluster LIMIT 1").fetchone()[0]
    )

    stats = assign_pending(conn)

    after = conn.execute("SELECT article_id, cluster_id FROM article_cluster ORDER BY article_id").fetchall()
    after_centroid = _read_centroid(
        conn, conn.execute("SELECT cluster_id FROM article_cluster LIMIT 1").fetchone()[0]
    )

    assert stats["pending"] == 0
    assert before == after
    np.testing.assert_array_equal(before_centroid, after_centroid)


# FR-507: distinct_sources counts distinct sources, not member articles.
def test_distinct_sources_counts_sources_not_articles():
    conn = _make_conn()
    now = datetime.now(timezone.utc)
    src = _insert_source(conn)

    v1 = _vec(1.0, 0.0, 0.0)
    v2 = _vec(0.99, 0.14, 0.0)
    v3 = _vec(0.98, 0.19, 0.0)
    _insert_article(conn, src, "First report", now, v1)
    _insert_article(conn, src, "Second report", now + timedelta(minutes=5), v2)
    _insert_article(conn, src, "Third report", now + timedelta(minutes=10), v3)

    assign_pending(conn)

    cluster_id = conn.execute("SELECT cluster_id FROM article_cluster LIMIT 1").fetchone()[0]
    row = conn.execute(
        "SELECT member_count, distinct_sources FROM cluster WHERE id = ?", (cluster_id,)
    ).fetchone()
    assert row[0] == 3
    assert row[1] == 1
