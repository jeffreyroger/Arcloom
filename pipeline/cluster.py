"""FR-500 through FR-507: Level 1 event clustering via incremental centroid
matching.

Clustering is single-pass and incremental (FR-502): only articles missing an
article_cluster row are processed, in ascending published_at order, and a
processed article is never revisited. This makes a backfill over the whole
corpus and an ordinary incremental run (a handful of new articles per
15-minute pipeline invocation) the same code path.

Candidate clusters are restricted to those active within ACTIVE_WINDOW_H of
the article being processed (FR-503). "Active" is measured against the
article's own published_at rather than wall-clock time: processing runs in
published_at order, so this is equivalent to wall-clock recency for a live
incremental run, and it is what makes a backfill over old historical
articles behave identically to having processed them as they originally
arrived (idempotent regardless of when the backfill itself is run).

The centroid is stored as the literal running mean of member embeddings
(FR-504) -- it is not renormalized to a unit vector. Similarity is computed
as cosine similarity (dot product over the product of norms) rather than a
plain dot product, so a non-unit centroid still compares correctly against
the L2-normalized article embeddings.
"""

import logging
import sqlite3
from datetime import datetime, timedelta, timezone

import numpy as np

import config

_log = logging.getLogger("arcloom.cluster")

_ISO_FMT = "%Y-%m-%dT%H:%M:%SZ"


def _iso(dt: datetime) -> str:
    return dt.strftime(_ISO_FMT)


def _parse_iso(s: str) -> datetime:
    return datetime.strptime(s, _ISO_FMT).replace(tzinfo=timezone.utc)


def _to_vector(blob: bytes) -> np.ndarray:
    return np.frombuffer(blob, dtype=np.float32)


def _cosine_similarity(a: np.ndarray, b: np.ndarray) -> float:
    denom = float(np.linalg.norm(a) * np.linalg.norm(b))
    if denom == 0.0:
        return 0.0
    return float(np.dot(a, b) / denom)


# FR-503: candidate clusters are those with activity no older than
# cutoff_iso (article.published_at - ACTIVE_WINDOW_H).
def _active_clusters(conn: sqlite3.Connection, cutoff_iso: str):
    rows = conn.execute(
        "SELECT id, centroid, member_count FROM cluster WHERE updated_at >= ?",
        (cutoff_iso,),
    ).fetchall()
    return [(row[0], _to_vector(row[1]), row[2]) for row in rows]


# FR-504: best-matching centroid above TAU_EVENT, else None. TAU_EVENT is
# fixed at 0.75 per config.py -- not eye-tuned here or anywhere in this
# module (week 3's job, per the gold-set sweep in FR-505).
def _best_match(vector: np.ndarray, candidates: list) -> tuple[int, float] | None:
    best_id, best_score = None, -1.0
    for cluster_id, centroid, _ in candidates:
        score = _cosine_similarity(vector, centroid)
        if score > best_score:
            best_id, best_score = cluster_id, score
    if best_id is not None and best_score > config.TAU_EVENT:
        return best_id, best_score
    return None


# FR-506: canonical title is the title of the member from the
# highest-weight source, ties broken by earliest published_at. Recomputed
# from scratch on every membership change rather than incrementally
# compared against the incoming article, since a later-arriving member from
# a still-higher-weight source can always displace the current title.
def _recompute_canonical_title(conn: sqlite3.Connection, cluster_id: int) -> str:
    row = conn.execute(
        """
        SELECT a.title FROM article a
        JOIN article_cluster ac ON ac.article_id = a.id
        JOIN source s ON s.id = a.source_id
        WHERE ac.cluster_id = ?
        ORDER BY s.weight DESC, a.published_at ASC
        LIMIT 1
        """,
        (cluster_id,),
    ).fetchone()
    return row[0]


# FR-507: distinct-source count, the coverage signal shown to Readers --
# counts distinct source_id among members, not member articles.
def _distinct_sources(conn: sqlite3.Connection, cluster_id: int) -> int:
    row = conn.execute(
        """
        SELECT COUNT(DISTINCT a.source_id) FROM article a
        JOIN article_cluster ac ON ac.article_id = a.id
        WHERE ac.cluster_id = ?
        """,
        (cluster_id,),
    ).fetchone()
    return row[0]


# FR-504: join article_id to an existing cluster. The centroid becomes the
# running mean of member embeddings; canonical_title and distinct_sources
# are recomputed afterward since the new member can change either.
def _join_cluster(
    conn: sqlite3.Connection,
    cluster_id: int,
    article_id: int,
    vector: np.ndarray,
    old_centroid: np.ndarray,
    old_member_count: int,
    published_at: str,
) -> None:
    new_member_count = old_member_count + 1
    new_centroid = (old_centroid * old_member_count + vector) / new_member_count

    conn.execute(
        "INSERT INTO article_cluster (article_id, cluster_id) VALUES (?, ?)",
        (article_id, cluster_id),
    )
    conn.execute(
        "UPDATE cluster SET centroid = ?, member_count = ?, updated_at = ? WHERE id = ?",
        (new_centroid.astype(np.float32).tobytes(), new_member_count, published_at, cluster_id),
    )
    conn.execute(
        "UPDATE cluster SET canonical_title = ?, distinct_sources = ? WHERE id = ?",
        (
            _recompute_canonical_title(conn, cluster_id),
            _distinct_sources(conn, cluster_id),
            cluster_id,
        ),
    )


# FR-501/FR-504: seed a new single-member cluster for article_id.
def _seed_cluster(
    conn: sqlite3.Connection, article_id: int, title: str, vector: np.ndarray, published_at: str
) -> int:
    cluster_id = conn.execute(
        """
        INSERT INTO cluster (canonical_title, centroid, created_at, updated_at, member_count, distinct_sources)
        VALUES (?, ?, ?, ?, 1, 1)
        """,
        (title, vector.astype(np.float32).tobytes(), published_at, published_at),
    ).lastrowid
    conn.execute(
        "INSERT INTO article_cluster (article_id, cluster_id) VALUES (?, ?)",
        (article_id, cluster_id),
    )
    return cluster_id


# FR-501 through FR-507: assign every embedded, unclustered article to
# exactly one cluster. Only articles with a current-model embedding and no
# existing article_cluster row are selected (FR-501/FR-502) -- an empty
# pending set is the expected, common case for an incremental run.
def assign_pending(conn: sqlite3.Connection) -> dict:
    rows = conn.execute(
        """
        SELECT a.id, a.title, a.embedding, a.published_at
        FROM article a
        LEFT JOIN article_cluster ac ON ac.article_id = a.id
        WHERE a.embedding IS NOT NULL AND a.embed_model = ? AND ac.article_id IS NULL
        ORDER BY a.published_at ASC, a.id ASC
        """,
        (config.EMBED_MODEL_NAME,),
    ).fetchall()

    stats = {"pending": len(rows), "joined": 0, "seeded": 0}
    if not rows:
        _log.info("cluster: nothing pending", extra={"stage": "cluster"})
        return stats

    for article_id, title, embedding, published_at in rows:
        vector = _to_vector(embedding)
        cutoff_iso = _iso(_parse_iso(published_at) - timedelta(hours=config.ACTIVE_WINDOW_H))
        candidates = _active_clusters(conn, cutoff_iso)
        match = _best_match(vector, candidates)

        if match is not None:
            cluster_id, _score = match
            old_centroid, old_member_count = next(
                (centroid, member_count) for cid, centroid, member_count in candidates if cid == cluster_id
            )
            _join_cluster(conn, cluster_id, article_id, vector, old_centroid, old_member_count, published_at)
            stats["joined"] += 1
        else:
            _seed_cluster(conn, article_id, title, vector, published_at)
            stats["seeded"] += 1
        conn.commit()

    _log.info(
        f"cluster: pending={stats['pending']} joined={stats['joined']} seeded={stats['seeded']}",
        extra={"stage": "cluster"},
    )
    return stats


# Manual backfill entry point (`python -m pipeline.cluster [--db PATH]`),
# and the report requested for week 2's singleton-percentage measurement:
# total clusters, size distribution, singleton percentage, and the ten
# largest clusters by member_count with canonical title and distinct-source
# count.
def main() -> int:
    import argparse
    from pathlib import Path

    import db

    parser = argparse.ArgumentParser(description="Backfill event clustering for embedded articles")
    parser.add_argument("--db", type=Path, default=None, help="Alternate database path (default: arcloom.db)")
    args = parser.parse_args()

    db_path = args.db or db.DB_PATH
    conn = db.get_connection(db_path)
    conn.row_factory = None
    db.init_schema(conn)  # creates cluster/article_cluster if this DB predates week 2

    stats = assign_pending(conn)
    print(f"pending={stats['pending']} joined={stats['joined']} seeded={stats['seeded']}")

    rows = conn.execute("SELECT member_count FROM cluster").fetchall()
    sizes = sorted((row[0] for row in rows), reverse=True)
    total_clusters = len(sizes)
    singleton_count = sum(1 for s in sizes if s == 1)
    singleton_pct = (singleton_count / total_clusters * 100) if total_clusters else 0.0

    print(f"\ntotal_clusters={total_clusters}")
    print(f"singleton_pct={singleton_pct:.1f}% ({singleton_count}/{total_clusters})")

    from collections import Counter

    dist = Counter(sizes)
    print("size_distribution:")
    for size in sorted(dist, reverse=True):
        print(f"  size={size}: {dist[size]} cluster(s)")

    print("\ntop_10_clusters:")
    top_rows = conn.execute(
        "SELECT canonical_title, member_count, distinct_sources FROM cluster ORDER BY member_count DESC LIMIT 10"
    ).fetchall()
    for title, member_count, distinct_sources in top_rows:
        print(f"  members={member_count} distinct_sources={distinct_sources} title={title!r}")

    conn.close()
    return 0


if __name__ == "__main__":
    import sys

    sys.exit(main())
