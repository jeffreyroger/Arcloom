"""Week 2 render deliverable (02-IMPLEMENTATION-PLAN.md): Jinja2 -> a single
static index.html. No FR-5xx covers rendering directly, so this module's
obligations trace to two other sources instead: Design Principle 4 ("Link
out, always" -- 00-PROJECT-BRIEF.md #5) and the legal posture ("the outbound
link is the most prominent element on every card" -- 00-PROJECT-BRIEF.md
#8). Every member article's own link is rendered, not just one
representative link per cluster -- the summary/title is what helps a reader
decide whether to click, never a replacement for the click itself (G9 also
means there is nothing else safe to show).

G1 (no web framework before week 11) is why this is a template rendered to
a static file rather than a served view.
"""

import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

from jinja2 import Environment, FileSystemLoader

import config

_TEMPLATE_DIR = Path(__file__).parent.parent / "templates"
_DEFAULT_OUTPUT = Path(__file__).parent.parent / "index.html"

_env = Environment(loader=FileSystemLoader(_TEMPLATE_DIR), autoescape=True)


def _iso(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


# Clusters active in the last ACTIVE_WINDOW_H hours (reusing the existing
# config constant rather than introducing a new one, per G5/CLAUDE.md),
# ordered by recency (most recently active cluster first).
def _active_clusters(conn: sqlite3.Connection, cutoff_iso: str) -> list[dict]:
    cluster_rows = conn.execute(
        """
        SELECT id, canonical_title, updated_at, distinct_sources
        FROM cluster
        WHERE updated_at >= ?
        ORDER BY updated_at DESC
        """,
        (cutoff_iso,),
    ).fetchall()

    clusters = []
    for cluster_id, canonical_title, updated_at, distinct_sources in cluster_rows:
        article_rows = conn.execute(
            """
            SELECT a.title, a.canonical_url, s.name
            FROM article a
            JOIN article_cluster ac ON ac.article_id = a.id
            JOIN source s ON s.id = a.source_id
            WHERE ac.cluster_id = ?
            ORDER BY a.published_at DESC
            """,
            (cluster_id,),
        ).fetchall()
        clusters.append(
            {
                "canonical_title": canonical_title,
                "updated_at": updated_at,
                "distinct_sources": distinct_sources,
                "articles": [
                    {"title": title, "canonical_url": canonical_url, "source_name": source_name}
                    for title, canonical_url, source_name in article_rows
                ],
            }
        )
    return clusters


# Renders the last ACTIVE_WINDOW_H hours of event clusters to a single
# static index.html. `now` is an injection point for deterministic tests;
# production callers always take the default (real wall-clock time).
def render_index(
    conn: sqlite3.Connection, output_path: Path | None = None, now: datetime | None = None
) -> dict:
    now = now or datetime.now(timezone.utc)
    output_path = output_path or _DEFAULT_OUTPUT
    cutoff_iso = _iso(now - timedelta(hours=config.ACTIVE_WINDOW_H))

    clusters = _active_clusters(conn, cutoff_iso)
    article_count = sum(len(c["articles"]) for c in clusters)

    template = _env.get_template("index.html.j2")
    html = template.render(
        clusters=clusters,
        window_hours=config.ACTIVE_WINDOW_H,
        generated_at=_iso(now),
    )
    output_path.write_text(html, encoding="utf-8")

    return {"cluster_count": len(clusters), "article_count": article_count}


# Manual entry point (`python -m pipeline.render [--db PATH] [--out PATH]`).
def main() -> int:
    import argparse

    import db

    parser = argparse.ArgumentParser(description="Render the static event-cluster feed page")
    parser.add_argument("--db", type=Path, default=None, help="Alternate database path (default: arcloom.db)")
    parser.add_argument("--out", type=Path, default=None, help="Output HTML path (default: index.html)")
    args = parser.parse_args()

    conn = db.get_connection(args.db or db.DB_PATH)
    stats = render_index(conn, output_path=args.out)
    conn.close()

    print(f"clusters={stats['cluster_count']} articles={stats['article_count']}")
    return 0


if __name__ == "__main__":
    import sys

    sys.exit(main())
