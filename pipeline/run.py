"""Single pipeline entry point.

Wraps pipeline.ingest: owns the run_log lifecycle, prints per-stage counts
and durations to stdout (NFR-403), and defines the only two conditions that
exit non-zero — config unreadable, database unwritable. An individual feed
failing is captured in stage_counts and never affects the exit code
(NFR-401).

Safely re-runnable after a crash (NFR-402): the run_log start row and the
feeds.yaml -> source sync each commit immediately as they happen, so
evidence of an attempted run survives even if the rest doesn't. The bulk of
a run — article inserts and per-source status updates — lands in one
transaction at the end; a crash before that commit leaves the database
exactly as it was before this run started, so simply re-running is safe.
"""

import argparse
import asyncio
import json
import sqlite3
import sys
import time
from datetime import datetime, timezone

import config
import db
from pipeline.ingest import ingest_once, sync_sources


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# --dry-run: fetches and parses but writes nothing. Reads (never writes) any
# existing source row by feed_url so conditional GET is still exercised.
def _dry_run_sources(conn: sqlite3.Connection, feeds: list[dict]) -> list[dict]:
    urls = [feed["url"] for feed in feeds if feed["enabled"]]
    existing: dict[str, tuple] = {}
    if urls:
        placeholders = ",".join("?" for _ in urls)
        rows = conn.execute(
            f"SELECT feed_url, etag, last_modified FROM source WHERE feed_url IN ({placeholders})",
            urls,
        ).fetchall()
        existing = {row[0]: (row[1], row[2]) for row in rows}

    sources = []
    for feed in feeds:
        if not feed["enabled"]:
            continue
        etag, last_modified = existing.get(feed["url"], (None, None))
        sources.append(
            {"id": feed["name"], "feed_url": feed["url"], "etag": etag, "last_modified": last_modified}
        )
    return sources


async def run(dry_run: bool = False) -> int:
    t_start = time.perf_counter()

    # Total failure #1: config unreadable.
    try:
        feeds = config.load_feeds()
    except Exception as exc:
        print(f"[run] FATAL: could not load feeds.yaml: {exc}", file=sys.stderr)
        return 1
    print(f"[run] load_feeds: {len(feeds)} feeds ({time.perf_counter() - t_start:.2f}s)")

    # Total failure #2: database unwritable.
    run_id = None
    try:
        conn = db.get_connection()
        conn.row_factory = sqlite3.Row
        db.init_schema(conn)

        if not dry_run:
            t_stage = time.perf_counter()
            run_id = conn.execute(
                "INSERT INTO run_log (started_at) VALUES (?)", (_now_iso(),)
            ).lastrowid
            conn.commit()
            sync_sources(conn, feeds)
            print(f"[run] sync_sources: {len(feeds)} feeds synced ({time.perf_counter() - t_stage:.2f}s)")
    except Exception as exc:
        print(f"[run] FATAL: database unwritable: {exc}", file=sys.stderr)
        return 1

    if dry_run:
        sources = _dry_run_sources(conn, feeds)
    else:
        rows = conn.execute(
            "SELECT id, feed_url, etag, last_modified FROM source WHERE enabled = 1"
        ).fetchall()
        sources = [dict(row) for row in rows]

    t_stage = time.perf_counter()
    counts, errors = await ingest_once(conn, sources, dry_run=dry_run)
    fetch_elapsed = time.perf_counter() - t_stage
    print(
        f"[run] ingest: {counts['feeds_attempted']} attempted, "
        f"{counts['status_200']} x200, {counts['status_304']} x304, "
        f"{counts['skipped_robots']} skipped(robots), {counts['failures']} failed "
        f"({fetch_elapsed:.2f}s)"
    )
    print(
        f"[run] articles: {counts['entries_seen']} seen, "
        f"{counts['articles_inserted']} inserted, {counts['duplicates_skipped']} duplicates"
    )

    if dry_run:
        conn.rollback()
    else:
        conn.commit()
        conn.execute(
            "UPDATE run_log SET finished_at=?, stage_counts=?, errors=? WHERE id=?",
            (_now_iso(), json.dumps(counts), json.dumps(errors) if errors else None, run_id),
        )
        conn.commit()

    conn.close()

    total_elapsed = time.perf_counter() - t_start
    suffix = " (dry-run, nothing written)" if dry_run else ""
    print(f"[run] TOTAL: {total_elapsed:.2f}s{suffix}")

    if errors:
        print(f"[run] {len(errors)} error(s):")
        for line in errors:
            print(f"  - {line}")

    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Arc Loom pipeline entry point")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch and parse feeds but write nothing to the database",
    )
    args = parser.parse_args()
    return asyncio.run(run(dry_run=args.dry_run))


if __name__ == "__main__":
    sys.exit(main())
