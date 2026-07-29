"""Read-only pipeline health report. Standalone, not part of the pipeline."""

import sqlite3
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from tools.validate_feeds import longitudinal_report, print_longitudinal_report  # noqa: E402

DB_PATH = Path(__file__).resolve().parent.parent / "arcloom.db"


def main() -> int:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    total_articles = conn.execute("SELECT COUNT(*) FROM article").fetchone()[0]

    cutoff = (datetime.now(timezone.utc) - timedelta(hours=24)).strftime("%Y-%m-%dT%H:%M:%SZ")
    last_24h = conn.execute(
        "SELECT COUNT(*) FROM article WHERE fetched_at >= ?", (cutoff,)
    ).fetchone()[0]

    print(f"Total articles: {total_articles}")
    print(f"Articles ingested in last 24h (by fetched_at): {last_24h}")

    print("\nFeeds by status:")
    for row in conn.execute("SELECT status, COUNT(*) AS n FROM source GROUP BY status ORDER BY status"):
        print(f"  {row['status']}: {row['n']}")

    print("\nTop 5 feeds by fail_streak:")
    for row in conn.execute(
        "SELECT name, fail_streak, status FROM source ORDER BY fail_streak DESC LIMIT 5"
    ):
        print(f"  {row['name']}: fail_streak={row['fail_streak']} status={row['status']}")

    newest = conn.execute("SELECT MAX(published_at) FROM article").fetchone()[0]
    oldest = conn.execute("SELECT MIN(published_at) FROM article").fetchone()[0]
    print(f"\nNewest published_at: {newest}")
    print(f"Oldest published_at: {oldest}")

    # A point-in-time validate_feeds.py run can't see "enabled N days,
    # produced nothing" -- that only shows up by querying history, which is
    # why this runs here too, on every routine health check, not just when
    # someone remembers to run --longitudinal by hand.
    print("\nLongitudinal source check (zero_yield / stalled / blocked_but_ok):")
    results = longitudinal_report(conn)
    flagged = print_longitudinal_report(results) if results else False

    conn.close()
    return 1 if flagged else 0


if __name__ == "__main__":
    sys.exit(main())
