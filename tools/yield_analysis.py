"""Week 1 closeout, done-criterion 3: 500-2,000 articles/day, measured
retroactively from published_at instead of waiting on a dedicated 24h soak
window.

Read-only against arcloom.db. Nothing here writes to the database.

Grouping by published_at (not fetched_at) is deliberate: publication date
reflects when a story actually happened, independent of our poll cadence.
But a source's very first fetch pulls in whatever the feed's current window
already contains -- articles published well before we ever started
watching that source. Those are backfill, not observed daily yield, and are
excluded from the "backfill-excluded" figures (source_first_fetch =
MIN(article.fetched_at) per source_id; any article with published_at
before that is backfill for that source).

A day only counts toward the criterion-3 median if run_log shows the
scheduler wasn't down for more than COVERAGE_GAP_FAIL_HOURS that day --
otherwise a quiet day reflects an outage, not low yield.
"""

import json
import sqlite3
import statistics
import sys
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
import config  # noqa: E402
import db  # noqa: E402

WINDOW_DAYS = 14
COVERAGE_GAP_FAIL_HOURS = 4.0
CRITERION_LOW, CRITERION_HIGH = 500, 2000
TOP_N = 5


def _parse_ts(ts: str) -> datetime:
    return datetime.strptime(ts, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)


def _day_str(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d")


def _window_days(reference: datetime) -> list[str]:
    """The WINDOW_DAYS calendar days before `reference`'s date -- excludes
    the current (partial) day."""
    today = reference.date()
    return sorted((today - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(1, WINDOW_DAYS + 1))


# Only gaps whose *raw* length exceeds COVERAGE_GAP_FAIL_HOURS count as an
# outage at all (normal ~15-minute cron jitter must never disqualify a
# day); each qualifying gap's overlap with a given day is then summed, so a
# day touched by more than one outage is still correctly disqualified.
def _scheduler_downtime_by_day(conn: sqlite3.Connection, days: list[str]) -> dict[str, float]:
    starts = sorted(_parse_ts(row[0]) for row in conn.execute("SELECT started_at FROM run_log"))
    downtime = {d: 0.0 for d in days}

    day_bounds = {}
    for d in days:
        day_start = datetime.strptime(d, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        day_bounds[d] = (day_start, day_start + timedelta(days=1))

    if len(starts) < 2:
        return {d: 24.0 for d in days}

    for t0, t1 in zip(starts, starts[1:]):
        gap_hours = (t1 - t0).total_seconds() / 3600
        if gap_hours <= COVERAGE_GAP_FAIL_HOURS:
            continue
        for d, (day_start, day_end) in day_bounds.items():
            overlap_start = max(t0, day_start)
            overlap_end = min(t1, day_end)
            if overlap_end > overlap_start:
                downtime[d] += (overlap_end - overlap_start).total_seconds() / 3600

    # A day entirely outside the run_log's recorded span has no coverage
    # information at all -- treat it as fully down, not as a clean zero.
    for d, (day_start, day_end) in day_bounds.items():
        if day_end <= starts[0] or day_start >= starts[-1]:
            downtime[d] = 24.0

    return downtime


def _first_fetch_by_source(conn: sqlite3.Connection) -> dict[int, datetime]:
    rows = conn.execute("SELECT source_id, MIN(fetched_at) FROM article GROUP BY source_id")
    return {source_id: _parse_ts(first) for source_id, first in rows}


def _collect(conn: sqlite3.Connection, first_fetch: dict[int, datetime], sources: dict[int, dict]):
    raw_by_day = defaultdict(int)
    excl_by_day = defaultdict(int)
    excl_by_day_pack = defaultdict(lambda: defaultdict(int))
    excl_by_source_total = defaultdict(int)

    for source_id, published_at in conn.execute("SELECT source_id, published_at FROM article"):
        try:
            pub_dt = _parse_ts(published_at)
        except ValueError:
            continue

        day = _day_str(pub_dt)
        raw_by_day[day] += 1

        ff = first_fetch.get(source_id)
        if ff is not None and pub_dt < ff:
            continue  # backfill: predates this source's first-ever fetch

        excl_by_day[day] += 1
        excl_by_source_total[source_id] += 1
        for pack in json.loads(sources.get(source_id, {}).get("packs") or "[]"):
            excl_by_day_pack[day][pack] += 1

    return raw_by_day, excl_by_day, excl_by_day_pack, excl_by_source_total


def _zero_yield_ok_sources(conn: sqlite3.Connection, cutoff: datetime) -> list[str]:
    """FR-104/validator gap: a source marked 'ok' with zero articles
    actually fetched in the whole window -- validate_feeds.py can't catch
    this since it only ever checks a feed once, at one point in time."""
    cutoff_iso = cutoff.strftime("%Y-%m-%dT%H:%M:%SZ")
    rows = conn.execute(
        """
        SELECT s.name, COUNT(a.id) AS n
        FROM source s
        LEFT JOIN article a ON a.source_id = s.id AND a.fetched_at >= ?
        WHERE s.status = 'ok'
        GROUP BY s.id
        HAVING n = 0
        ORDER BY s.name
        """,
        (cutoff_iso,),
    ).fetchall()
    return [row[0] for row in rows]


def main() -> int:
    conn = db.get_connection()
    conn.row_factory = sqlite3.Row

    now = datetime.now(timezone.utc)
    days = _window_days(now)

    sources = {row["id"]: dict(row) for row in conn.execute("SELECT id, name, packs, status FROM source")}
    first_fetch = _first_fetch_by_source(conn)
    raw_by_day, excl_by_day, excl_by_day_pack, excl_by_source_total = _collect(conn, first_fetch, sources)
    downtime_by_day = _scheduler_downtime_by_day(conn, days)
    qualifying = {d: downtime_by_day[d] <= COVERAGE_GAP_FAIL_HOURS for d in days}

    print(f"Window: last {WINDOW_DAYS} days by published_at, {days[0]} .. {days[-1]} (current partial day excluded)\n")

    print(f"{'DATE':<12} {'RAW':>6} {'EXCL':>6} {'DOWNTIME_H':>10}  QUALIFYING")
    print("-" * 52)
    for d in days:
        flag = "yes" if qualifying[d] else f"no (down {downtime_by_day[d]:.1f}h)"
        print(f"{d:<12} {raw_by_day.get(d, 0):>6} {excl_by_day.get(d, 0):>6} {downtime_by_day[d]:>10.1f}  {flag}")

    qualifying_counts = [excl_by_day.get(d, 0) for d in days if qualifying[d]]
    n_feeds = len(config.load_feeds())

    print("\n" + "=" * 72)
    print("CRITERION 3: 500-2,000 articles/day (backfill-excluded, published_at)")
    print("=" * 72)
    print(f"Qualifying full days: {len(qualifying_counts)} of {WINDOW_DAYS}")
    if qualifying_counts:
        median = statistics.median(qualifying_counts)
        meets = CRITERION_LOW <= median <= CRITERION_HIGH
        print(f"Median: {median:g} articles/day")
        print(f"Meets 500-2,000: {'YES' if meets else 'NO'}")
    else:
        print("Median: N/A -- no day in the last 14 has scheduler coverage clean of a "
              f">{COVERAGE_GAP_FAIL_HOURS:.0f}h gap. Criterion 3 cannot be measured yet from this window.")
    print(
        f"Note: {n_feeds} feeds is expected to fall short of 500-2,000/day on its own; "
        "re-run this script after feed expansion (week 1's 60-80 target, or later weeks' up to 100)."
    )

    print("\n" + "=" * 72)
    print("ARTICLES/DAY PER PACK (backfill-excluded, mean over the 14-day window)")
    print("=" * 72)
    pack_totals: dict[str, int] = defaultdict(int)
    for d in days:
        for pack, n in excl_by_day_pack.get(d, {}).items():
            pack_totals[pack] += n
    for pack, total in sorted(pack_totals.items(), key=lambda kv: -kv[1]):
        print(f"  {pack:<16} {total / WINDOW_DAYS:>8.2f}/day  ({total} total)")

    # Ranking excludes disabled and degraded sources: those are already
    # tracked failures with an established remediation path (health.py,
    # fail_streak). What's actionable for feed expansion is an
    # ostensibly-healthy feed that still yields little or nothing.
    active_ids = {sid for sid, s in sources.items() if s["status"] == "ok"}
    active_totals = [
        (sources[sid]["name"], excl_by_source_total.get(sid, 0))
        for sid in active_ids
    ]
    active_totals.sort(key=lambda kv: -kv[1])

    print("\n" + "=" * 72)
    print(f"TOP {TOP_N} YIELD SOURCES (status='ok', backfill-excluded, 14-day total)")
    print("=" * 72)
    for name, total in active_totals[:TOP_N]:
        print(f"  {name:<40} {total}")

    print(f"\nBOTTOM {TOP_N} YIELD SOURCES (status='ok', backfill-excluded, 14-day total)")
    print("-" * 72)
    for name, total in sorted(active_totals, key=lambda kv: kv[1])[:TOP_N]:
        print(f"  {name:<40} {total}")
    print(
        "Note: a source can show 0 here yet have fetched articles historically -- this "
        "list is backfill-excluded observed yield, not raw fetch activity. See the "
        "status='ok' zero-yield flag below for the raw-activity version of this check."
    )

    print("\n" + "=" * 72)
    print(f"SOURCES FLAGGED status='ok' WITH ZERO ARTICLES IN {WINDOW_DAYS} DAYS")
    print("=" * 72)
    cutoff = now - timedelta(days=WINDOW_DAYS)
    flagged = _zero_yield_ok_sources(conn, cutoff)
    if flagged:
        for name in flagged:
            print(f"  {name}")
        print(f"\n{len(flagged)} source(s) report 'ok' but yielded nothing -- the validator's one-time "
              "snapshot check missed this; investigate feed structure/entry extraction.")
    else:
        print("  none")

    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
