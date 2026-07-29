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

Coverage rule (source-and-day based, not a scheduler-downtime rule): a
(source, day) pair counts as "fully captured" if there was at least one
successful fetch of that source within FEED_WINDOW_DAYS of the publish day.
An earlier version of this script instead disqualified an entire day if
run_log showed any >4h scheduler gap that day -- wrong for a published_at
measurement, because published_at is independent of when we polled. RSS
feeds are snapshots holding the last N entries, not streams: if a source
publishes 20 articles Tuesday and we first poll Thursday, we still capture
all 20 with Tuesday's publication dates. A scheduler gap only loses data
when it exceeds the feed's retention window (typically days), not when it's
a few hours -- so a same-day outage should not, by itself, disqualify that
day. That rule disqualified all 14 of 14 days in practice.

Per-source fetch-success evidence comes from run_log.errors: each clean run
(stage_counts IS NOT NULL, i.e. not a crash/interrupt) records a per-source
failure line ("source {id}: ...") for anything that didn't succeed;
enabled sources absent from that list succeeded (200 or 304). This assumes
the currently-enabled source set was also enabled during past runs in the
window -- feeds.yaml doesn't change often enough at week-1 scale for that
to matter, but it is an approximation, not a historical record.
"""

import json
import re
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
COVERAGE_QUALIFY_FRACTION = 0.90
CRITERION_LOW, CRITERION_HIGH = 500, 2000
TOP_N = 5

_SOURCE_ERROR_RE = re.compile(r"^source (\d+):")


def _parse_ts(ts: str) -> datetime:
    return datetime.strptime(ts, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)


def _day_str(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d")


def _window_days(reference: datetime) -> list[str]:
    """The WINDOW_DAYS calendar days before `reference`'s date -- excludes
    the current (partial) day."""
    today = reference.date()
    return sorted((today - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(1, WINDOW_DAYS + 1))


# Per source, the set of calendar days on which at least one clean run did
# NOT record a failure for that source -- i.e. evidence of a successful
# fetch that day. Only clean runs (stage_counts IS NOT NULL) are used: a
# crash/interrupt row's `errors` column holds a traceback/interrupt marker,
# not the per-source diagnostic list, and would otherwise be misread as
# "nothing failed" for every source.
#
# Known gap: pipeline/ingest.py's `errors` list only itemizes true
# failures, not robots.txt skips (FR-203) -- a source permanently
# disallowed by its own robots.txt is therefore counted here as
# "succeeding" every run, though it structurally never yields articles.
# Its EXCL/day contribution stays correctly at 0 regardless, so this only
# risks over-crediting that source's coverage, not inflating the article
# count. Fixing this precisely would require ingest.py to itemize skipped
# source ids per run, which run_log does not currently do.
def _fetch_success_days_by_source(conn: sqlite3.Connection, source_ids: set[int]) -> dict[int, set[str]]:
    success_days: dict[int, set[str]] = {sid: set() for sid in source_ids}
    rows = conn.execute("SELECT started_at, errors FROM run_log WHERE stage_counts IS NOT NULL")
    for started_at, errors_raw in rows:
        try:
            day = _day_str(_parse_ts(started_at))
        except ValueError:
            continue
        failed_ids: set[int] = set()
        if errors_raw:
            for line in json.loads(errors_raw):
                m = _SOURCE_ERROR_RE.match(line)
                if m:
                    failed_ids.add(int(m.group(1)))
        for sid in source_ids:
            if sid not in failed_ids:
                success_days[sid].add(day)
    return success_days


# A (source, day) pair is fully captured if the source had a successful
# fetch on `day` itself or any of the following FEED_WINDOW_DAYS-1 days --
# any of those polls would have caught the article before it could scroll
# out of the feed's retention window.
def _coverage_by_day(
    days: list[str], source_ids: set[int], success_days: dict[int, set[str]]
) -> dict[str, tuple[float, int, int]]:
    coverage = {}
    for d in days:
        day_dt = datetime.strptime(d, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        window = [(day_dt + timedelta(days=i)).strftime("%Y-%m-%d") for i in range(config.FEED_WINDOW_DAYS)]
        captured = sum(
            1 for sid in source_ids if success_days.get(sid, set()) & set(window)
        )
        total = len(source_ids)
        coverage[d] = (captured / total if total else 0.0, captured, total)
    return coverage


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
    # Coverage denominator is enabled AND status='ok' sources: a degraded
    # source (e.g. the deliberately-broken canary URL required by week 1's
    # done-criteria) has fail_streak-based tracking of its own and can never
    # register a successful fetch again, so counting it here would put a
    # permanent ceiling on every day's coverage fraction regardless of how
    # healthy the rest of polling is. Matches the exclusion already applied
    # to the yield-ranking sections below.
    coverage_ids = {
        row[0] for row in conn.execute("SELECT id FROM source WHERE enabled = 1 AND status = 'ok'")
    }
    first_fetch = _first_fetch_by_source(conn)
    raw_by_day, excl_by_day, excl_by_day_pack, excl_by_source_total = _collect(conn, first_fetch, sources)

    success_days = _fetch_success_days_by_source(conn, coverage_ids)
    coverage_by_day = _coverage_by_day(days, coverage_ids, success_days)
    qualifying = {d: coverage_by_day[d][0] >= COVERAGE_QUALIFY_FRACTION for d in days}

    print(f"Window: last {WINDOW_DAYS} days by published_at, {days[0]} .. {days[-1]} (current partial day excluded)")
    print(
        f"Coverage rule: a source counts as captured for day D if it had >=1 successful "
        f"fetch within {config.FEED_WINDOW_DAYS} day(s) of D; a day qualifies at "
        f">={COVERAGE_QUALIFY_FRACTION:.0%} of {len(coverage_ids)} enabled+ok sources captured.\n"
    )

    print(f"{'DATE':<12} {'RAW':>6} {'EXCL':>6} {'COVERAGE':>10}  QUALIFYING")
    print("-" * 52)
    for d in days:
        frac, captured, total = coverage_by_day[d]
        flag = "yes" if qualifying[d] else "no"
        print(f"{d:<12} {raw_by_day.get(d, 0):>6} {excl_by_day.get(d, 0):>6} "
              f"{captured:>4}/{total:<4}  {flag}")

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
        print(f"Median: N/A -- no day in the last {WINDOW_DAYS} reached "
              f"{COVERAGE_QUALIFY_FRACTION:.0%} source coverage. Criterion 3 cannot be measured "
              "yet from this window.")
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

    print("\n" + "=" * 72)
    print("PER-PACK DAILY YIELD (backfill-excluded) -- pack balance drives feed expansion")
    print("=" * 72)
    pack_names = sorted(pack_totals.keys())
    if pack_names:
        header = f"{'DATE':<12}" + "".join(f"{p:>16}" for p in pack_names)
        print(header)
        print("-" * len(header))
        for d in days:
            row = [excl_by_day_pack.get(d, {}).get(p, 0) for p in pack_names]
            print(f"{d:<12}" + "".join(f"{c:>16}" for c in row))
        print("(a source can carry more than one pack, so rows need not sum to EXCL above)")
    else:
        print("  no pack data")

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
