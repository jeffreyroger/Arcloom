"""Week 1 closeout: 3-hour real-Task-Scheduler continuity check.

This is the 12-run "12 real-feed continuity runs" item DECISIONS.md's
"Week 1 criterion 1" entry lists as not-yet-executed, run against the
now-cleaned-up feeds.yaml under the actual Windows Task Scheduler (not the
mock-server accelerated soak, which already covers the run-count-dependent
properties in isolation).

Reads logs/week1_continuity_baseline.json (max(run_log.id) and
COUNT(*) FROM article recorded immediately before the scheduled task was
enabled) and reports everything that has happened in run_log since.

Read-only against arcloom.db, except for the idempotency check at the end,
which disables the ArcLoom scheduled task, invokes `python -m pipeline.run`
twice back-to-back itself, reads the resulting run_log rows, and
re-enables the task.
"""

import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
import config  # noqa: E402
import db  # noqa: E402
from tools.validate_feeds import longitudinal_report, print_longitudinal_report  # noqa: E402

BASELINE_PATH = ROOT / "logs" / "week1_continuity_baseline.json"

EXPECTED_RUNS_LOW, EXPECTED_RUNS_HIGH = 11, 13
GAP_LIMIT_MIN = 20
WINDOW_HOURS = 3


def _parse_ts(ts: str) -> datetime:
    return datetime.strptime(ts, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)


def _load_baseline() -> dict:
    with open(BASELINE_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


# run_log.errors holds a JSON array of per-source diagnostics on a clean
# run (stage_counts set), but a plain string ("interrupted: ..." or a
# traceback) on the two exception paths in pipeline/run.py -- distinguish
# on stage_counts first, then on the errors string's shape.
def _classify(row: dict) -> str:
    """clean | crash | interrupted | hung (started, never recorded a finish)."""
    if row["finished_at"] is None:
        return "hung"
    if row["stage_counts"] is not None:
        return "clean"
    if row["errors"] and row["errors"].startswith("interrupted:"):
        return "interrupted"
    return "crash"


def _runs_in_window(conn, baseline_id: int) -> list[dict]:
    rows = conn.execute(
        "SELECT id, started_at, finished_at, stage_counts, errors FROM run_log "
        "WHERE id > ? ORDER BY started_at",
        (baseline_id,),
    ).fetchall()
    return [dict(r) for r in rows]


def _report_runs(runs: list[dict]) -> tuple[int, int]:
    """Returns (crash_count, unexplained_gap_count)."""
    print("=" * 72)
    print(f"RUNS IN WINDOW (expect {EXPECTED_RUNS_LOW}-{EXPECTED_RUNS_HIGH} over {WINDOW_HOURS}h at 15-min cadence)")
    print("=" * 72)
    print(f"Total runs recorded: {len(runs)}")

    kinds = {"clean": 0, "crash": 0, "interrupted": 0, "hung": 0}
    for r in runs:
        kinds[_classify(r)] += 1
    print(f"clean={kinds['clean']}  crash={kinds['crash']}  interrupted={kinds['interrupted']}  hung={kinds['hung']}")

    print(f"\n{'ID':>5}  {'STARTED_AT':<21}  {'KIND':<12}  DETAIL")
    print("-" * 72)
    for r in runs:
        kind = _classify(r)
        detail = ""
        if kind == "clean":
            sc = json.loads(r["stage_counts"])
            detail = f"{sc['articles_inserted']} inserted, {sc['failures']} failed"
        elif kind in ("crash", "interrupted"):
            detail = (r["errors"] or "").splitlines()[0][:60]
        elif kind == "hung":
            detail = "no finished_at recorded"
        print(f"{r['id']:>5}  {r['started_at']:<21}  {kind:<12}  {detail}")

    print("\nGaps between consecutive started_at:")
    unexplained = 0
    for prev, cur in zip(runs, runs[1:]):
        gap_min = (_parse_ts(cur["started_at"]) - _parse_ts(prev["started_at"])).total_seconds() / 60
        if gap_min > GAP_LIMIT_MIN:
            prev_kind = _classify(prev)
            explained = prev_kind in ("crash", "interrupted", "hung")
            if not explained:
                unexplained += 1
            tag = f"(preceded by {prev_kind} run {prev['id']})" if explained else "UNEXPLAINED"
            print(f"  {prev['started_at']} -> {cur['started_at']}: {gap_min:.1f} min  {tag}")
    if not any(
        (_parse_ts(cur["started_at"]) - _parse_ts(prev["started_at"])).total_seconds() / 60 > GAP_LIMIT_MIN
        for prev, cur in zip(runs, runs[1:])
    ):
        print("  none")

    print(f"\nCrashes: {kinds['crash']}")
    print(f"Unexplained gaps (>{GAP_LIMIT_MIN}min, no crash/interrupt/hung run to account for it): {unexplained}")
    return kinds["crash"], unexplained


def _idempotency_check() -> tuple[bool, dict, dict]:
    """Disables the ArcLoom scheduled task, runs pipeline.run twice
    back-to-back, and re-enables the task. Returns
    (idempotent, first_run_stage_counts, second_run_stage_counts)."""
    print("\n" + "=" * 72)
    print("IDEMPOTENCY CHECK (pipeline.run invoked twice back-to-back)")
    print("=" * 72)

    subprocess.run(
        ["powershell.exe", "-NonInteractive", "-Command", "Disable-ScheduledTask -TaskName ArcLoom"],
        check=True, capture_output=True, text=True,
    )
    try:
        first = subprocess.run([sys.executable, "-m", "pipeline.run"], cwd=ROOT, capture_output=True, text=True)
        second = subprocess.run([sys.executable, "-m", "pipeline.run"], cwd=ROOT, capture_output=True, text=True)
        for label, proc in (("run 1", first), ("run 2", second)):
            if proc.returncode != 0:
                print(f"{label} exited {proc.returncode}:\n{proc.stderr}")
    finally:
        subprocess.run(
            ["powershell.exe", "-NonInteractive", "-Command", "Enable-ScheduledTask -TaskName ArcLoom"],
            check=True, capture_output=True, text=True,
        )

    conn = db.get_connection()
    rows = conn.execute(
        "SELECT stage_counts FROM run_log WHERE stage_counts IS NOT NULL ORDER BY id DESC LIMIT 2"
    ).fetchall()
    conn.close()
    if len(rows) < 2:
        print("Could not find two clean run_log rows after the idempotency runs.")
        return False, {}, {}

    second_counts, first_counts = json.loads(rows[0][0]), json.loads(rows[1][0])
    print(f"Run 1: {first_counts['articles_inserted']} articles inserted, {first_counts['failures']} failed")
    print(f"Run 2: {second_counts['articles_inserted']} articles inserted, {second_counts['failures']} failed")
    idempotent = second_counts["articles_inserted"] == 0
    print(f"Second run inserted zero new rows: {'YES' if idempotent else 'NO'}")
    return idempotent, first_counts, second_counts


def _yield_analysis_verdict() -> tuple[str, str]:
    """Runs tools/yield_analysis.py and pulls its criterion-3 verdict lines
    verbatim -- criterion 3 is measured from published_at over a 14-day
    window, not from this 3-hour continuity run."""
    proc = subprocess.run([sys.executable, "tools/yield_analysis.py"], cwd=ROOT, capture_output=True, text=True)
    meets_line = next((l for l in proc.stdout.splitlines() if l.startswith("Meets 500-2,000:")), "Meets 500-2,000: UNKNOWN")
    median_line = next((l for l in proc.stdout.splitlines() if l.startswith("Median:")), "Median: N/A")
    return meets_line, median_line


def main() -> int:
    baseline = _load_baseline()
    conn = db.get_connection()
    import sqlite3
    conn.row_factory = sqlite3.Row

    runs = _runs_in_window(conn, baseline["run_log_id"])
    crash_count, unexplained_gaps = _report_runs(runs)

    current_articles = conn.execute("SELECT COUNT(*) FROM article").fetchone()[0]
    print(f"\nArticles: {baseline['article_count']} at window start -> {current_articles} now "
          f"(+{current_articles - baseline['article_count']})")

    print("\n" + "=" * 72)
    print("SOURCE HEALTH (validate_feeds.py --longitudinal)")
    print("=" * 72)
    longitudinal = longitudinal_report(conn)
    longitudinal_flagged = print_longitudinal_report(longitudinal) if longitudinal else False

    print("\n" + "=" * 72)
    print("PER-PACK DAILY YIELD -- see tools/yield_analysis.py for the full 14-day, "
          "backfill-excluded breakdown used by criterion 3 below")
    print("=" * 72)
    from collections import defaultdict
    pack_counts: dict[str, int] = defaultdict(int)
    sources = {row["id"]: json.loads(row["packs"]) for row in conn.execute("SELECT id, packs FROM source")}
    for source_id, fetched_at in conn.execute(
        "SELECT source_id, fetched_at FROM article WHERE fetched_at >= ?", (baseline["window_start"],)
    ):
        for pack in sources.get(source_id, []):
            pack_counts[pack] += 1
    if pack_counts:
        for pack, n in sorted(pack_counts.items(), key=lambda kv: -kv[1]):
            print(f"  {pack:<16} {n}")
    else:
        print("  no articles fetched in window")

    idempotent, first_counts, second_counts = _idempotency_check()

    conn.close()

    print("\n" + "=" * 72)
    print("VERDICT: WEEK 1 DONE CRITERIA")
    print("=" * 72)

    c1_pass = crash_count == 0 and unexplained_gaps == 0
    print(
        f"1. Pipeline runs unattended with zero crashes: {'PASS' if c1_pass else 'FAIL'}\n"
        f"   Evidence: accelerated soak (120 runs, DECISIONS.md 'Week 1 criterion 1') "
        f"for run-count-dependent properties + this {WINDOW_HOURS}h/{len(runs)}-run real-feed "
        f"continuity window ({crash_count} crashes, {unexplained_gaps} unexplained gaps) for the "
        f"real-scheduler path the soak cannot cover."
    )

    print(
        f"\n2. Running twice back-to-back inserts zero new rows: {'PASS' if idempotent else 'FAIL'}\n"
        f"   Evidence: this run's explicit back-to-back pipeline.run invocation "
        f"(run 1: {first_counts.get('articles_inserted', '?')} inserted, "
        f"run 2: {second_counts.get('articles_inserted', '?')} inserted)."
    )

    meets_line, median_line = _yield_analysis_verdict()
    c3_pass = meets_line.strip().endswith("YES")
    print(
        f"\n3. SELECT COUNT(*) FROM article yields 500-2,000/day: {'PASS' if c3_pass else 'FAIL'}\n"
        f"   Evidence: retroactive yield analysis against published_at, corrected version "
        f"(tools/yield_analysis.py), NOT this 3-hour window -- publication date is independent "
        f"of poll cadence, and this window is too short to measure a daily rate.\n"
        f"   {median_line} / {meets_line}"
    )

    n_feeds_enabled = sum(1 for f in config.load_feeds() if f["enabled"])
    canary_conn = db.get_connection()
    canary = canary_conn.execute(
        "SELECT status FROM source WHERE name = 'Broken Feed Canary'"
    ).fetchone()
    canary_conn.close()
    c4_pass = canary is not None and canary[0] == "degraded"
    print(
        f"\n4. Deliberately broken feed correctly marked degraded: {'PASS' if c4_pass else 'FAIL'}\n"
        f"   Evidence: source table, this run -- 'Broken Feed Canary' status = "
        f"{canary[0] if canary else 'MISSING'!r}."
    )

    print(
        f"\nNote: feeds.yaml has {n_feeds_enabled} enabled feeds against week 1's 60-80 "
        f"deliverable target -- known gap, not remediated by this check."
    )
    if longitudinal_flagged:
        print("Note: source health flags (zero_yield/stalled/blocked_but_ok) fired above -- see report.")

    return 0


if __name__ == "__main__":
    sys.exit(main())
