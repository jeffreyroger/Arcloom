"""Week 1 closeout: accelerated soak, substituting for the 24h unattended run.

Rationale (recorded per week 1's done-criteria): pipeline/run.py is a
one-shot process that exits between runs, so cross-run *memory* leaks are
structurally impossible. What can actually degrade over many runs is
persistent state — database growth, WAL checkpointing, log/run_log
accumulation, and cache expiry paths — and those are run-count-dependent,
not wall-clock-dependent. 120 compressed runs exercises that state machine
more than 96 runs spread over 24 real hours would.

Drives:
  - tools/mock_feed_server.py as a subprocess, so this never touches real
    publishers.
  - pipeline/run.py --feeds feeds.mock.yaml --db soak.db, 120 times, against
    a database production code never reads (NFR-401/NFR-402 unaffected).
  - ARCLOOM_ROBOTS_CACHE_TTL_H=0.005 (18s) in the *child* subprocess
    environment only — os.environ.copy() means the override never leaks
    into this process or any other, so there is nothing to "restore" for
    config.py's real 24h default. This deliberately forces the
    cache-expiry-and-refetch path (FR-203) within the 20s soak interval,
    which a 40-minute run at the real TTL would never reach.
"""

import json
import os
import socket
import subprocess
import sys
import time
from pathlib import Path
from statistics import mean

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
import config  # noqa: E402

DB_PATH = ROOT / "soak.db"
FEEDS_PATH = ROOT / "feeds.mock.yaml"
LOG_PATH = ROOT / "logs" / "soak_run_log.jsonl"
MOCK_PORT = 8765
SOAK_RUNS = 120
INTERVAL_S = 20
ROBOTS_TTL_OVERRIDE_H = 0.005  # 18s: shorter than INTERVAL_S, forces expiry every run
CANARY_NAME = "Mock Invalid Host Canary"
FULLBODY_SOURCE_NAME = "Mock Full Body"
DRIFT_FAIL_PCT = 20.0
WAL_GROWTH_FAIL_RATIO = 1.2


def _wait_for_server(port: int, timeout: float = 10.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.5):
                return
        except OSError:
            time.sleep(0.2)
    raise RuntimeError(f"mock_feed_server did not come up on port {port} within {timeout}s")


def _fresh_db() -> None:
    for suffix in ("", "-wal", "-shm"):
        p = Path(str(DB_PATH) + suffix)
        if p.exists():
            p.unlink()


def _run_once(env: dict, run_no: int) -> dict:
    t0 = time.perf_counter()
    result = subprocess.run(
        [sys.executable, "-m", "pipeline.run", "--feeds", str(FEEDS_PATH), "--db", str(DB_PATH)],
        cwd=ROOT,
        env=env,
        capture_output=True,
        text=True,
    )
    duration = time.perf_counter() - t0

    import sqlite3

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    row = conn.execute(
        "SELECT stage_counts, errors, finished_at FROM run_log ORDER BY id DESC LIMIT 1"
    ).fetchone()
    run_log_rows = conn.execute("SELECT COUNT(*) FROM run_log").fetchone()[0]
    conn.close()

    stage_counts = json.loads(row["stage_counts"]) if row and row["stage_counts"] else {}

    db_bytes = DB_PATH.stat().st_size if DB_PATH.exists() else 0
    wal_path = Path(str(DB_PATH) + "-wal")
    wal_bytes = wal_path.stat().st_size if wal_path.exists() else 0

    return {
        "run": run_no,
        "exit_code": result.returncode,
        "duration_s": round(duration, 3),
        "db_bytes": db_bytes,
        "wal_bytes": wal_bytes,
        "run_log_rows": run_log_rows,
        "articles_inserted": stage_counts.get("articles_inserted"),
        "duplicates_skipped": stage_counts.get("duplicates_skipped"),
        "skipped_robots": stage_counts.get("skipped_robots"),
        "robots_cache_hits": stage_counts.get("robots_cache_hits"),
        "robots_fetches": stage_counts.get("robots_fetches"),
        "finished_at": row["finished_at"] if row else None,
        "errors_raw": row["errors"] if row else None,
        "stderr_tail": (result.stderr or "")[-500:],
    }


def _soak(env: dict) -> list[dict]:
    records = []
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(LOG_PATH, "w", encoding="utf-8") as logf:
        for run_no in range(1, SOAK_RUNS + 1):
            record = _run_once(env, run_no)
            records.append(record)
            logf.write(json.dumps(record) + "\n")
            logf.flush()
            print(
                f"[soak] run {run_no:>3}/{SOAK_RUNS}: exit={record['exit_code']} "
                f"dur={record['duration_s']:.2f}s db={record['db_bytes']}B wal={record['wal_bytes']}B "
                f"ins={record['articles_inserted']} dup={record['duplicates_skipped']} "
                f"skip_robots={record['skipped_robots']} robots_fetches={record['robots_fetches']}"
            )
            if run_no < SOAK_RUNS:
                time.sleep(INTERVAL_S)
    return records


# NFR-402: an unexplained gap is a run_log row with neither a finished_at
# timestamp nor an errors value -- i.e. the process died somewhere run.py's
# own exception handling couldn't reach (e.g. killed, not raised).
def _unexplained_gaps() -> int:
    import sqlite3

    conn = sqlite3.connect(DB_PATH)
    count = conn.execute(
        "SELECT COUNT(*) FROM run_log WHERE finished_at IS NULL AND errors IS NULL"
    ).fetchone()[0]
    conn.close()
    return count


def _canary_status() -> tuple[int | None, str | None]:
    import sqlite3

    conn = sqlite3.connect(DB_PATH)
    row = conn.execute(
        "SELECT fail_streak, status FROM source WHERE name = ?", (CANARY_NAME,)
    ).fetchone()
    conn.close()
    return (row[0], row[1]) if row else (None, None)


def _max_fullbody_snippet_len() -> int | None:
    import sqlite3

    conn = sqlite3.connect(DB_PATH)
    row = conn.execute(
        """
        SELECT MAX(LENGTH(a.snippet)) FROM article a
        JOIN source s ON s.id = a.source_id
        WHERE s.name = ?
        """,
        (FULLBODY_SOURCE_NAME,),
    ).fetchone()
    conn.close()
    return row[0] if row else None


def _report(records: list[dict]) -> bool:
    verdicts = []  # (label, ok, detail)

    crashes = [r for r in records if r["exit_code"] != 0]
    verdicts.append(("Crashes (exit_code != 0)", len(crashes) == 0, f"{len(crashes)} of {len(records)} runs"))

    gaps = _unexplained_gaps()
    verdicts.append(("Unexplained gaps (finished_at/errors both NULL)", gaps == 0, f"{gaps} row(s)"))

    early = [r["duration_s"] for r in records if 1 <= r["run"] <= 20]
    late = [r["duration_s"] for r in records if 100 <= r["run"] <= 120]
    mean_early, mean_late = mean(early), mean(late)
    pct_change = ((mean_late - mean_early) / mean_early * 100) if mean_early else 0.0
    verdicts.append((
        "Run duration drift (runs 1-20 vs 100-120)",
        abs(pct_change) <= DRIFT_FAIL_PCT,
        f"mean {mean_early:.2f}s -> {mean_late:.2f}s ({pct_change:+.1f}%)",
    ))

    by_run = {r["run"]: r for r in records}
    db_20, db_120 = by_run[20]["db_bytes"], by_run[120]["db_bytes"]
    articles_after_20 = sum(r["articles_inserted"] or 0 for r in records if r["run"] > 20)
    db_growth_expected = articles_after_20 == 0
    verdicts.append((
        "Database growth (run 20 -> 120, article growth after warm-up)",
        db_growth_expected,
        f"{db_20}B -> {db_120}B ({db_120 - db_20:+d}B); {articles_after_20} article(s) inserted after run 20",
    ))

    wal_series = [r["wal_bytes"] for r in records]
    wal_monotonic = all(b >= a for a, b in zip(wal_series, wal_series[1:]))
    wal_start = next((w for w in wal_series if w > 0), 0)
    wal_end = wal_series[-1]
    wal_ratio = (wal_end / wal_start) if wal_start else (1.0 if wal_end == 0 else float("inf"))
    wal_ok = not (wal_monotonic and wal_ratio > WAL_GROWTH_FAIL_RATIO)
    verdicts.append((
        "WAL checkpoints (not monotonic growth)",
        wal_ok,
        f"monotonic={wal_monotonic}, {wal_start}B -> {wal_end}B (x{wal_ratio:.2f})",
    ))

    total_inserted_after_run1 = sum(r["articles_inserted"] or 0 for r in records if r["run"] >= 2)
    verdicts.append((
        "Idempotency (articles inserted, runs 2-120)",
        total_inserted_after_run1 == 0,
        f"{total_inserted_after_run1} new article(s) across runs 2-120",
    ))

    fetches_after_run1 = sum(1 for r in records if r["run"] >= 2 and (r["robots_fetches"] or 0) > 0)
    verdicts.append((
        "Robots cache expiry (18s TTL forces refetch)",
        fetches_after_run1 > 0,
        f"{fetches_after_run1} of {len(records) - 1} post-run-1 runs triggered a robots.txt refetch",
    ))

    fail_streak, status = _canary_status()
    canary_ok = fail_streak is not None and fail_streak >= config.FAIL_STREAK_LIMIT and status == "degraded"
    verdicts.append((
        "Canary degraded (invalid host)",
        canary_ok,
        f"fail_streak={fail_streak}, status={status!r}",
    ))

    blocked_skipped_every_run = all((r["skipped_robots"] or 0) == 1 for r in records)
    verdicts.append((
        "Blocked feed never fetched (FR-203)",
        blocked_skipped_every_run,
        f"skipped_robots==1 on {sum(1 for r in records if (r['skipped_robots'] or 0) == 1)}/{len(records)} runs",
    ))

    max_snippet = _max_fullbody_snippet_len()
    snippet_ok = max_snippet is not None and max_snippet <= 300
    verdicts.append((
        "Full-body snippet <= 300 chars (NFR-602)",
        snippet_ok,
        f"max snippet length = {max_snippet}",
    ))

    label_w = max(len(label) for label, _, _ in verdicts)
    print("\n" + "=" * 100)
    print("ACCELERATED SOAK VERDICT")
    print("=" * 100)
    for label, ok, detail in verdicts:
        print(f"{'PASS' if ok else 'FAIL':<5} {label:<{label_w}}  {detail}")
    print("=" * 100)

    all_ok = all(ok for _, ok, _ in verdicts)
    print("OVERALL:", "PASS" if all_ok else "FAIL")
    return all_ok


def main() -> int:
    print(f"[soak] resetting {DB_PATH}")
    _fresh_db()

    print(f"[soak] starting mock_feed_server on port {MOCK_PORT}")
    server_proc = subprocess.Popen(
        [sys.executable, str(ROOT / "tools" / "mock_feed_server.py"), "--port", str(MOCK_PORT), "--quiet"],
        cwd=ROOT,
    )
    try:
        _wait_for_server(MOCK_PORT)

        env = os.environ.copy()
        env["ARCLOOM_ROBOTS_CACHE_TTL_H"] = str(ROBOTS_TTL_OVERRIDE_H)
        print(
            f"[soak] child env ARCLOOM_ROBOTS_CACHE_TTL_H={ROBOTS_TTL_OVERRIDE_H} "
            f"({ROBOTS_TTL_OVERRIDE_H * 3600:.0f}s TTL); this process's own environment is untouched"
        )

        records = _soak(env)
    finally:
        print("[soak] stopping mock_feed_server")
        server_proc.terminate()
        try:
            server_proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            server_proc.kill()

    assert "ARCLOOM_ROBOTS_CACHE_TTL_H" not in os.environ, "override leaked into parent environment"

    ok = _report(records)
    print(f"\n[soak] per-run log: {LOG_PATH}")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
