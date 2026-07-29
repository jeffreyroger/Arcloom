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
import sqlite3
import subprocess
import sys
import threading
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
WAL_SAMPLE_INTERVAL_S = 0.2
DB_GROWTH_FAIL_BYTES_PER_RUN = 2048  # 2 KB/run with zero articles inserted is unexplained


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


# The database is a one-shot process's file: by the time subprocess.run()
# returns, pipeline/run.py has already closed its only connection, and
# SQLite checkpoints (truncates) the WAL on last-connection-close. Sampling
# after exit therefore always reads 0 -- this is why the WAL check used to
# report a PASS it hadn't earned. To see the WAL mid-flight, a background
# thread holds its own read-only connection open for the run's duration
# and polls the -wal file's size every WAL_SAMPLE_INTERVAL_S while the
# child subprocess is in flight.
def _sample_wal_during(stop: threading.Event, samples: list[int]) -> None:
    wal_path = Path(str(DB_PATH) + "-wal")
    try:
        ro_conn = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True)
    except sqlite3.OperationalError:
        return  # DB file doesn't exist yet (only possible before run 1)
    try:
        while not stop.is_set():
            samples.append(wal_path.stat().st_size if wal_path.exists() else 0)
            stop.wait(WAL_SAMPLE_INTERVAL_S)
    finally:
        ro_conn.close()


def _run_once(env: dict, run_no: int) -> dict:
    wal_samples: list[int] = []
    stop = threading.Event()
    sampler = threading.Thread(target=_sample_wal_during, args=(stop, wal_samples), daemon=True)
    sampler.start()

    t0 = time.perf_counter()
    result = subprocess.run(
        [sys.executable, "-m", "pipeline.run", "--feeds", str(FEEDS_PATH), "--db", str(DB_PATH)],
        cwd=ROOT,
        env=env,
        capture_output=True,
        text=True,
    )
    duration = time.perf_counter() - t0

    stop.set()
    sampler.join(timeout=2)

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    row = conn.execute(
        "SELECT stage_counts, errors, finished_at FROM run_log ORDER BY id DESC LIMIT 1"
    ).fetchone()
    run_log_rows = conn.execute("SELECT COUNT(*) FROM run_log").fetchone()[0]
    conn.close()

    stage_counts = json.loads(row["stage_counts"]) if row and row["stage_counts"] else {}

    db_bytes = DB_PATH.stat().st_size if DB_PATH.exists() else 0
    wal_bytes_end = 0  # always 0: SQLite checkpoints the WAL on connection close
    wal_bytes_max = max(wal_samples) if wal_samples else 0
    wal_samples_taken = len(wal_samples)

    return {
        "run": run_no,
        "exit_code": result.returncode,
        "duration_s": round(duration, 3),
        "db_bytes": db_bytes,
        "wal_bytes": wal_bytes_end,
        "wal_bytes_max": wal_bytes_max,
        "wal_samples_taken": wal_samples_taken,
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
                f"dur={record['duration_s']:.2f}s db={record['db_bytes']}B "
                f"wal_max={record['wal_bytes_max']}B ({record['wal_samples_taken']} samples) "
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
    conn = sqlite3.connect(DB_PATH)
    count = conn.execute(
        "SELECT COUNT(*) FROM run_log WHERE finished_at IS NULL AND errors IS NULL"
    ).fetchone()[0]
    conn.close()
    return count


def _canary_status() -> tuple[int | None, str | None]:
    conn = sqlite3.connect(DB_PATH)
    row = conn.execute(
        "SELECT fail_streak, status FROM source WHERE name = ?", (CANARY_NAME,)
    ).fetchone()
    conn.close()
    return (row[0], row[1]) if row else (None, None)


def _max_fullbody_snippet_len() -> int | None:
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
    verdicts = []  # (label, status, detail) -- status in {"PASS", "FAIL", "NOT OBSERVED"}

    crashes = [r for r in records if r["exit_code"] != 0]
    verdicts.append((
        "Crashes (exit_code != 0)",
        "PASS" if len(crashes) == 0 else "FAIL",
        f"{len(crashes)} of {len(records)} runs",
    ))

    gaps = _unexplained_gaps()
    verdicts.append((
        "Unexplained gaps (finished_at/errors both NULL)",
        "PASS" if gaps == 0 else "FAIL",
        f"{gaps} row(s)",
    ))

    early = [r["duration_s"] for r in records if 1 <= r["run"] <= 20]
    late = [r["duration_s"] for r in records if 100 <= r["run"] <= 120]
    mean_early, mean_late = mean(early), mean(late)
    pct_change = ((mean_late - mean_early) / mean_early * 100) if mean_early else 0.0
    verdicts.append((
        "Run duration drift (runs 1-20 vs 100-120)",
        "PASS" if abs(pct_change) <= DRIFT_FAIL_PCT else "FAIL",
        f"mean {mean_early:.2f}s -> {mean_late:.2f}s ({pct_change:+.1f}%)",
    ))

    # Idempotency: does a repeat run insert new articles? Not the same
    # question as "does the database grow" -- run_log accumulation grows
    # the file with zero articles inserted, which is a separate, correctly
    # bounded question handled below.
    total_inserted_after_run1 = sum(r["articles_inserted"] or 0 for r in records if r["run"] >= 2)
    verdicts.append((
        "Idempotency (articles inserted, runs 2-120)",
        "PASS" if total_inserted_after_run1 == 0 else "FAIL",
        f"{total_inserted_after_run1} new article(s) across runs 2-120",
    ))

    # DB growth: bytes/run over the post-warm-up window (run 20 -> 120,
    # after the one-time run-1 inserts), independent of idempotency. Only a
    # FAIL if bytes are growing *and* nothing (articles) explains it --
    # growth driven by real inserts is expected and not a defect.
    by_run = {r["run"]: r for r in records}
    db_20, db_120 = by_run[20]["db_bytes"], by_run[120]["db_bytes"]
    growth_total = db_120 - db_20
    bytes_per_run = growth_total / (120 - 20)
    articles_after_20 = sum(r["articles_inserted"] or 0 for r in records if r["run"] > 20)
    db_growth_unexplained = articles_after_20 == 0 and bytes_per_run > DB_GROWTH_FAIL_BYTES_PER_RUN
    verdicts.append((
        "DB growth (bytes/run, run 20 -> 120)",
        "FAIL" if db_growth_unexplained else "PASS",
        f"{db_20}B -> {db_120}B ({growth_total:+d}B, {bytes_per_run:.0f}B/run); "
        f"{articles_after_20} article(s) inserted after run 20 "
        f"(bound: <={DB_GROWTH_FAIL_BYTES_PER_RUN}B/run when zero articles inserted)",
    ))

    # WAL: peak size sampled by a background reader while each child run is
    # in flight (see _sample_wal_during) -- post-exit sampling always reads
    # 0 because SQLite checkpoints the WAL on the writer's last connection
    # close, so that can never be evidence of anything. If the sampler
    # never caught a nonzero size on any run, say so plainly instead of
    # reporting a pass it didn't earn.
    wal_peaks = [r["wal_bytes_max"] for r in records]
    observed_runs = sum(1 for w in wal_peaks if w > 0)
    if observed_runs == 0:
        verdicts.append((
            "WAL mid-run peak (sampled while child run is in flight)",
            "NOT OBSERVED",
            f"sampler recorded 0B on all {len(records)} runs -- either checkpoints faster than "
            f"the {WAL_SAMPLE_INTERVAL_S * 1000:.0f}ms poll interval, or mid-run sampling never "
            f"attached before the run finished",
        ))
    else:
        early_peaks = [w for w in wal_peaks[:20] if w > 0]
        late_peaks = [w for w in wal_peaks[-20:] if w > 0]
        mean_early_peak = mean(early_peaks) if early_peaks else 0.0
        mean_late_peak = mean(late_peaks) if late_peaks else 0.0
        wal_ratio = (mean_late_peak / mean_early_peak) if mean_early_peak else float("inf")
        wal_growth_ok = mean_early_peak == 0 or wal_ratio <= WAL_GROWTH_FAIL_RATIO
        verdicts.append((
            "WAL mid-run peak (sampled while child run is in flight)",
            "PASS" if wal_growth_ok else "FAIL",
            f"observed on {observed_runs}/{len(records)} runs; mean peak runs 1-20={mean_early_peak:.0f}B, "
            f"runs 100-120={mean_late_peak:.0f}B (x{wal_ratio:.2f}), max ever={max(wal_peaks)}B",
        ))

    fetches_after_run1 = sum(1 for r in records if r["run"] >= 2 and (r["robots_fetches"] or 0) > 0)
    verdicts.append((
        "Robots cache expiry (18s TTL forces refetch)",
        "PASS" if fetches_after_run1 > 0 else "FAIL",
        f"{fetches_after_run1} of {len(records) - 1} post-run-1 runs triggered a robots.txt refetch",
    ))

    fail_streak, status = _canary_status()
    canary_ok = fail_streak is not None and fail_streak >= config.FAIL_STREAK_LIMIT and status == "degraded"
    verdicts.append((
        "Canary degraded (invalid host)",
        "PASS" if canary_ok else "FAIL",
        f"fail_streak={fail_streak}, status={status!r}",
    ))

    blocked_skipped_every_run = all((r["skipped_robots"] or 0) == 1 for r in records)
    verdicts.append((
        "Blocked feed never fetched (FR-203)",
        "PASS" if blocked_skipped_every_run else "FAIL",
        f"skipped_robots==1 on {sum(1 for r in records if (r['skipped_robots'] or 0) == 1)}/{len(records)} runs",
    ))

    max_snippet = _max_fullbody_snippet_len()
    snippet_ok = max_snippet is not None and max_snippet <= 300
    verdicts.append((
        "Full-body snippet <= 300 chars (NFR-602)",
        "PASS" if snippet_ok else "FAIL",
        f"max snippet length = {max_snippet}",
    ))

    label_w = max(len(label) for label, _, _ in verdicts)
    print("\n" + "=" * 100)
    print("ACCELERATED SOAK VERDICT")
    print("=" * 100)
    for label, status, detail in verdicts:
        print(f"{status:<12} {label:<{label_w}}  {detail}")
    print("=" * 100)

    # NOT OBSERVED means inconclusive, not failing -- it must not silently
    # read as a pass either, which is why it's printed distinctly above and
    # called out again here rather than folded into the PASS/FAIL count.
    not_observed = [label for label, status, _ in verdicts if status == "NOT OBSERVED"]
    all_ok = all(status != "FAIL" for _, status, _ in verdicts)
    print("OVERALL:", "PASS" if all_ok else "FAIL", end="")
    if not_observed:
        print(f"  ({len(not_observed)} check(s) NOT OBSERVED: {', '.join(not_observed)})")
    else:
        print()
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
