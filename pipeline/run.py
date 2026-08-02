"""Single pipeline entry point.

Wraps pipeline.ingest: owns the run_log lifecycle, logs per-stage counts
and durations (NFR-403), and defines the only two conditions that exit
non-zero — database unwritable, config unreadable. An individual feed
failing is captured in stage_counts and never affects the exit code
(NFR-401).

Safely re-runnable after a crash (NFR-402): the run_log start row and the
feeds.yaml -> source sync each commit immediately as they happen, so
evidence of an attempted run survives even if the rest doesn't. The bulk of
a run — article inserts and per-source status updates — lands in one
transaction at the end; a crash before that commit leaves the database
exactly as it was before this run started, so simply re-running is safe.
"""

import os
import sys


# The scheduled task runs pythonw.exe, which has no console -- sys.stdout
# and sys.stderr are None there, not merely redirected. Our own logging uses
# a FileHandler and is unaffected, but third-party libraries are not: week 2
# adds sentence-transformers, which drives tqdm progress bars that write to
# sys.stderr unconditionally. Writing to None raises AttributeError, which
# would present as a run dying silently during embedding -- the same
# signature as the week 1 kill defect this guards against. Must run before
# any import that might print (hence the split from the import block below,
# and the os/sys-only imports ahead of it).
def _guard_pythonw_streams() -> None:
    if sys.stdout is None:
        sys.stdout = open(os.devnull, "w")
    if sys.stderr is None:
        sys.stderr = open(os.devnull, "w")
    os.environ.setdefault("HF_HUB_DISABLE_PROGRESS_BARS", "1")
    os.environ.setdefault("TQDM_DISABLE", "1")


_guard_pythonw_streams()

import argparse
import asyncio
import ctypes
import json
import logging
import sqlite3
import subprocess
import time
import traceback
from datetime import datetime, timedelta, timezone
from pathlib import Path

import config
import db
from pipeline.embed import embed_pending
from pipeline.ingest import ingest_once, sync_sources
from pipeline.simhash import apply_bypass


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# NFR-403: run_log.started_at needs enough resolution to distinguish two
# runs a scheduler fired seconds apart, and to line up against sub-second
# OS event-log timestamps (Kernel-Power, Task Scheduler) during diagnosis.
def _now_iso_precise() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


# NFR-402: output must reach disk immediately regardless of Python's stdout
# buffering and survive a kill that bypasses exception handling entirely
# (e.g. an OS console-control event) -- see DECISIONS.md "Week 1
# retrospective". A FileHandler's emit() flushes after every record, so no
# in-process buffer stands between a log call and the bytes on disk. Every
# record carries run_id (via _RunContext, updated once the run_log row
# exists) and stage (passed explicitly per call site) so a killed run can be
# correlated against its run_log row after the fact.
class _RunContextFilter(logging.Filter):
    def __init__(self) -> None:
        super().__init__()
        self.run_id: int | str = "-"

    def filter(self, record: logging.LogRecord) -> bool:
        if not hasattr(record, "stage"):
            record.stage = "-"
        record.run_id = self.run_id
        return True


_run_context = _RunContextFilter()


def _configure_logging() -> logging.Logger:
    log_path = Path("logs/pipeline.log")
    log_path.parent.mkdir(parents=True, exist_ok=True)

    handler = logging.FileHandler(log_path, encoding="utf-8")
    handler.addFilter(_run_context)
    formatter = logging.Formatter(
        fmt="%(asctime)sZ pid=%(process)d run_id=%(run_id)s stage=%(stage)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )
    formatter.converter = time.gmtime
    handler.setFormatter(formatter)

    logger = logging.getLogger("arcloom")
    logger.setLevel(logging.INFO)
    logger.addHandler(handler)
    logger.propagate = False
    return logger


log = _configure_logging()


def _log(stage: str, msg: str, level: int = logging.INFO) -> None:
    log.log(level, msg, extra={"stage": stage})


# NFR-403: 128 of week 1's run_log rows were left with finished_at/errors
# both NULL -- a run started and vanished, with no record of how far it
# got. _mark_stage() writes the current stage to run_log.last_stage with an
# immediate commit, so a kill between two calls leaves the row showing
# exactly the last stage it completed, and logs the wall-clock elapsed time
# at that boundary so a consistent elapsed value across kills would point
# at an undiscovered timeout. Deliberately does NOT cover a "robots_checked"
# boundary distinct from fetch: pipeline.ingest checks robots.txt and fetches
# per-source, interleaved and concurrent (FR-205 per-host politeness), so
# there is no global "all robots checked" moment before fetching starts
# without restructuring that concurrency model -- out of scope for
# instrumentation-only work.
def _mark_stage(conn: sqlite3.Connection, run_id, stage: str, t_start: float) -> None:
    elapsed = time.perf_counter() - t_start
    _log(stage, f"elapsed={elapsed:.3f}s")
    if run_id is not None:
        conn.execute("UPDATE run_log SET last_stage=? WHERE id=?", (stage, run_id))
        conn.commit()


# NFR-403: cheap process-identity context recorded once at run start so a
# killed run's PID, interpreter, and console state can be queried instead of
# re-derived. Distinguishing python.exe (allocates a console -- a target for
# CTRL_C_EVENT/etc, see DECISIONS.md "Week 1 retrospective") from
# pythonw.exe (no console) is the whole point of the console_attached check.
def _process_context() -> dict:
    console_attached = None
    try:
        console_attached = bool(ctypes.windll.kernel32.GetConsoleWindow())
    except (AttributeError, OSError):
        console_attached = None  # non-Windows: ctypes.windll doesn't exist
    return {
        "pid": os.getpid(),
        "interpreter": Path(sys.executable).name if sys.executable else "unknown",
        "console_attached": console_attached,
        "last_resume_at": _last_resume_iso(),
    }


# NFR-403: this machine has no S1/S2/S3 sleep, only Modern Standby (S0 Low
# Power Idle) -- see DECISIONS.md "Week 1 retrospective" correction,
# 2026-08-01. The last Kernel-Power resume/Modern-Standby-exit event
# (Id 107 or 507) recorded before this run started is cheap to fetch and
# turns "did this run overlap a standby transition" into a query against
# run_log instead of a manual event-log lookup after the fact. Best-effort:
# any failure (non-Windows, event log unavailable, timeout) yields None
# rather than affecting the run.
def _last_resume_iso() -> str | None:
    try:
        result = subprocess.run(
            [
                "powershell",
                "-NoProfile",
                "-Command",
                "(Get-WinEvent -FilterHashtable @{LogName='System';"
                "ProviderName='Microsoft-Windows-Kernel-Power';Id=107,507} "
                "-MaxEvents 1).TimeCreated.ToString('o')",
            ],
            capture_output=True,
            text=True,
            timeout=5,
        )
        value = result.stdout.strip()
        return value or None
    except Exception:
        return None


# Gap: no SRS requirement ID covers abandoned-run detection (see
# config.ABANDONED_THRESHOLD_M comment and DECISIONS.md "Week 1
# retrospective" correction, 2026-08-01). A row with finished_at/errors both
# NULL and started_at older than the threshold cannot still be a live run —
# converts an unknown into a known-abandoned state instead of leaving it
# permanently ambiguous. Runs before any other work in run() (still needs a
# DB connection, so cannot run before that) so it never touches the current
# run's own row, which is inserted afterward with a fresh started_at.
def _reap_abandoned_runs(conn: sqlite3.Connection) -> int:
    cutoff = (
        datetime.now(timezone.utc) - timedelta(minutes=config.ABANDONED_THRESHOLD_M)
    ).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    result = conn.execute(
        "UPDATE run_log SET errors=?, finished_at=started_at "
        "WHERE finished_at IS NULL AND errors IS NULL AND started_at < ?",
        ("abandoned: no completion recorded", cutoff),
    )
    return result.rowcount


# Gap: no SRS requirement ID covers operational-table retention (see
# config.RUN_LOG_RETENTION_D comment and DECISIONS.md). Deletes run_log rows
# whose started_at is older than the retention window; returns rows deleted.
def _prune_run_log(conn: sqlite3.Connection) -> int:
    cutoff = (datetime.now(timezone.utc) - timedelta(days=config.RUN_LOG_RETENTION_D)).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )
    return conn.execute("DELETE FROM run_log WHERE started_at < ?", (cutoff,)).rowcount


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


async def run(dry_run: bool = False, feeds_path=None, db_path=None) -> int:
    t_start = time.perf_counter()

    # NFR-403: the very first action, before config load and before any
    # network call. If an incomplete run_log row ever shows last_stage
    # exactly 'started' with nothing after, the process died essentially at
    # launch -- ruling out anything feed/network-related and pointing at the
    # OS or the scheduler instead. This log line lands even if the DB is the
    # thing that's broken.
    _log("started", "run() entered")

    # Total failure #1: database unwritable. Checked (and the run_log
    # 'started' row written) before config load, per the ordering above --
    # a config error must not prevent the startup marker from existing.
    run_id = None
    conn = None
    try:
        conn = db.get_connection(db_path) if db_path else db.get_connection()
        conn.row_factory = sqlite3.Row
        db.init_schema(conn)

        if not dry_run:
            reaped = _reap_abandoned_runs(conn)
            conn.commit()
            if reaped:
                _log("reaper", f"marked {reaped} abandoned run_log row(s) (no completion within {config.ABANDONED_THRESHOLD_M}m)")

            run_id = conn.execute(
                "INSERT INTO run_log (started_at, last_stage) VALUES (?, ?)",
                (_now_iso_precise(), "started"),
            ).lastrowid
            conn.commit()
            _run_context.run_id = run_id

            process_ctx = _process_context()
            conn.execute(
                "UPDATE run_log SET stage_counts=? WHERE id=?",
                (json.dumps({"process": process_ctx}), run_id),
            )
            conn.commit()
            _log("started", f"process={process_ctx}")
    except Exception as exc:
        _log("db_init", f"FATAL: database unwritable: {exc}", level=logging.ERROR)
        return 1

    # Total failure #2: config unreadable. The run_log row already exists
    # (last_stage='started') at this point, so a config failure is now
    # recorded with a reason instead of vanishing without a trace.
    try:
        feeds = config.load_feeds(feeds_path) if feeds_path else config.load_feeds()
    except Exception as exc:
        _log("config_loaded", f"FATAL: could not load feeds.yaml: {exc}", level=logging.ERROR)
        if not dry_run and run_id is not None:
            conn.execute(
                "UPDATE run_log SET finished_at=?, errors=? WHERE id=? AND errors IS NULL",
                (_now_iso(), f"FATAL: could not load feeds.yaml: {exc}", run_id),
            )
            conn.commit()
        conn.close()
        return 1
    _mark_stage(conn, run_id, "config_loaded", t_start)
    _log("config_loaded", f"{len(feeds)} feeds")

    counts = None
    errors: list[str] = []
    run_ok = False
    try:
        # sync_sources/_prune_run_log live inside this try (not the DB-connect
        # try/except above) so a failure here goes through the same
        # crash/interrupt handling as everything else below, instead of
        # being mislabeled "database unwritable".
        if dry_run:
            sources = _dry_run_sources(conn, feeds)
        else:
            sync_sources(conn, feeds)
            _mark_stage(conn, run_id, "sources_synced", t_start)
            _log("sources_synced", f"{len(feeds)} feeds synced")

            pruned = _prune_run_log(conn)
            conn.commit()
            _mark_stage(conn, run_id, "pruned", t_start)
            if pruned:
                _log("pruned", f"pruned {pruned} run_log row(s) older than {config.RUN_LOG_RETENTION_D}d")

            rows = conn.execute(
                "SELECT id, feed_url, etag, last_modified FROM source WHERE enabled = 1"
            ).fetchall()
            sources = [dict(row) for row in rows]

        _mark_stage(conn, run_id, "fetch_started", t_start)
        t_stage = time.perf_counter()
        counts, errors = await ingest_once(conn, sources, dry_run=dry_run)
        fetch_elapsed = time.perf_counter() - t_stage
        _mark_stage(conn, run_id, "fetch_complete", t_start)
        _log(
            "fetch_complete",
            f"{counts['feeds_attempted']} attempted, "
            f"{counts['status_200']} x200, {counts['status_304']} x304, "
            f"{counts['skipped_robots']} skipped(robots), {counts['failures']} failed "
            f"({fetch_elapsed:.2f}s)",
        )
        _log(
            "fetch_complete",
            f"articles: {counts['entries_seen']} seen, "
            f"{counts['articles_inserted']} inserted, {counts['duplicates_skipped']} duplicates",
        )
        _log(
            "fetch_complete",
            f"robots.txt: {counts['robots_cache_hits']} cache hits, "
            f"{counts['robots_fetches']} fetches",
        )

        if dry_run:
            conn.rollback()
        else:
            conn.commit()
            _mark_stage(conn, run_id, "articles_written", t_start)

            # FR-301 through FR-303: simhash + syndication bypass runs before
            # embedding so a near-duplicate's copied embedding already
            # satisfies embed_pending's pending-set query below, instead of
            # being embedded twice.
            bypass_stats = apply_bypass(conn)
            _mark_stage(conn, run_id, "simhash", t_start)
            _log(
                "simhash",
                f"computed={bypass_stats['computed']} candidates={bypass_stats['candidates']} "
                f"bypassed={bypass_stats['bypassed']} rate={bypass_stats['rate']:.3f}",
            )

            # FR-401 through FR-405: incremental embedding. Lazy-loaded (see
            # pipeline/embed.py) so an incremental run with nothing pending
            # doesn't pay the ~2-5s model load; logged either way so run
            # duration variance is explainable.
            embed_stats = embed_pending(conn)
            _mark_stage(conn, run_id, "embedded", t_start)
            _log(
                "embedded",
                f"{embed_stats['embedded']} of {embed_stats['pending']} pending, "
                f"model_loaded={embed_stats['model_loaded']}",
            )
        run_ok = True
        _mark_stage(conn, run_id, "complete", t_start)

    # NFR-402: an operator-requested stop (Ctrl+C, or SystemExit from e.g. a
    # signal handler). Recorded distinctly from a crash — "interrupted:
    # <type>", never a traceback — and then re-raised: the operator asked the
    # process to stop, so we must actually let it stop, not swallow this and
    # return as if everything were fine.
    except (KeyboardInterrupt, SystemExit) as exc:
        _log("interrupt", f"INTERRUPTED: {type(exc).__name__}", level=logging.WARNING)
        if not dry_run and run_id is not None:
            conn.execute(
                "UPDATE run_log SET finished_at=?, errors=? WHERE id=? AND errors IS NULL",
                (_now_iso(), f"interrupted: {type(exc).__name__}", run_id),
            )
            conn.commit()
        raise

    # NFR-402: a genuine crash. Record the traceback so it reads differently
    # from both a clean run and an operator interrupt in run_log, then exit
    # non-zero (visible in Task Scheduler's LastTaskResult) rather than
    # re-raising — one failed run must not take down the 15-minute schedule
    # for every run after it.
    except Exception:
        _log("crash", f"CRASH:\n{traceback.format_exc()}", level=logging.ERROR)
        if not dry_run and run_id is not None:
            conn.execute(
                "UPDATE run_log SET finished_at=?, errors=? WHERE id=? AND errors IS NULL",
                (_now_iso(), traceback.format_exc(), run_id),
            )
            conn.commit()
        return 1

    finally:
        # Only the clean-completion path (run_ok) writes here. The
        # "AND errors IS NULL" guard is what keeps this from ever
        # overwriting a traceback or interrupt marker already written by
        # either except branch above — both run before this, in all cases
        # (return, raise, or falling through). stage_counts is merged with
        # the process-context dict written at run start (see above) rather
        # than replacing it, so a completed run's row keeps both.
        if run_ok and not dry_run and run_id is not None:
            existing = conn.execute(
                "SELECT stage_counts FROM run_log WHERE id=?", (run_id,)
            ).fetchone()
            process_ctx = json.loads(existing[0])["process"] if existing and existing[0] else None
            conn.execute(
                "UPDATE run_log SET finished_at=?, stage_counts=?, errors=? WHERE id=? AND errors IS NULL",
                (
                    _now_iso(),
                    json.dumps({"process": process_ctx, "counts": counts}),
                    json.dumps(errors) if errors else None,
                    run_id,
                ),
            )
            conn.commit()
        conn.close()

    total_elapsed = time.perf_counter() - t_start
    suffix = " (dry-run, nothing written)" if dry_run else ""
    _log("total", f"TOTAL: {total_elapsed:.2f}s{suffix}")

    if errors:
        _log("total", f"{len(errors)} error(s):", level=logging.WARNING)
        for line in errors:
            _log("total", f"  - {line}", level=logging.WARNING)

    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Arc Loom pipeline entry point")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch and parse feeds but write nothing to the database",
    )
    # FR-101: config-driven feeds — testing affordance so tools/mock_feed_server.py
    # (feeds.mock.yaml) can be exercised without touching the real feeds.yaml.
    parser.add_argument(
        "--feeds",
        type=Path,
        default=None,
        help="Alternate feeds config path (default: feeds.yaml)",
    )
    # tools/accelerated_soak.py: run against a separate soak.db so soak testing
    # never touches production data.
    parser.add_argument(
        "--db",
        type=Path,
        default=None,
        help="Alternate database path (default: arcloom.db)",
    )
    args = parser.parse_args()
    return asyncio.run(run(dry_run=args.dry_run, feeds_path=args.feeds, db_path=args.db))


if __name__ == "__main__":
    sys.exit(main())
