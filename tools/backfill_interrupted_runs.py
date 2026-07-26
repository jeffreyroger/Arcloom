"""NFR-402: one-time backfill for historical run_log gaps predating the
KeyboardInterrupt/SystemExit fix in pipeline/run.py.

IMPORTANT LIMITATION: logs/pipeline.log carries no per-line timestamps, and a
process killed before its stdout buffer flushes leaves *zero* trace there
(not even its first print) — so a row with no matching evidence cannot be
proven to be a dev-time interrupt. This script only backfills rows that sit
in a tight timing cluster with a neighboring run (< CLUSTER_WINDOW_S apart),
the one objective signal that distinguishes a manual rapid-retry session
(consistent with the ^C markers actually present in the log) from the clean,
isolated 15-minute unattended cadence. Anything else is left untouched and
reported loudly — it is a real, unexplained autonomous failure, not a
development artifact.
"""

import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import db  # noqa: E402

CLUSTER_WINDOW_S = 180


def _parse(ts: str) -> datetime:
    return datetime.strptime(ts, "%Y-%m-%dT%H:%M:%SZ")


def main() -> int:
    conn = db.get_connection()
    rows = conn.execute(
        "SELECT id, started_at, finished_at, errors FROM run_log ORDER BY id"
    ).fetchall()

    backfilled = []
    unexplained = []

    for i, (run_id, started_at, finished_at, errors) in enumerate(rows):
        if finished_at is not None or errors is not None:
            continue

        t = _parse(started_at)
        prev_delta = (t - _parse(rows[i - 1][1])).total_seconds() if i > 0 else None
        next_delta = (_parse(rows[i + 1][1]) - t).total_seconds() if i + 1 < len(rows) else None
        clustered = (prev_delta is not None and prev_delta < CLUSTER_WINDOW_S) or (
            next_delta is not None and next_delta < CLUSTER_WINDOW_S
        )

        if clustered:
            conn.execute(
                "UPDATE run_log SET errors=? WHERE id=? AND finished_at IS NULL AND errors IS NULL",
                ("interrupted: historical (backfilled from SIGINT log)", run_id),
            )
            backfilled.append(run_id)
        else:
            unexplained.append(run_id)

    conn.commit()
    conn.close()

    print(f"Backfilled (timing-cluster match, consistent with a dev-time interrupt): {len(backfilled)}")
    print(f"  ids: {backfilled}")
    print()
    print(f"Unexplained (isolated on the unattended schedule, no interrupt evidence): {len(unexplained)}")
    print(f"  ids: {unexplained}")
    if unexplained:
        print()
        print(
            "These are NOT development interrupts. They sit on the clean 15-minute "
            "scheduled cadence with no ^C or traceback anywhere in the log. This is "
            "an unresolved autonomous failure and needs its own investigation — do "
            "not treat it as explained by this backfill."
        )

    return 0


if __name__ == "__main__":
    sys.exit(main())
