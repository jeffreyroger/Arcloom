"""One-time backfill: mark pre-existing run_log rows left with
finished_at/errors both NULL as 'abandoned: historical' (see DECISIONS.md
"Week 1 retrospective" correction, 2026-08-01, and config.ABANDONED_THRESHOLD_M).

Distinct from pipeline/run.py's _reap_abandoned_runs, which uses
'abandoned: no completion recorded' going forward and only touches rows
older than ABANDONED_THRESHOLD_M — this script exists because those 129 rows
predate the reaper and would otherwise sit there until the next run happened
to be more than the threshold newer, which for already-dead rows is already
true, but the distinct marker keeps "caught by the reaper on a live system"
separate from "known dead at the time this backfill ran" in any later query.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import db  # noqa: E402


def main() -> int:
    conn = db.get_connection()

    rows = conn.execute(
        "SELECT id, started_at FROM run_log WHERE finished_at IS NULL AND errors IS NULL"
    ).fetchall()

    for row_id, started_at in rows:
        conn.execute(
            "UPDATE run_log SET errors=?, finished_at=? WHERE id=?",
            ("abandoned: historical", started_at, row_id),
        )

    conn.commit()
    conn.close()

    print(f"Backfilled {len(rows)} row(s) as 'abandoned: historical'.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
