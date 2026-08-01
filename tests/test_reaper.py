import sqlite3
from datetime import datetime, timedelta, timezone

import config
import db
import pipeline.run as run_module


def _iso(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _make_conn(tmp_path):
    conn = sqlite3.connect(str(tmp_path / "test.db"))
    db.init_schema(conn)
    return conn


def _insert(conn, started_at, finished_at=None, errors=None):
    cur = conn.execute(
        "INSERT INTO run_log (started_at, finished_at, errors) VALUES (?, ?, ?)",
        (started_at, finished_at, errors),
    )
    conn.commit()
    return cur.lastrowid


def test_stale_incomplete_row_gets_marked(tmp_path):
    conn = _make_conn(tmp_path)
    stale_at = _iso(datetime.now(timezone.utc) - timedelta(minutes=config.ABANDONED_THRESHOLD_M + 5))
    row_id = _insert(conn, stale_at)

    reaped = run_module._reap_abandoned_runs(conn)
    conn.commit()

    assert reaped == 1
    row = conn.execute("SELECT finished_at, errors FROM run_log WHERE id=?", (row_id,)).fetchone()
    assert row[0] == stale_at
    assert row[1] == "abandoned: no completion recorded"


def test_fresh_incomplete_row_not_marked(tmp_path):
    conn = _make_conn(tmp_path)
    fresh_at = _iso(datetime.now(timezone.utc) - timedelta(minutes=config.ABANDONED_THRESHOLD_M - 5))
    row_id = _insert(conn, fresh_at)

    reaped = run_module._reap_abandoned_runs(conn)
    conn.commit()

    assert reaped == 0
    row = conn.execute("SELECT finished_at, errors FROM run_log WHERE id=?", (row_id,)).fetchone()
    assert row[0] is None
    assert row[1] is None


def test_already_errored_row_untouched(tmp_path):
    conn = _make_conn(tmp_path)
    stale_at = _iso(datetime.now(timezone.utc) - timedelta(minutes=config.ABANDONED_THRESHOLD_M + 5))
    row_id = _insert(conn, stale_at, errors="interrupted: KeyboardInterrupt")

    reaped = run_module._reap_abandoned_runs(conn)
    conn.commit()

    assert reaped == 0
    row = conn.execute("SELECT finished_at, errors FROM run_log WHERE id=?", (row_id,)).fetchone()
    assert row[0] is None
    assert row[1] == "interrupted: KeyboardInterrupt"


def test_completed_row_untouched(tmp_path):
    conn = _make_conn(tmp_path)
    stale_at = datetime.now(timezone.utc) - timedelta(minutes=config.ABANDONED_THRESHOLD_M + 5)
    finished_at = _iso(stale_at + timedelta(seconds=5))
    row_id = _insert(conn, _iso(stale_at), finished_at=finished_at)

    reaped = run_module._reap_abandoned_runs(conn)
    conn.commit()

    assert reaped == 0
    row = conn.execute("SELECT finished_at, errors FROM run_log WHERE id=?", (row_id,)).fetchone()
    assert row[0] == finished_at
    assert row[1] is None
