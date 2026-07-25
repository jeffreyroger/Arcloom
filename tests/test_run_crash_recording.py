import sqlite3

import pytest

import pipeline.run as run_module


class _BoomError(RuntimeError):
    pass


def _make_db(tmp_path):
    path = str(tmp_path / "test.db")
    return path, lambda: sqlite3.connect(path)


@pytest.mark.asyncio
async def test_uncaught_exception_records_finished_at_and_traceback(monkeypatch, tmp_path):
    # NFR-402: an uncaught exception mid-run must still leave finished_at and
    # a traceback in run_log.errors, not a silent gap with no way to tell a
    # real crash from an external kill.
    path, connect = _make_db(tmp_path)
    monkeypatch.setattr(run_module.db, "get_connection", connect)
    monkeypatch.setattr(run_module.config, "load_feeds", lambda: [])

    async def boom(*args, **kwargs):
        raise _BoomError("simulated crash")

    monkeypatch.setattr(run_module, "ingest_once", boom)

    with pytest.raises(_BoomError):
        await run_module.run(dry_run=False)

    conn = sqlite3.connect(path)
    row = conn.execute("SELECT finished_at, errors FROM run_log ORDER BY id DESC LIMIT 1").fetchone()
    assert row[0] is not None
    assert "simulated crash" in row[1]
    assert "Traceback" in row[1]


@pytest.mark.asyncio
async def test_clean_run_still_records_finished_at(monkeypatch, tmp_path):
    path, connect = _make_db(tmp_path)
    monkeypatch.setattr(run_module.db, "get_connection", connect)
    monkeypatch.setattr(run_module.config, "load_feeds", lambda: [])

    async def no_op_ingest(conn, sources, dry_run=False):
        return {
            "feeds_attempted": 0,
            "status_200": 0,
            "status_304": 0,
            "skipped_robots": 0,
            "failures": 0,
            "degraded": 0,
            "entries_seen": 0,
            "articles_inserted": 0,
            "duplicates_skipped": 0,
            "robots_cache_hits": 0,
            "robots_fetches": 0,
        }, []

    monkeypatch.setattr(run_module, "ingest_once", no_op_ingest)

    result = await run_module.run(dry_run=False)

    assert result == 0
    conn = sqlite3.connect(path)
    row = conn.execute("SELECT finished_at, errors FROM run_log ORDER BY id DESC LIMIT 1").fetchone()
    assert row[0] is not None
    assert row[1] is None
