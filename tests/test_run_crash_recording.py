import json
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
    # NFR-402 branch 2 (genuine crash): finished_at + full traceback in
    # run_log.errors, and the run exits non-zero rather than re-raising — a
    # single failed run must not take down the 15-minute schedule for every
    # run after it.
    path, connect = _make_db(tmp_path)
    monkeypatch.setattr(run_module.db, "get_connection", connect)
    monkeypatch.setattr(run_module.config, "load_feeds", lambda: [])

    async def boom(*args, **kwargs):
        raise _BoomError("simulated crash")

    monkeypatch.setattr(run_module, "ingest_once", boom)

    result = await run_module.run(dry_run=False)

    assert result == 1
    conn = sqlite3.connect(path)
    row = conn.execute("SELECT finished_at, errors FROM run_log ORDER BY id DESC LIMIT 1").fetchone()
    assert row[0] is not None
    assert "simulated crash" in row[1]
    assert "Traceback" in row[1]


@pytest.mark.asyncio
async def test_keyboard_interrupt_records_marker_and_propagates(monkeypatch, tmp_path):
    # NFR-402 branch 1 (operator-requested stop): recorded distinctly from a
    # crash ("interrupted: <type>", never a traceback), and the exception
    # MUST still propagate — the operator asked the process to stop, so
    # swallowing this and returning normally would be wrong.
    path, connect = _make_db(tmp_path)
    monkeypatch.setattr(run_module.db, "get_connection", connect)
    monkeypatch.setattr(run_module.config, "load_feeds", lambda: [])

    async def interrupt(*args, **kwargs):
        raise KeyboardInterrupt

    monkeypatch.setattr(run_module, "ingest_once", interrupt)

    with pytest.raises(KeyboardInterrupt):
        await run_module.run(dry_run=False)

    conn = sqlite3.connect(path)
    row = conn.execute("SELECT finished_at, errors FROM run_log ORDER BY id DESC LIMIT 1").fetchone()
    assert row[0] is not None
    assert row[1] == "interrupted: KeyboardInterrupt"


@pytest.mark.asyncio
async def test_system_exit_records_marker_and_propagates(monkeypatch, tmp_path):
    # Same distinct-interrupt handling for SystemExit as for KeyboardInterrupt
    # — both are BaseException subclasses that `except Exception` would miss.
    path, connect = _make_db(tmp_path)
    monkeypatch.setattr(run_module.db, "get_connection", connect)
    monkeypatch.setattr(run_module.config, "load_feeds", lambda: [])

    async def exit_now(*args, **kwargs):
        raise SystemExit(1)

    monkeypatch.setattr(run_module, "ingest_once", exit_now)

    with pytest.raises(SystemExit):
        await run_module.run(dry_run=False)

    conn = sqlite3.connect(path)
    row = conn.execute("SELECT finished_at, errors FROM run_log ORDER BY id DESC LIMIT 1").fetchone()
    assert row[0] is not None
    assert row[1] == "interrupted: SystemExit"


@pytest.mark.asyncio
async def test_finally_does_not_clobber_an_already_recorded_crash(monkeypatch, tmp_path):
    # The finally block's "completed" write must be guarded so it can never
    # overwrite a traceback (or interrupt marker) an except branch already
    # wrote — otherwise evidence of the actual failure would be replaced by
    # a false "completed" state.
    path, connect = _make_db(tmp_path)
    monkeypatch.setattr(run_module.db, "get_connection", connect)
    monkeypatch.setattr(run_module.config, "load_feeds", lambda: [])

    async def boom(*args, **kwargs):
        raise _BoomError("simulated crash")

    monkeypatch.setattr(run_module, "ingest_once", boom)

    result = await run_module.run(dry_run=False)

    assert result == 1
    conn = sqlite3.connect(path)
    row = conn.execute("SELECT stage_counts, errors FROM run_log ORDER BY id DESC LIMIT 1").fetchone()
    # stage_counts holds only the process-context dict written at run start
    # (NFR-403) -- the crash branch must never advance it to the
    # {"process": ..., "counts": ...} shape the clean-completion path writes.
    stage_counts = json.loads(row[0])
    assert "process" in stage_counts
    assert "counts" not in stage_counts
    assert "simulated crash" in row[1]
    assert "Traceback" in row[1]
    assert row[1] != "completed"


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
