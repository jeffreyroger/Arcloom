"""Thin sqlite3 helpers. No ORM (banned per CLAUDE.md)."""

import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path

SCHEMA_PATH = Path(__file__).parent / "schema.sql"
DB_PATH = Path(__file__).parent / "arcloom.db"


def get_connection(db_path: Path = DB_PATH) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def init_schema(conn: sqlite3.Connection) -> None:
    """Idempotent: safe to call on every run (CREATE TABLE IF NOT EXISTS)."""
    with open(SCHEMA_PATH, "r", encoding="utf-8") as f:
        conn.executescript(f.read())
    _migrate(conn)
    conn.commit()


# CREATE TABLE IF NOT EXISTS only helps brand-new databases; a column added
# to schema.sql after a database already exists needs an explicit,
# idempotent ALTER TABLE here (SQLite has no "ADD COLUMN IF NOT EXISTS").
def _migrate(conn: sqlite3.Connection) -> None:
    cols = {row[1] for row in conn.execute("PRAGMA table_info(source)")}
    if "disabled_reason" not in cols:
        conn.execute("ALTER TABLE source ADD COLUMN disabled_reason TEXT")
    if "first_seen_at" not in cols:
        conn.execute("ALTER TABLE source ADD COLUMN first_seen_at TEXT")
        _backfill_first_seen_at(conn)

    run_log_cols = {row[1] for row in conn.execute("PRAGMA table_info(run_log)")}
    if "last_stage" not in run_log_cols:
        conn.execute("ALTER TABLE run_log ADD COLUMN last_stage TEXT")


# Rows that predate first_seen_at have no ground truth for "when did we
# start trying to fetch this." Best available approximation, in order:
# this source's own earliest article (MIN(article.fetched_at)) if it ever
# produced one, else the earliest run_log.started_at in the whole database
# (i.e. "at least since this pipeline first ran") -- an honest lower bound,
# not a guess. tools/validate_feeds.py --longitudinal reports this
# approximation openly rather than silently treating it as exact.
def _backfill_first_seen_at(conn: sqlite3.Connection) -> None:
    fallback = conn.execute("SELECT MIN(started_at) FROM run_log").fetchone()[0]
    fallback = fallback or datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    conn.execute(
        """
        UPDATE source
        SET first_seen_at = COALESCE(
            (SELECT MIN(fetched_at) FROM article WHERE article.source_id = source.id),
            ?
        )
        WHERE first_seen_at IS NULL
        """,
        (fallback,),
    )


@contextmanager
def transaction(conn: sqlite3.Connection):
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
