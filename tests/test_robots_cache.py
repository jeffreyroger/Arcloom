import sqlite3
from datetime import datetime, timedelta, timezone

import pytest

import config
import db
from pipeline.ingest import _robots_allowed

ROBOTS_BODY = "User-agent: *\nAllow: /\n"


def _make_conn():
    conn = sqlite3.connect(":memory:")
    db.init_schema(conn)
    return conn


def _iso(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def _fetch_counter(monkeypatch, body=ROBOTS_BODY):
    calls = {"n": 0}

    async def fake_fetch(client, scheme, host):
        calls["n"] += 1
        return body

    monkeypatch.setattr("pipeline.ingest._fetch_robots_body", fake_fetch)
    return calls


@pytest.mark.asyncio
async def test_fresh_host_triggers_a_fetch(monkeypatch):
    conn = _make_conn()
    calls = _fetch_counter(monkeypatch)

    allowed, source = await _robots_allowed(conn, None, "https://example.com/feed.xml", {}, {})

    assert allowed is True
    assert source == "fetch"
    assert calls["n"] == 1
    row = conn.execute("SELECT body FROM robots_cache WHERE host=?", ("example.com",)).fetchone()
    assert row is not None


@pytest.mark.asyncio
async def test_cached_host_within_ttl_does_not_trigger_a_fetch(monkeypatch):
    conn = _make_conn()
    fresh = _iso(datetime.now(timezone.utc) - timedelta(hours=1))
    conn.execute(
        "INSERT INTO robots_cache (host, body, fetched_at) VALUES (?, ?, ?)",
        ("example.com", ROBOTS_BODY, fresh),
    )
    conn.commit()
    calls = _fetch_counter(monkeypatch)

    allowed, source = await _robots_allowed(conn, None, "https://example.com/feed.xml", {}, {})

    assert allowed is True
    assert source == "cache"
    assert calls["n"] == 0


@pytest.mark.asyncio
async def test_expired_cache_entry_triggers_a_refetch(monkeypatch):
    conn = _make_conn()
    stale = _iso(datetime.now(timezone.utc) - timedelta(hours=config.ROBOTS_CACHE_TTL_H + 1))
    conn.execute(
        "INSERT INTO robots_cache (host, body, fetched_at) VALUES (?, ?, ?)",
        ("example.com", ROBOTS_BODY, stale),
    )
    conn.commit()
    calls = _fetch_counter(monkeypatch)

    allowed, source = await _robots_allowed(conn, None, "https://example.com/feed.xml", {}, {})

    assert allowed is True
    assert source == "fetch"
    assert calls["n"] == 1
