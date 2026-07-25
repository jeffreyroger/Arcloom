"""FR-201 through FR-209: week 1 core ingestion.

Async httpx.AsyncClient + asyncio.gather + a semaphore. Nothing more
elaborate — no retry/backoff library, no plugin system for feed formats, no
rate-limiter class, no abstraction over feedparser. The fail-streak counter
on `source` is the entire error-handling strategy (week 1 Do NOT list).
"""

import asyncio
import json
import sqlite3
import time
from datetime import datetime, timezone
from html.parser import HTMLParser
from urllib import robotparser
from urllib.parse import urlsplit

import feedparser
import httpx

import config
from pipeline.normalize import canonicalize_url, normalize_timestamp

FETCH_TIMEOUT_S = 10.0
SNIPPET_MAX_CHARS = 2000


# NFR-403: run_log timestamps and article fetched_at are UTC ISO8601.
def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class _TagStripper(HTMLParser):
    def __init__(self):
        super().__init__()
        self._parts: list[str] = []

    def handle_data(self, data: str) -> None:
        self._parts.append(data)

    def get_text(self) -> str:
        return "".join(self._parts)


# FR-204: feed-provided snippets may carry inline HTML from the publisher —
# strip markup and cap length. Never the article body itself.
def _clean_snippet(raw: str | None) -> str | None:
    if not raw:
        return None
    stripper = _TagStripper()
    stripper.feed(raw)
    cleaned = stripper.get_text().strip()
    if not cleaned:
        return None
    return cleaned[:SNIPPET_MAX_CHARS]


# FR-101/FR-102/FR-103: sync feeds.yaml (via config.load_feeds, which fails
# loudly on malformed entries) into the source table at startup.
def sync_sources(conn: sqlite3.Connection, feeds: list[dict]) -> None:
    for feed in feeds:
        packs_json = json.dumps(feed["packs"])
        cur = conn.execute(
            "UPDATE source SET name=?, packs=?, lang=?, weight=?, enabled=? WHERE feed_url=?",
            (feed["name"], packs_json, feed["lang"], feed["weight"], int(feed["enabled"]), feed["url"]),
        )
        if cur.rowcount == 0:
            conn.execute(
                """
                INSERT INTO source (name, feed_url, packs, lang, weight, enabled)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (feed["name"], feed["url"], packs_json, feed["lang"], feed["weight"], int(feed["enabled"])),
            )
    conn.commit()


# FR-205: per-host politeness delay, tracked per hostname, not globally.
async def _wait_for_host(host: str, last_request_at: dict, host_locks: dict) -> None:
    lock = host_locks.setdefault(host, asyncio.Lock())
    async with lock:
        last = last_request_at.get(host)
        if last is not None:
            remaining = config.PER_HOST_DELAY_S - (time.monotonic() - last)
            if remaining > 0:
                await asyncio.sleep(remaining)
        last_request_at[host] = time.monotonic()


# FR-203: fetch robots.txt for a host. Failure is treated as allowed, logged.
async def _fetch_robots(client: httpx.AsyncClient, scheme: str, host: str):
    robots_url = f"{scheme}://{host}/robots.txt"
    try:
        resp = await client.get(
            robots_url, headers={"User-Agent": config.USER_AGENT}, timeout=FETCH_TIMEOUT_S
        )
    except httpx.HTTPError as exc:
        print(f"[ingest] robots.txt fetch failed for {host}: {exc}; treating as allowed")
        return None

    if resp.status_code >= 400:
        print(f"[ingest] robots.txt fetch failed for {host}: HTTP {resp.status_code}; treating as allowed")
        return None

    rp = robotparser.RobotFileParser()
    rp.parse(resp.text.splitlines())
    return rp


# FR-203: robots.txt checked before every feed fetch, cached per host with a
# ROBOTS_CACHE_TTL_H TTL.
async def _robots_allowed(
    client: httpx.AsyncClient, url: str, robots_cache: dict, robots_locks: dict
) -> bool:
    parts = urlsplit(url)
    host = parts.hostname or ""
    lock = robots_locks.setdefault(host, asyncio.Lock())

    async with lock:
        cached = robots_cache.get(host)
        now = time.monotonic()
        ttl_s = config.ROBOTS_CACHE_TTL_H * 3600
        if cached is None or now - cached[1] > ttl_s:
            rp = await _fetch_robots(client, parts.scheme, host)
            robots_cache[host] = (rp, now)
        else:
            rp = cached[0]

    if rp is None:
        return True
    return rp.can_fetch(config.USER_AGENT, url)


# FR-201/FR-202/FR-203/FR-205: conditional, robots-aware, polite feed fetch.
# Returns a plain result dict; never raises for network-level failures so a
# single feed can never abort the run (NFR-401).
async def _fetch_one(
    client: httpx.AsyncClient,
    source_row: sqlite3.Row,
    semaphore: asyncio.Semaphore,
    last_request_at: dict,
    host_locks: dict,
    robots_cache: dict,
    robots_locks: dict,
) -> dict:
    feed_url = source_row["feed_url"]
    source_id = source_row["id"]
    host = urlsplit(feed_url).hostname or ""

    await _wait_for_host(host, last_request_at, host_locks)

    async with semaphore:
        if not await _robots_allowed(client, feed_url, robots_cache, robots_locks):
            return {"source_id": source_id, "status": "skipped_robots"}

        headers = {"User-Agent": config.USER_AGENT}
        if source_row["etag"]:
            headers["If-None-Match"] = source_row["etag"]
        if source_row["last_modified"]:
            headers["If-Modified-Since"] = source_row["last_modified"]

        try:
            resp = await client.get(feed_url, headers=headers, timeout=FETCH_TIMEOUT_S)
        except httpx.HTTPError as exc:
            return {"source_id": source_id, "status": "error", "error": str(exc)}

    if resp.status_code == 304:
        # FR-201: 304 is recorded; zero further work — no parse, no insert.
        return {"source_id": source_id, "status": "not_modified"}

    if resp.status_code != 200:
        return {"source_id": source_id, "status": "error", "error": f"HTTP {resp.status_code}"}

    return {
        "source_id": source_id,
        "status": "ok",
        "etag": resp.headers.get("ETag"),
        "last_modified": resp.headers.get("Last-Modified"),
        "body": resp.content,
    }


# FR-204/FR-206/FR-207/FR-208: canonicalize URL, normalize timestamp, extract
# only the permitted fields, insert-or-ignore on canonical_url. In dry-run
# mode this only reads (SELECT by canonical_url) to classify would-insert
# vs. duplicate — it issues no INSERT.
def _process_entries(
    conn: sqlite3.Connection,
    source_id,
    entries: list,
    fetched_at_dt: datetime,
    dry_run: bool = False,
) -> tuple[int, int]:
    fetched_at_iso = fetched_at_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    inserted = 0
    duplicates = 0

    for entry in entries:
        raw_url = entry.get("link")
        title = (entry.get("title") or "").strip()
        if not raw_url or not title:
            continue

        canonical_url = canonicalize_url(raw_url)

        if dry_run:
            exists = conn.execute(
                "SELECT 1 FROM article WHERE canonical_url = ? LIMIT 1", (canonical_url,)
            ).fetchone()
            if exists:
                duplicates += 1
            else:
                inserted += 1
            continue

        published_at, inferred = normalize_timestamp(entry, fetched_at_dt)
        snippet = _clean_snippet(entry.get("summary"))
        author = entry.get("author")

        cur = conn.execute(
            """
            INSERT OR IGNORE INTO article
                (source_id, canonical_url, title, snippet, author,
                 published_at, fetched_at, timestamp_inferred)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (source_id, canonical_url, title, snippet, author, published_at, fetched_at_iso, int(inferred)),
        )
        if cur.rowcount:
            inserted += 1
        else:
            duplicates += 1

    return inserted, duplicates


# FR-104/NFR-401: fail-streak counter is the entire error strategy. Returns
# True if this call just pushed the source into `degraded`.
def _mark_source_result(
    conn: sqlite3.Connection,
    source_id: int,
    ok: bool,
    etag: str | None = None,
    last_modified: str | None = None,
) -> bool:
    if ok:
        conn.execute(
            """
            UPDATE source
            SET fail_streak = 0, last_ok_at = ?, status = 'ok',
                etag = COALESCE(?, etag), last_modified = COALESCE(?, last_modified)
            WHERE id = ?
            """,
            (_now_iso(), etag, last_modified, source_id),
        )
        return False

    conn.execute("UPDATE source SET fail_streak = fail_streak + 1 WHERE id = ?", (source_id,))
    row = conn.execute("SELECT fail_streak FROM source WHERE id = ?", (source_id,)).fetchone()
    if row is not None and row[0] >= config.FAIL_STREAK_LIMIT:
        conn.execute("UPDATE source SET status = 'degraded' WHERE id = ?", (source_id,))
        return True
    return False


# FR-201 through FR-209: fetch and process one batch of sources concurrently.
# Idempotent (FR-209): running this twice over the same feed state inserts
# zero new rows the second time. This function owns no run_log or top-level
# commit/exit-code concerns — that belongs to pipeline/run.py, the single
# pipeline entry point. In dry-run mode, no `source` or `article` row is
# written; sources must already carry whatever etag/last_modified the
# caller wants used for the conditional GET.
async def ingest_once(
    conn: sqlite3.Connection, sources: list[dict], dry_run: bool = False
) -> tuple[dict, list[str]]:
    semaphore = asyncio.Semaphore(config.MAX_CONCURRENCY)
    last_request_at: dict = {}
    host_locks: dict = {}
    robots_cache: dict = {}
    robots_locks: dict = {}

    async with httpx.AsyncClient(follow_redirects=True) as client:
        results = await asyncio.gather(
            *(
                _fetch_one(client, src, semaphore, last_request_at, host_locks, robots_cache, robots_locks)
                for src in sources
            )
        )

    fetched_at_dt = datetime.now(timezone.utc)
    counts = {
        "feeds_attempted": len(sources),
        "status_200": 0,
        "status_304": 0,
        "skipped_robots": 0,
        "failures": 0,
        "degraded": 0,
        "entries_seen": 0,
        "articles_inserted": 0,
        "duplicates_skipped": 0,
    }
    errors: list[str] = []

    for result, src in zip(results, sources):
        source_id = src["id"]
        status = result["status"]

        if status == "ok":
            try:
                parsed = feedparser.parse(result["body"])
                counts["entries_seen"] += len(parsed.entries)
                inserted, duplicates = _process_entries(
                    conn, source_id, parsed.entries, fetched_at_dt, dry_run=dry_run
                )
                counts["articles_inserted"] += inserted
                counts["duplicates_skipped"] += duplicates
                counts["status_200"] += 1
                if not dry_run:
                    _mark_source_result(
                        conn, source_id, ok=True, etag=result.get("etag"), last_modified=result.get("last_modified")
                    )
            except Exception as exc:  # NFR-401: a single feed must never abort the run
                counts["failures"] += 1
                errors.append(f"source {source_id}: processing error: {exc}")
                if not dry_run and _mark_source_result(conn, source_id, ok=False):
                    counts["degraded"] += 1
        elif status == "not_modified":
            counts["status_304"] += 1
            if not dry_run:
                _mark_source_result(conn, source_id, ok=True)
        elif status == "skipped_robots":
            counts["skipped_robots"] += 1
        else:
            counts["failures"] += 1
            errors.append(f"source {source_id}: {result.get('error')}")
            if not dry_run and _mark_source_result(conn, source_id, ok=False):
                counts["degraded"] += 1

    return counts, errors
