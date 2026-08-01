"""FR-201 through FR-209: week 1 core ingestion.

Async httpx.AsyncClient + asyncio.gather + a semaphore. Nothing more
elaborate — no retry/backoff library, no plugin system for feed formats, no
rate-limiter class, no abstraction over feedparser. The fail-streak counter
on `source` is the entire error-handling strategy (week 1 Do NOT list).
"""

import asyncio
import json
import logging
import sqlite3
import time
from datetime import datetime, timedelta, timezone
from urllib import robotparser
from urllib.parse import urlsplit

import feedparser
import httpx

import config
from pipeline.normalize import canonicalize_url, derive_snippet, normalize_timestamp

# NFR-402: propagates to the "arcloom" logger's FileHandler configured in
# pipeline/run.py, so these lines carry the same run_id/pid/stage framing.
_log = logging.getLogger("arcloom.ingest")


# NFR-403: run_log timestamps and article fetched_at are UTC ISO8601.
def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# FR-101/FR-102/FR-103: sync feeds.yaml (via config.load_feeds, which fails
# loudly on malformed entries) into the source table at startup.
#
# FR-101/AC-7: removal from feeds.yaml MUST NOT require a code change to stop
# ingestion of that feed. Rows are reconciled in both directions: feeds new
# to the file are inserted, feeds present in both are updated, and feeds no
# longer in the file are marked status='disabled', enabled=0 — never
# deleted, so past articles keep a valid source_id and run_log stays
# coherent. `disabled` (removed from config, or kept with enabled: false and
# a disabled_reason, both intentional/config-driven) is distinct from
# `degraded` (failing repeatedly, unintentional) and from `blocked`
# (robots.txt disallows it specifically -- also config-driven once recorded
# via disabled_reason, but labeled separately since the fetch attempt would
# be legal for other reasons, just refused by the publisher). A feed
# re-enabled in the file has its disabled/blocked status optimistically
# cleared back to 'ok'; the next real fetch attempt corrects it either way.
# A degraded status is left alone since that reflects observed failures,
# not config.
def sync_sources(conn: sqlite3.Connection, feeds: list[dict]) -> None:
    seen_urls = []
    for feed in feeds:
        packs_json = json.dumps(feed["packs"])
        seen_urls.append(feed["url"])
        reason = feed.get("disabled_reason")

        if feed["enabled"]:
            status_expr = "CASE WHEN status IN ('disabled', 'blocked') THEN 'ok' ELSE status END"
            status_params = ()
        else:
            forced_status = "blocked" if reason and reason.startswith("robots.txt") else "disabled"
            status_expr = "?"
            status_params = (forced_status,)

        cur = conn.execute(
            f"""
            UPDATE source
            SET name=?, packs=?, lang=?, weight=?, enabled=?, disabled_reason=?,
                status = {status_expr}
            WHERE feed_url=?
            """,
            (feed["name"], packs_json, feed["lang"], feed["weight"], int(feed["enabled"]), reason)
            + status_params
            + (feed["url"],),
        )
        if cur.rowcount == 0:
            insert_status = "ok" if feed["enabled"] else (
                "blocked" if reason and reason.startswith("robots.txt") else "disabled"
            )
            # first_seen_at set once, here, and never touched again --
            # tools/validate_feeds.py --longitudinal's basis for "how long
            # has this source had the opportunity to produce anything."
            conn.execute(
                """
                INSERT INTO source (name, feed_url, packs, lang, weight, enabled, disabled_reason, status, first_seen_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (feed["name"], feed["url"], packs_json, feed["lang"], feed["weight"],
                 int(feed["enabled"]), reason, insert_status, _now_iso()),
            )

    if seen_urls:
        placeholders = ",".join("?" for _ in seen_urls)
        conn.execute(
            f"UPDATE source SET status='disabled', enabled=0 WHERE feed_url NOT IN ({placeholders})",
            seen_urls,
        )
    else:
        conn.execute("UPDATE source SET status='disabled', enabled=0")

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


# FR-203: fetch robots.txt for a host over the network. `netloc` (not just
# hostname) so a feed on a non-default port gets its own origin's robots.txt
# rather than silently requesting port 80/443 and treating the resulting
# failure as allowed — caught by tools/accelerated_soak.py exercising the
# mock feed server on port 8765. Failure (network error or HTTP >=400) is
# treated as allowed, and logged — never cached, so the next run retries
# rather than permanently trusting a transient failure.
async def _fetch_robots_body(client: httpx.AsyncClient, scheme: str, netloc: str) -> str | None:
    robots_url = f"{scheme}://{netloc}/robots.txt"
    try:
        resp = await client.get(
            robots_url, headers={"User-Agent": config.USER_AGENT}, timeout=config.FETCH_TIMEOUT_S
        )
    except httpx.HTTPError as exc:
        _log.warning(f"robots.txt fetch failed for {netloc}: {exc}; treating as allowed", extra={"stage": "robots"})
        return None

    if resp.status_code >= 400:
        _log.warning(
            f"robots.txt fetch failed for {netloc}: HTTP {resp.status_code}; treating as allowed",
            extra={"stage": "robots"},
        )
        return None

    return resp.text


def _parse_robots(body: str) -> robotparser.RobotFileParser:
    rp = robotparser.RobotFileParser()
    rp.parse(body.splitlines())
    return rp


def _robots_cache_fresh(fetched_at: str) -> bool:
    fetched_dt = datetime.strptime(fetched_at, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    return datetime.now(timezone.utc) - fetched_dt < timedelta(hours=config.ROBOTS_CACHE_TTL_H)


# FR-203: robots.txt checked before every feed fetch. The cache is persisted
# in the `robots_cache` table (host, body, fetched_at) so ROBOTS_CACHE_TTL_H
# survives across runs — the process exits every ~15 minutes, so an
# in-memory-only cache never had a chance to apply and needlessly refetched
# every host's robots.txt on every run. `robots_cache`/`robots_locks` here
# are this run's in-memory layer, avoiding duplicate DB reads or fetches for
# a host checked more than once within a single run. Returns (allowed,
# source) where source is "cache" or "fetch", for stage_counts reporting.
async def _robots_allowed(
    conn: sqlite3.Connection,
    client: httpx.AsyncClient,
    url: str,
    robots_cache: dict,
    robots_locks: dict,
) -> tuple[bool, str]:
    parts = urlsplit(url)
    host = parts.hostname or ""
    lock = robots_locks.setdefault(host, asyncio.Lock())

    async with lock:
        # A hit here means this call did zero work regardless of how the
        # entry first got populated (this run's own DB read or network
        # fetch) -- it must report "cache", not the original caller's
        # source, or every within-run reuse of a freshly-fetched host would
        # be misreported as a fetch in stage_counts.
        if host in robots_cache:
            rp = robots_cache[host]
            source = "cache"
        else:
            row = conn.execute(
                "SELECT body, fetched_at FROM robots_cache WHERE host = ?", (host,)
            ).fetchone()
            if row is not None and _robots_cache_fresh(row[1]):
                rp = _parse_robots(row[0])
                source = "cache"
            else:
                body = await _fetch_robots_body(client, parts.scheme, parts.netloc)
                source = "fetch"
                if body is not None:
                    conn.execute(
                        """
                        INSERT INTO robots_cache (host, body, fetched_at) VALUES (?, ?, ?)
                        ON CONFLICT(host) DO UPDATE SET body = excluded.body, fetched_at = excluded.fetched_at
                        """,
                        (host, body, _now_iso()),
                    )
                    conn.commit()
                    rp = _parse_robots(body)
                else:
                    rp = None
            robots_cache[host] = rp

    allowed = True if rp is None else rp.can_fetch(config.USER_AGENT, url)
    return allowed, source


# FR-201/FR-202/FR-203/FR-205: conditional, robots-aware, polite feed fetch.
# Returns a plain result dict; never raises for network-level failures so a
# single feed can never abort the run (NFR-401).
async def _fetch_one(
    conn: sqlite3.Connection,
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
        allowed, robots_source = await _robots_allowed(conn, client, feed_url, robots_cache, robots_locks)
        if not allowed:
            return {"source_id": source_id, "status": "skipped_robots", "robots_source": robots_source}

        headers = {"User-Agent": config.USER_AGENT}
        if source_row["etag"]:
            headers["If-None-Match"] = source_row["etag"]
        if source_row["last_modified"]:
            headers["If-Modified-Since"] = source_row["last_modified"]

        try:
            resp = await client.get(feed_url, headers=headers, timeout=config.FETCH_TIMEOUT_S)
        except httpx.HTTPError as exc:
            return {"source_id": source_id, "status": "error", "error": str(exc), "robots_source": robots_source}

    if resp.status_code == 304:
        # FR-201: 304 is recorded; zero further work — no parse, no insert.
        return {"source_id": source_id, "status": "not_modified", "robots_source": robots_source}

    if resp.status_code != 200:
        return {
            "source_id": source_id,
            "status": "error",
            "error": f"HTTP {resp.status_code}",
            "robots_source": robots_source,
        }

    return {
        "source_id": source_id,
        "status": "ok",
        "etag": resp.headers.get("ETag"),
        "last_modified": resp.headers.get("Last-Modified"),
        "body": resp.content,
        "robots_source": robots_source,
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
        snippet = derive_snippet(entry.get("summary"), title) or None
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
                _fetch_one(conn, client, src, semaphore, last_request_at, host_locks, robots_cache, robots_locks)
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
        "robots_cache_hits": 0,
        "robots_fetches": 0,
    }
    errors: list[str] = []

    for result, src in zip(results, sources):
        source_id = src["id"]
        status = result["status"]

        robots_source = result.get("robots_source")
        if robots_source == "cache":
            counts["robots_cache_hits"] += 1
        elif robots_source == "fetch":
            counts["robots_fetches"] += 1

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
            # FR-203: a robots.txt-disallowed source is never a failure and
            # must not touch fail_streak, but it also must never be left
            # reporting 'ok' -- distinct from both ok and degraded so it's
            # visible in triage (tools/yield_analysis.py's zero-yield flag)
            # instead of silently looking healthy forever.
            if not dry_run:
                conn.execute(
                    "UPDATE source SET status = 'blocked' WHERE id = ? AND status != 'degraded'",
                    (source_id,),
                )
        else:
            counts["failures"] += 1
            errors.append(f"source {source_id}: {result.get('error')}")
            if not dry_run and _mark_source_result(conn, source_id, ok=False):
                counts["degraded"] += 1

    return counts, errors
