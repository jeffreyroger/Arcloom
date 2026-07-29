"""Standalone pre-flight check on feeds.yaml.

Not part of the pipeline (that's pipeline/, week 1's ingestion path). This
tool exists because an RSS URL recalled from memory is often dead, moved, or
never existed — feeds.yaml should not be trusted until every entry in it has
been fetched and inspected at least once.

FR-101/FR-102: reads feed entries from feeds.yaml.
FR-202: uses the project's truthful, contactable User-Agent.
FR-205: respects the per-host politeness delay from config.py.

--longitudinal (added after Tech Policy Press and three robots-blocked
sources sat at status='ok' with zero articles for two weeks, undetected):
a point-in-time network check like the one above cannot see "this source
has been enabled for 14 days and produced nothing" -- that requires the
production database's history, not another live fetch. This mode queries
arcloom.db instead of the network (except for the blocked_but_ok
regression guard, which by definition needs a live robots.txt read).
"""

import sqlite3
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib import robotparser
from urllib.parse import urlsplit

import feedparser
import httpx
import yaml

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import config  # noqa: E402
import db  # noqa: E402

FEEDS_PATH = Path(__file__).resolve().parent.parent / "feeds.yaml"

# --longitudinal thresholds, as specified for this check (not a G5 sweep
# candidate -- these are fixed reporting windows, not a quality-tuned
# constant).
ZERO_YIELD_MIN_AGE_D = 7
STALLED_WINDOW_D = 14


def _host(url: str) -> str:
    return (urlsplit(url).hostname or "").lower()


def _newest_published(entries) -> datetime | None:
    newest = None
    for entry in entries:
        struct = entry.get("published_parsed") or entry.get("updated_parsed")
        if not struct:
            continue
        try:
            dt = datetime(*struct[:6], tzinfo=timezone.utc)
        except (TypeError, ValueError):
            continue
        if newest is None or dt > newest:
            newest = dt
    return newest


def validate_feed(entry: dict, client: httpx.Client) -> dict:
    name = entry["name"]
    url = entry["url"]

    status = None
    bozo = True
    entry_count = 0
    days_since_newest = None
    resp = None

    try:
        resp = client.get(url, headers={"User-Agent": config.USER_AGENT})
        status = resp.status_code
    except httpx.HTTPError as exc:
        status = f"ERR:{type(exc).__name__}"

    if resp is not None and status == 200:
        parsed = feedparser.parse(resp.content)
        bozo = bool(parsed.bozo)
        entry_count = len(parsed.entries)
        newest = _newest_published(parsed.entries)
        if newest is not None:
            days_since_newest = (datetime.now(timezone.utc) - newest).days

    dead = (
        status != 200
        or bozo
        or entry_count == 0
        or days_since_newest is None
        or days_since_newest > config.STALE_DAYS
    )

    return {
        "name": name,
        "status": status,
        "bozo": bozo,
        "entry_count": entry_count,
        "days_since_newest": days_since_newest,
        "dead": dead,
    }


def _print_table(results: list[dict]) -> None:
    name_w = max((len(r["name"]) for r in results), default=4)
    header = f"{'NAME':<{name_w}}  {'STATUS':<8}  {'ENTRIES':>7}  {'DAYS SINCE NEWEST':>18}  FLAG"
    print(header)
    print("-" * len(header))
    for r in results:
        days_str = str(r["days_since_newest"]) if r["days_since_newest"] is not None else "n/a"
        flag = "DEAD" if r["dead"] else "ok"
        print(
            f"{r['name']:<{name_w}}  {str(r['status']):<8}  {r['entry_count']:>7}  "
            f"{days_str:>18}  {flag}"
        )


def _parse_ts(ts: str) -> datetime:
    return datetime.strptime(ts, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)


# blocked_but_ok is the one live check in --longitudinal: whether robots.txt
# currently disallows a source recorded as status='ok'. This should be
# structurally impossible after pipeline/ingest.py's skipped_robots handler
# sets status='blocked' on any such source going forward -- flagging it
# here is a regression guard, not routine monitoring, which is why it's the
# only network call in an otherwise DB-only mode. Same "failure = allowed"
# policy as pipeline/ingest.py: an unreadable robots.txt is not evidence of
# a block.
def _robots_disallows(feed_url: str, client: httpx.Client) -> bool:
    parts = urlsplit(feed_url)
    robots_url = f"{parts.scheme}://{parts.netloc}/robots.txt"
    try:
        resp = client.get(robots_url, headers={"User-Agent": config.USER_AGENT})
    except httpx.HTTPError:
        return False
    if resp.status_code >= 400:
        return False
    rp = robotparser.RobotFileParser()
    rp.parse(resp.text.splitlines())
    return not rp.can_fetch(config.USER_AGENT, feed_url)


def longitudinal_report(conn: sqlite3.Connection) -> list[dict]:
    now = datetime.now(timezone.utc)
    cutoff_14d = (now - timedelta(days=STALLED_WINDOW_D)).strftime("%Y-%m-%dT%H:%M:%SZ")

    rows = conn.execute(
        "SELECT id, name, feed_url, status, first_seen_at, last_ok_at FROM source WHERE enabled = 1"
    ).fetchall()

    results = []
    robots_cache: dict[str, bool] = {}

    with httpx.Client(follow_redirects=True, timeout=config.FETCH_TIMEOUT_S) as client:
        for row in rows:
            source_id, name, feed_url, status, first_seen_at, last_ok_at = row

            total_articles = conn.execute(
                "SELECT COUNT(*) FROM article WHERE source_id = ?", (source_id,)
            ).fetchone()[0]
            articles_14d = conn.execute(
                "SELECT COUNT(*) FROM article WHERE source_id = ? AND fetched_at >= ?",
                (source_id, cutoff_14d),
            ).fetchone()[0]

            days_since_first_seen = (now - _parse_ts(first_seen_at)).days if first_seen_at else None

            zero_yield = (
                total_articles == 0
                and days_since_first_seen is not None
                and days_since_first_seen > ZERO_YIELD_MIN_AGE_D
            )
            stalled = total_articles > 0 and articles_14d == 0

            blocked_but_ok = False
            if status == "ok":
                netloc = urlsplit(feed_url).netloc
                if netloc not in robots_cache:
                    robots_cache[netloc] = _robots_disallows(feed_url, client)
                blocked_but_ok = robots_cache[netloc]

            results.append({
                "name": name,
                "status": status,
                "days_since_first_seen": days_since_first_seen,
                "total_articles": total_articles,
                "articles_14d": articles_14d,
                "last_ok_at": last_ok_at,
                "zero_yield": zero_yield,
                "stalled": stalled,
                "blocked_but_ok": blocked_but_ok,
            })

    return results


def print_longitudinal_report(results: list[dict]) -> bool:
    """Returns True if any flag fired (for callers wanting a nonzero exit)."""
    name_w = max((len(r["name"]) for r in results), default=4)
    header = (
        f"{'NAME':<{name_w}}  {'STATUS':<9}  {'AGE_D':>6}  {'TOTAL':>6}  "
        f"{'14D':>4}  {'LAST_OK_AT':<21}  FLAGS"
    )
    print(header)
    print("-" * len(header))
    for r in results:
        age_str = str(r["days_since_first_seen"]) if r["days_since_first_seen"] is not None else "n/a"
        last_ok = r["last_ok_at"] or "never"
        flags = []
        if r["zero_yield"]:
            flags.append("ZERO_YIELD")
        if r["stalled"]:
            flags.append("STALLED")
        if r["blocked_but_ok"]:
            flags.append("BLOCKED_BUT_OK")
        print(
            f"{r['name']:<{name_w}}  {r['status']:<9}  {age_str:>6}  {r['total_articles']:>6}  "
            f"{r['articles_14d']:>4}  {last_ok:<21}  {' '.join(flags) if flags else '-'}"
        )

    by_status: dict[str, int] = {}
    for r in results:
        by_status[r["status"]] = by_status.get(r["status"], 0) + 1
    zero_yield_n = sum(1 for r in results if r["zero_yield"])
    stalled_n = sum(1 for r in results if r["stalled"])
    blocked_but_ok_n = sum(1 for r in results if r["blocked_but_ok"])

    print(f"\n{len(results)} enabled source(s). By status: "
          + ", ".join(f"{k}={v}" for k, v in sorted(by_status.items())))
    print(f"zero_yield={zero_yield_n}  stalled={stalled_n}  blocked_but_ok={blocked_but_ok_n}")

    if blocked_but_ok_n:
        print(
            "\nblocked_but_ok is a regression guard: status='ok' should be impossible "
            "for a robots.txt-disallowed source since pipeline/ingest.py's skipped_robots "
            "handler sets status='blocked'. Investigate immediately."
        )

    return bool(zero_yield_n or stalled_n or blocked_but_ok_n)


def main() -> int:
    if "--longitudinal" in sys.argv:
        conn = db.get_connection()
        conn.row_factory = sqlite3.Row
        results = longitudinal_report(conn)
        conn.close()
        if not results:
            print("No enabled sources.")
            return 0
        flagged = print_longitudinal_report(results)
        return 1 if flagged else 0

    with open(FEEDS_PATH, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    feeds = data.get("feeds") or []

    if not feeds:
        print("No feeds in feeds.yaml.")
        return 0

    results = []
    last_request_at: dict[str, float] = {}

    with httpx.Client(follow_redirects=True, timeout=config.FETCH_TIMEOUT_S) as client:
        for entry in feeds:
            host = _host(entry["url"])
            last = last_request_at.get(host)
            if last is not None:
                wait = config.PER_HOST_DELAY_S - (time.monotonic() - last)
                if wait > 0:
                    time.sleep(wait)
            results.append(validate_feed(entry, client))
            last_request_at[host] = time.monotonic()

    _print_table(results)

    dead_count = sum(1 for r in results if r["dead"])
    if dead_count:
        print(f"\n{dead_count} of {len(results)} feed(s) flagged dead.")
        return 1

    print(f"\nAll {len(results)} feeds healthy.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
