"""Standalone pre-flight check on feeds.yaml.

Not part of the pipeline (that's pipeline/, week 1's ingestion path). This
tool exists because an RSS URL recalled from memory is often dead, moved, or
never existed — feeds.yaml should not be trusted until every entry in it has
been fetched and inspected at least once.

FR-101/FR-102: reads feed entries from feeds.yaml.
FR-202: uses the project's truthful, contactable User-Agent.
FR-205: respects the per-host politeness delay from config.py.
"""

import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlsplit

import feedparser
import httpx
import yaml

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import config  # noqa: E402

FEEDS_PATH = Path(__file__).resolve().parent.parent / "feeds.yaml"


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


def main() -> int:
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
