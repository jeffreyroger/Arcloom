"""Feed autodiscovery: given a homepage URL, find its feed URL.

Primary method: <link rel="alternate" type="application/rss+xml"|
"application/atom+xml"> in the page's <head> -- the publisher's own
declaration of where its feed lives. This is preferred because a path
guessed from memory is often wrong, stale, or a dead redirect.

Fallback (2026-07-29, after a 20-homepage batch found a declared <link>
tag on only 1 -- <link> autodiscovery has declined as a web convention even
on sites that still serve a working feed at a conventional URL): if no
<link> tag is found, try a short fixed list of conventional feed paths
(FALLBACK_PATHS below) and accept one only if it actually parses as a feed
with at least one entry -- a 200 response alone isn't enough, since some
sites 200 every path with a generic page. A fallback hit is always
reported as a fallback, distinctly from a declared one: it is a disclosed,
generic list of conventions applied uniformly, not a per-publication guess
from memory, but the distinction matters for anyone reviewing the results.
Read-only.

FR-101/FR-102: produces (homepage, feed_url) candidates for feeds.yaml;
does not write to it -- adding an entry remains a manual, reviewed step.
"""

import argparse
import re
import sys
from pathlib import Path
from urllib.parse import urljoin

import feedparser
import httpx

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import config  # noqa: E402

_LINK_TAG_RE = re.compile(r"<link\b[^>]*>", re.IGNORECASE)
_REL_ALTERNATE_RE = re.compile(r'rel=["\']alternate["\']', re.IGNORECASE)
_TYPE_FEED_RE = re.compile(r'type=["\'](application/rss\+xml|application/atom\+xml)["\']', re.IGNORECASE)
_HREF_RE = re.compile(r'href=["\']([^"\']+)["\']', re.IGNORECASE)

FALLBACK_PATHS = ["/feed", "/feed/", "/rss.xml", "/rss/", "/feed/rss.xml", "/atom.xml", "/index.xml"]


def _find_declared(resp: httpx.Response) -> tuple[str | None, str | None]:
    for tag in _LINK_TAG_RE.findall(resp.text):
        if not _REL_ALTERNATE_RE.search(tag):
            continue
        type_match = _TYPE_FEED_RE.search(tag)
        if not type_match:
            continue
        href_match = _HREF_RE.search(tag)
        if not href_match:
            continue
        return urljoin(str(resp.url), href_match.group(1)), type_match.group(1)
    return None, None


def _try_fallback_paths(base_url: str, client: httpx.Client) -> tuple[str | None, str | None]:
    for path in FALLBACK_PATHS:
        candidate = urljoin(base_url, path)
        try:
            resp = client.get(candidate, headers={"User-Agent": config.USER_AGENT})
        except httpx.HTTPError:
            continue
        if resp.status_code != 200:
            continue
        parsed = feedparser.parse(resp.content)
        if not parsed.bozo and len(parsed.entries) > 0:
            return str(resp.url), path
    return None, None


def discover(homepage_url: str, client: httpx.Client) -> dict:
    result = {"homepage": homepage_url, "feed_url": None, "feed_type": None, "method": None, "error": None}
    try:
        resp = client.get(homepage_url, headers={"User-Agent": config.USER_AGENT})
    except httpx.HTTPError as exc:
        result["error"] = f"{type(exc).__name__}: {exc}"
        return result

    if resp.status_code != 200:
        result["error"] = f"HTTP {resp.status_code}"
        return result

    feed_url, feed_type = _find_declared(resp)
    if feed_url is not None:
        result["feed_url"] = feed_url
        result["feed_type"] = feed_type
        result["method"] = "declared"
        return result

    fallback_url, fallback_path = _try_fallback_paths(str(resp.url), client)
    if fallback_url is not None:
        result["feed_url"] = fallback_url
        result["feed_type"] = None
        result["method"] = f"fallback:{fallback_path}"
        return result

    result["error"] = "no <link rel=alternate> declared and no conventional fallback path parsed as a feed"
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description="Discover the publisher-declared feed URL for a homepage")
    parser.add_argument("urls", nargs="*", help="Homepage URL(s)")
    parser.add_argument("--file", type=Path, help="Path to a file of homepage URLs, one per line")
    args = parser.parse_args()

    urls = list(args.urls)
    if args.file:
        urls.extend(
            line.strip() for line in args.file.read_text(encoding="utf-8").splitlines()
            if line.strip() and not line.strip().startswith("#")
        )
    if not urls:
        print("usage: discover_feed.py <homepage-url> [<homepage-url> ...] | --file urls.txt", file=sys.stderr)
        return 2

    found = 0
    with httpx.Client(follow_redirects=True, timeout=config.FETCH_TIMEOUT_S) as client:
        for homepage in urls:
            r = discover(homepage, client)
            if r["feed_url"]:
                found += 1
                print(f"{r['homepage']}\t{r['feed_url']}\t{r['method']}")
            else:
                print(f"{r['homepage']}\tNOT_FOUND\t{r['error']}")

    print(f"\n{found} of {len(urls)} homepage(s) yielded a feed (declared or fallback).", file=sys.stderr)
    return 0 if found else 1


if __name__ == "__main__":
    sys.exit(main())
