"""Pure normalization functions. No network, no database."""

import re
from datetime import datetime, timedelta, timezone
from html.parser import HTMLParser
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from dateutil import parser as dateutil_parser

import config

# FR-204 vs NFR-602/AC-10 (see DECISIONS.md): a feed <description> may contain
# the entire article rather than a publisher-authored summary. Bounding to 2
# sentences / 300 chars, whichever is shorter, makes the result structurally
# incapable of being "the article body" regardless of what was stuffed in.
_SNIPPET_MAX_SENTENCES = 2
_SNIPPET_MAX_CHARS = 300
_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+(?=[A-Z])")


class _TagStripper(HTMLParser):
    def __init__(self):
        super().__init__()
        self._parts: list[str] = []

    def handle_data(self, data: str) -> None:
        self._parts.append(data)

    def get_text(self) -> str:
        return "".join(self._parts)


def canonicalize_url(url: str) -> str:
    """FR-207: canonicalize URL — strip utm_*, fbclid, gclid, fragment; lowercase host."""
    parts = urlsplit(url.strip())
    host = parts.hostname or ""
    host = host.lower()
    netloc = parts.netloc.lower()

    # one level of known redirect wrapper resolution
    if host in config.WRAPPER_HOSTS:
        query_pairs = parse_qsl(parts.query, keep_blank_values=True)
        for name, value in query_pairs:
            if name in config.WRAPPER_URL_PARAM_NAMES and value.startswith(("http://", "https://")):
                return canonicalize_url(value)

    scheme = parts.scheme.lower()
    path = parts.path
    if path != "/" and path.endswith("/"):
        path = path.rstrip("/")

    query_pairs = parse_qsl(parts.query, keep_blank_values=True)
    filtered = [
        (k, v)
        for k, v in query_pairs
        if not k.lower().startswith(config.TRACKING_PREFIXES) and k.lower() not in config.TRACKING_EXACT
    ]
    query = urlencode(filtered)

    if scheme == "http" and host in config.KNOWN_HTTPS_HOSTS:
        scheme = "https"

    return urlunsplit((scheme, netloc, path, query, ""))


def _fmt_utc(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _coerce_utc(value) -> datetime:
    dt = value if isinstance(value, datetime) else dateutil_parser.parse(value)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def normalize_timestamp(entry, fetched_at) -> tuple[str, bool]:
    """FR-206: normalize to UTC; fall back to fetched_at when absent, unparseable,
    or implausibly far in the future."""
    fetched_dt = _coerce_utc(fetched_at)
    dt = None

    for field in ("published_parsed", "updated_parsed"):
        struct = entry.get(field)
        if struct:
            try:
                dt = datetime(*struct[:6], tzinfo=timezone.utc)
                break
            except (TypeError, ValueError):
                dt = None

    if dt is None:
        raw = entry.get("published") or entry.get("updated")
        if raw:
            try:
                dt = _coerce_utc(dateutil_parser.parse(raw))
            except (ValueError, OverflowError, TypeError):
                dt = None

    if dt is None:
        return _fmt_utc(fetched_dt), True

    if dt > fetched_dt + timedelta(hours=config.FUTURE_TOLERANCE_H):
        return _fmt_utc(fetched_dt), True

    return _fmt_utc(dt), False


# FR-204/NFR-602/AC-10: derive a genuine snippet from a feed-provided
# description without ever storing article body text. See DECISIONS.md
# ("FR-204 vs NFR-602: description-as-body") for the resolution this
# implements. `title` is accepted per that resolution's signature but not
# currently used to alter the result.
def derive_snippet(raw_description: str | None, title: str) -> str:
    if not raw_description:
        return ""

    stripper = _TagStripper()
    stripper.feed(raw_description)
    text = re.sub(r"\s+", " ", stripper.get_text()).strip()
    if not text:
        return ""

    sentences = _SENTENCE_SPLIT_RE.split(text)
    snippet = " ".join(sentences[:_SNIPPET_MAX_SENTENCES]).strip()

    if len(snippet) <= _SNIPPET_MAX_CHARS:
        return snippet

    truncated = snippet[:_SNIPPET_MAX_CHARS]
    last_space = truncated.rfind(" ")
    if last_space > 0:
        truncated = truncated[:last_space]
    return truncated.rstrip()
