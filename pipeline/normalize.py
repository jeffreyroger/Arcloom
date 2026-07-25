"""Pure normalization functions. No network, no database."""

from datetime import datetime, timedelta, timezone
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from dateutil import parser as dateutil_parser

# FR-207: query params stripped during canonicalization
_TRACKING_PREFIXES = ("utm_",)
_TRACKING_EXACT = {"fbclid", "gclid", "mc_cid", "mc_eid", "ref", "source"}

# FR-207: known redirect wrapper hosts, resolved one level via a query param
_WRAPPER_HOSTS = {"news.google.com", "feedproxy.google.com"}
_WRAPPER_URL_PARAM_NAMES = ("url", "u", "q")

# FR-207: hosts known to serve HTTPS reliably; http is upgraded only for these.
# Conservative default of wrapper hosts only — forcing https on an arbitrary
# feed host risks pointing at a URL that doesn't exist.
_KNOWN_HTTPS_HOSTS = {"news.google.com", "feedproxy.google.com"}

# FR-206: a parsed timestamp further than this into the future is treated as
# a feed bug, not fact, and triggers the fetched_at fallback.
_FUTURE_TOLERANCE_H = 48


def canonicalize_url(url: str) -> str:
    """FR-207: canonicalize URL — strip utm_*, fbclid, gclid, fragment; lowercase host."""
    parts = urlsplit(url.strip())
    host = parts.hostname or ""
    host = host.lower()
    netloc = parts.netloc.lower()

    # one level of known redirect wrapper resolution
    if host in _WRAPPER_HOSTS:
        query_pairs = parse_qsl(parts.query, keep_blank_values=True)
        for name, value in query_pairs:
            if name in _WRAPPER_URL_PARAM_NAMES and value.startswith(("http://", "https://")):
                return canonicalize_url(value)

    scheme = parts.scheme.lower()
    path = parts.path
    if path != "/" and path.endswith("/"):
        path = path.rstrip("/")

    query_pairs = parse_qsl(parts.query, keep_blank_values=True)
    filtered = [
        (k, v)
        for k, v in query_pairs
        if not k.lower().startswith(_TRACKING_PREFIXES) and k.lower() not in _TRACKING_EXACT
    ]
    query = urlencode(filtered)

    if scheme == "http" and host in _KNOWN_HTTPS_HOSTS:
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

    if dt > fetched_dt + timedelta(hours=_FUTURE_TOLERANCE_H):
        return _fmt_utc(fetched_dt), True

    return _fmt_utc(dt), False
