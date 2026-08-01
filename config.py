# NFR-503: every tuned constant lives in this module, never inline in logic.
# SRS §5 configuration surface. Constants marked "not live until week N" are
# declared now so the surface is visible, but MUST NOT be read by any week-1
# code path.

import os
from pathlib import Path

import yaml

FEEDS_PATH = Path(__file__).parent / "feeds.yaml"

# --- Live in week 1 ---

# FR-503: active window for candidate clusters (also used to bound article
# retention windows during ingestion). Default per SRS §5; sweep is W3's job.
# Defined week 1, first used week 2 (clustering).
ACTIVE_WINDOW_H = 72

# NFR-301: retention window for articles before pruning eligibility.
# Defined week 1, first used week 2 (clustering).
RETENTION_D = 400

# FR-205: per-host politeness delay, seconds.
PER_HOST_DELAY_S = 2.0

# FR-205: global fetch concurrency cap.
MAX_CONCURRENCY = 8

# FR-104: consecutive parse failures before a feed is flagged degraded.
FAIL_STREAK_LIMIT = 20

# FR-203: robots.txt cache TTL, hours. Overridable via ARCLOOM_ROBOTS_CACHE_TTL_H
# so tools/accelerated_soak.py can force the cache-expiry-and-refetch path
# within a short soak run instead of waiting 24h for it to occur naturally.
# Production default is unchanged at 24.
ROBOTS_CACHE_TTL_H = float(os.environ.get("ARCLOOM_ROBOTS_CACHE_TTL_H", 24))

# FR-202: truthful, contactable User-Agent — must not impersonate a browser
# or another crawler.
USER_AGENT = "ArcLoom/0.1 (+https://github.com/jeffreyroger/Arcloom)"

# --- Operational settings (NFR-503: centralized here; not G5 tunables —
# these are timeouts/tolerances, not quality-affecting thresholds, so no
# measurement plan is required for them) ---

# FR-201/FR-203: HTTP client timeout, seconds. Shared by feed fetches,
# robots.txt fetches (pipeline/ingest.py) and feeds.yaml pre-flight checks
# (tools/validate_feeds.py) — one value, not two redundant ones.
FETCH_TIMEOUT_S = 10.0

# Gap: no SRS requirement ID covers operational-table retention — NFR-301
# covers article retention only, not run_log. Tracked in DECISIONS.md
# ("Week 1 criterion 1" entry) pending week 12's SRS revision. run_log grows
# ~375B/run (~13MB/year at 15-min cadence) with no bound otherwise.
RUN_LOG_RETENTION_D = 90

# Gap: no SRS requirement ID covers abandoned-run detection — mitigation for
# the still-open NFR-402/NFR-403 gap (see DECISIONS.md "Week 1 retrospective"
# correction, 2026-08-01): some runs vanish with finished_at/errors both NULL
# and no trace of why. A run cannot still be legitimately alive this long
# given the 10-minute Task Scheduler ExecutionTimeLimit and 4-40s observed
# durations, so pipeline/run.py's reaper treats any such row older than this
# threshold as abandoned rather than leaving it permanently ambiguous.
ABANDONED_THRESHOLD_M = 30

# FR-206: a parsed article timestamp further into the future than this is
# treated as a feed bug, not fact, and triggers the fetched_at fallback.
FUTURE_TOLERANCE_H = 48

# tools/validate_feeds.py: a feed whose newest entry is older than this is
# flagged dead in the pre-flight report.
STALE_DAYS = 30

# tools/yield_analysis.py: assumed upper bound on how long a source's RSS
# feed retains an entry before it can scroll out of the window. A
# (source, day) pair is only counted as "fully captured" for criterion 3 if
# at least one successful fetch of that source landed within this many days
# of the publish date -- otherwise its day's articles may have already
# scrolled out of the feed before any poll ever saw them.
FEED_WINDOW_DAYS = 3

# --- Static reference data (NFR-503: centralized here; not G5 tunables —
# lists of known hosts/params, not thresholds) ---

# FR-207: query params stripped during URL canonicalization. `ref` and
# `source` are deliberately excluded — FR-207 only specifies utm_*, fbclid,
# gclid, and fragment, and both are load-bearing on some sites (e.g.
# distinguishing content sections), so stripping them can collapse
# genuinely distinct URLs into one and cause false-positive dedup.
TRACKING_PREFIXES = ("utm_",)
TRACKING_EXACT = {"fbclid", "gclid", "mc_cid", "mc_eid"}

# FR-207: known redirect wrapper hosts, resolved one level via a query param.
WRAPPER_HOSTS = {"news.google.com", "feedproxy.google.com"}
WRAPPER_URL_PARAM_NAMES = ("url", "u", "q")

# FR-207: hosts known to serve HTTPS reliably; http is upgraded only for
# these. Conservative default of wrapper hosts only — forcing https on an
# arbitrary feed host risks pointing at a URL that doesn't exist.
KNOWN_HTTPS_HOSTS = {"news.google.com", "feedproxy.google.com"}

# --- Not live until later weeks ---

TAU_EVENT = 0.75          # not live until week 2/3 — W3 sweep justifies this value
SIMHASH_HAMMING = 3        # not live until week 2 — W2 spot check
TAU_STORY = 0.55           # not live until week 5/6 — W6 sweep
W_JACCARD = 0.6            # not live until week 5/6 — W6 ablation
W_EMBED = 0.25             # not live until week 5/6 — W6 ablation
W_RECENCY = 0.15           # not live until week 5/6 — W6 ablation
STORYLINE_TTL_D = 45       # not live until week 5
STATE_TOKEN_BUDGET = 800   # not live until week 7
SIG_THRESHOLD_PCTL = None  # not live until week 10 — calibrated, not a literal
DAILY_BUDGET_K = 2         # not live until week 10


# FR-101/FR-102/FR-103: read and validate feeds.yaml. Fails loudly, naming
# the offending entry — malformed entries are never silently skipped.
def load_feeds(path: Path = FEEDS_PATH) -> list[dict]:
    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}

    raw_feeds = data.get("feeds")
    if raw_feeds is None:
        raise ValueError(f"{path}: missing top-level 'feeds' key")

    feeds = []
    for i, entry in enumerate(raw_feeds):
        label = entry.get("name") if isinstance(entry, dict) and entry.get("name") else f"<entry #{i}>"

        if not isinstance(entry, dict):
            raise ValueError(f"feeds.yaml entry {label!r}: not a mapping")

        name = entry.get("name")
        if not name or not isinstance(name, str):
            raise ValueError(f"feeds.yaml entry {label!r}: 'name' is required and must be a string")

        url = entry.get("url")
        if not url or not isinstance(url, str):
            raise ValueError(f"feeds.yaml entry {label!r}: 'url' is required and must be a string")

        packs = entry.get("packs")
        if not packs or not isinstance(packs, list) or not all(isinstance(p, str) for p in packs):
            raise ValueError(f"feeds.yaml entry {label!r}: 'packs' is required and must be a list of strings")

        enabled = entry.get("enabled", True)
        if not isinstance(enabled, bool):
            raise ValueError(f"feeds.yaml entry {label!r}: 'enabled' must be a bool")

        weight = entry.get("weight", 1.0)
        if not isinstance(weight, (int, float)) or isinstance(weight, bool):
            raise ValueError(f"feeds.yaml entry {label!r}: 'weight' must be a number")

        lang = entry.get("lang", "en")
        if not isinstance(lang, str):
            raise ValueError(f"feeds.yaml entry {label!r}: 'lang' must be a string")

        # FR-102: not one of the spec's minimum required fields, but FR-102
        # lists a minimum, not an exclusive set. Machine-readable record of
        # why a source is disabled -- week 12's README needs a count of
        # exclusions and their reasons, which free-text comments can't give.
        disabled_reason = entry.get("disabled_reason")
        if disabled_reason is not None and not isinstance(disabled_reason, str):
            raise ValueError(f"feeds.yaml entry {label!r}: 'disabled_reason' must be a string")
        if enabled and disabled_reason:
            raise ValueError(f"feeds.yaml entry {label!r}: 'disabled_reason' set but 'enabled' is true")

        feeds.append(
            {
                "name": name,
                "url": url,
                "packs": packs,
                "enabled": enabled,
                "weight": float(weight),
                "lang": lang,
                "disabled_reason": disabled_reason,
            }
        )

    return feeds
