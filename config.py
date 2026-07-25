# NFR-503: every tuned constant lives in this module, never inline in logic.
# SRS §5 configuration surface. Constants marked "not live until week N" are
# declared now so the surface is visible, but MUST NOT be read by any week-1
# code path.

from pathlib import Path

import yaml

FEEDS_PATH = Path(__file__).parent / "feeds.yaml"

# --- Live in week 1 ---

# FR-503: active window for candidate clusters (also used to bound article
# retention windows during ingestion). Default per SRS §5; sweep is W3's job.
ACTIVE_WINDOW_H = 72

# NFR-301: retention window for articles before pruning eligibility.
RETENTION_D = 400

# FR-205: per-host politeness delay, seconds.
PER_HOST_DELAY_S = 2.0

# FR-205: global fetch concurrency cap.
MAX_CONCURRENCY = 8

# FR-104: consecutive parse failures before a feed is flagged degraded.
FAIL_STREAK_LIMIT = 20

# FR-203: robots.txt cache TTL, hours.
ROBOTS_CACHE_TTL_H = 24

# FR-202: truthful, contactable User-Agent. Fill in the real repo URL before
# deploying — must not impersonate a browser or another crawler.
USER_AGENT = "ArcLoom/0.1 (+https://github.com/<placeholder>/arcloom)"

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

        feeds.append(
            {
                "name": name,
                "url": url,
                "packs": packs,
                "enabled": enabled,
                "weight": float(weight),
                "lang": lang,
            }
        )

    return feeds
