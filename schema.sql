-- Arc Loom — Week 1/2 schema
-- Week 1 implements the source, article and run_log tables; week 2 adds
-- cluster and article_cluster (FR-500 through FR-507) per SRS §4.
-- entity, cluster_entity, storyline, development, reader, interest, follow,
-- feedback and notification tables belong to later weeks (4, 5, 8, 9, 10)
-- and MUST NOT be created here.

-- FR-101/FR-102/FR-103/FR-104: feed configuration and ingestion state
CREATE TABLE IF NOT EXISTS source (
  id            INTEGER PRIMARY KEY,
  name          TEXT NOT NULL,
  feed_url      TEXT NOT NULL UNIQUE,
  packs         TEXT NOT NULL,          -- JSON array
  lang          TEXT NOT NULL DEFAULT 'en',
  weight        REAL NOT NULL DEFAULT 1.0,
  enabled       INTEGER NOT NULL DEFAULT 1,
  etag          TEXT,
  last_modified TEXT,
  last_ok_at    TEXT,
  fail_streak   INTEGER NOT NULL DEFAULT 0,
  status        TEXT NOT NULL DEFAULT 'ok',  -- ok | degraded | disabled | blocked
  disabled_reason TEXT,  -- FR-102 extension: machine-readable reason when enabled=0
  first_seen_at TEXT  -- set once, at INSERT, never updated: tools/validate_feeds.py --longitudinal's basis for "days configured with zero yield"
);

-- FR-204/G9: article body text is never fetched or stored. Only title,
-- feed-provided summary/description, canonical URL, publication timestamp,
-- author and feed-provided tags are permitted. No content/body column.
CREATE TABLE IF NOT EXISTS article (
  id                 INTEGER PRIMARY KEY,
  source_id          INTEGER NOT NULL REFERENCES source(id),
  canonical_url      TEXT NOT NULL UNIQUE,
  title              TEXT NOT NULL,
  snippet            TEXT,
  author             TEXT,
  published_at       TEXT NOT NULL,       -- UTC ISO8601
  fetched_at         TEXT NOT NULL,
  timestamp_inferred INTEGER NOT NULL DEFAULT 0,
  simhash            INTEGER,
  embedding          BLOB,                -- float32, L2-normalized (W2)
  embed_model        TEXT
);
CREATE INDEX IF NOT EXISTS idx_article_pub ON article(published_at);

-- FR-203: persisted robots.txt cache, keyed by host. The process exits every
-- 15 minutes, so an in-memory-only cache never let ROBOTS_CACHE_TTL_H apply
-- across runs; this table is what makes the TTL actually take effect.
-- Infrastructure for the existing week-1 FR-203 requirement, not new scope.
CREATE TABLE IF NOT EXISTS robots_cache (
  host       TEXT PRIMARY KEY,
  body       TEXT NOT NULL,
  fetched_at TEXT NOT NULL
);

-- Level 1 event clustering (FR-500 through FR-507). storyline_id, summary
-- and summary_hash belong to weeks 5 and 7 respectively but are declared
-- now as part of the table's full SRS §4 definition -- same convention as
-- article.embedding in week 1's table: the column exists, but nothing in
-- week 2 code reads or writes storyline_id/summary/summary_hash.
-- storyline_id has no REFERENCES clause yet, unlike SRS §4's literal text:
-- with PRAGMA foreign_keys=ON (db.py), SQLite requires a FK's target table
-- to exist at INSERT time even to store NULL, and storyline MUST NOT be
-- created before week 5 (CLAUDE.md scope). The REFERENCES constraint is
-- added back in week 5's migration once storyline exists.
CREATE TABLE IF NOT EXISTS cluster (
  id               INTEGER PRIMARY KEY,
  canonical_title  TEXT NOT NULL,
  centroid         BLOB,
  created_at       TEXT NOT NULL,
  updated_at       TEXT NOT NULL,
  member_count     INTEGER NOT NULL DEFAULT 0,
  distinct_sources INTEGER NOT NULL DEFAULT 0,
  summary          TEXT,                   -- W7
  summary_hash     TEXT,                   -- W7, member-set hash for cache
  storyline_id     INTEGER   -- W5, FK added once storyline table exists
);
CREATE INDEX IF NOT EXISTS idx_cluster_updated ON cluster(updated_at);

CREATE TABLE IF NOT EXISTS article_cluster (
  article_id INTEGER NOT NULL REFERENCES article(id),
  cluster_id INTEGER NOT NULL REFERENCES cluster(id),
  PRIMARY KEY (article_id, cluster_id)
);

-- Observability (all weeks)
CREATE TABLE IF NOT EXISTS run_log (
  id          INTEGER PRIMARY KEY,
  started_at  TEXT NOT NULL,
  finished_at TEXT,
  stage_counts TEXT,   -- JSON: {"process": {...at start}, "counts": {...at completion}}
  errors      TEXT,
  -- NFR-403: last completed stage boundary (started, config_loaded,
  -- sources_synced, pruned, fetch_started, fetch_complete, articles_written,
  -- complete). A row killed mid-run shows its last_stage as whatever it
  -- last reached, localizing where an unaccounted-for process death
  -- happened instead of leaving finished_at/errors both NULL with no clue.
  last_stage  TEXT
);
