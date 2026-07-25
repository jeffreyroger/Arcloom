-- Arc Loom — Week 1 schema
-- Implements ONLY the source, article and run_log tables per SRS §4.
-- cluster, article_cluster, entity, storyline, development, reader,
-- interest, follow, feedback and notification tables belong to later
-- weeks (2, 4, 5, 8, 9, 10) and MUST NOT be created here.

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
  status        TEXT NOT NULL DEFAULT 'ok'   -- ok | degraded | disabled
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

-- Observability (all weeks)
CREATE TABLE IF NOT EXISTS run_log (
  id          INTEGER PRIMARY KEY,
  started_at  TEXT NOT NULL,
  finished_at TEXT,
  stage_counts TEXT,   -- JSON
  errors      TEXT
);
