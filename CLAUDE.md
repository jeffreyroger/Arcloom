# Arc Loom — Working Reference

Arc Loom is a personal, open-source news system that tracks storylines over time instead of surfacing new articles, notifying only when a storyline genuinely develops. It uses a two-level clustering architecture: Level 1 groups articles into event clusters via incremental centroid matching over sentence-transformer embeddings within a 72h window (pure semantic similarity); Level 2 links event clusters into weeks-to-months storylines where entity-set Jaccard similarity is the dominant scoring term and embedding cosine similarity is only secondary. Silence is the default: nothing is surfaced or notified unless it clears a measured, eval-justified threshold.

## Global constraints (G1–G10) — apply to every week, non-negotiable

| # | Constraint | Rationale |
|---|---|---|
| **G1** | No web framework before week 11. Jinja2 → static HTML only. | Frameworks invite UI work, which is infinitely absorbing and not the project. |
| **G2** | No LLM API calls before week 7. | Free iteration on clustering is worth more than early summaries. |
| **G3** | No vector database, ever, without a latency measurement proving SQLite insufficient. | The measurement will never come. That is the point. |
| **G4** | No user accounts before week 9. | Auth is a week of work that teaches nothing. |
| **G5** | No new tunable constant without a plan to measure it. | Untuned magic numbers are how these projects rot. |
| **G6** | No feed count above 100 before week 12. | More feeds mask quality problems with volume. |
| **G7** | If a week runs over budget, its stretch goals are deleted, not deferred. | Deferred work accumulates into an unfinishable backlog. |
| **G8** | One commit per working session, minimum. Broken states get committed with a `WIP:` prefix. | Solo projects die in uncommitted branches. |
| **G9** | Article body text is never fetched or stored. | Legal posture. Non-negotiable. |
| **G10** | Weeks 3, 6 and 8 are **gates**. Do not begin the next week until the gate's number is measured and recorded, even if it is bad. | A bad measured number is progress. An unmeasured system is not. |

## Current week: **Week 1 — Ingestion and schema**

**Goal:** Reliably pull 60–80 feeds into SQLite without losing or duplicating anything.

**Time budget:** 10h (4h schema+config, 4h ingest, 2h hardening)

**Deliverables**
- `schema.sql` implementing the source/article tables from SRS §4 (those two only)
- `feeds.yaml` with 60–80 AI-industry + tech-policy feeds, tagged into 3–4 packs
- `pipeline/ingest.py` — conditional GET, robots.txt cache, per-host delay, concurrency cap
- URL canonicalization and timestamp normalization with a unit test each
- `run_log` writes on every run
- Cron entry running every 15 minutes

**Do NOT**
- Embed anything
- Cluster anything
- Build any view, page, or template
- Add more than 80 feeds
- Write a retry/backoff framework — a fail-streak counter is enough
- Use an async framework beyond `httpx` + `asyncio.gather` with a semaphore

**Done when**
- Pipeline runs 24 hours unattended with zero crashes
- Running it twice back-to-back inserts zero new rows
- `SELECT COUNT(*) FROM article` is between 500 and 2,000 after a day
- At least one deliberately broken feed URL is in `feeds.yaml` and is correctly marked `degraded`

**Tripwires**
- Writing a `Feed` class hierarchy → stop, use a dict
- Adding a `content` or `body` column → violates G9
- More than 90 minutes on timezone parsing → hardcode `dateutil.parser` fallback and move on

> Update this section (goal, deliverables, Do NOT list, done-tests, tripwires) verbatim from `02-IMPLEMENTATION-PLAN.md` when advancing to the next week. Do not advance past a gate week (3, 6, 8) without its measured number recorded per G10.

## Requirement traceability rule

Every function or module that implements a requirement MUST carry a comment citing its requirement ID, e.g.:

```python
# FR-207: canonicalize URL — strip utm_*, fbclid, gclid, fragment; lowercase host
```

Any code you are about to write that cannot be traced to a requirement ID (FR-xxx, NFR-xxx, or an explicit deliverable in the current week's plan) must be flagged in your response *before* writing it — say which requirement it's missing and ask, don't silently add it.

## Scope enforcement rule

If asked for something that violates a global constraint (G1–G10) or falls outside the current week's scope: **do not build it.** State which constraint or week-scope boundary it violates, and stop. Do not implement a "smaller" or "temporary" version as a workaround.

## Tech stack decisions

- **Language:** Python
- **Storage:** SQLite, no ORM
- **Web framework:** none until week 11 (G1)
- **Vector database:** never, without a measured latency justification (G3)
- **LLM calls:** none until week 7 (G2)

## Banned dependencies — Week 1

Do not install or import: `tenacity`, `celery`, `sqlalchemy`, `pydantic`, `click`, `rich`, `scrapy`, `beautifulsoup4`, `requests`.

**Allowed:** `httpx`, `feedparser`, `pyyaml`, `python-dateutil`, `pytest`.
