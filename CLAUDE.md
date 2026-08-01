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

## Week 1 close state (recorded at handoff to week 2)

17 alive feeds (against a 60-80 target — known, unremediated gap), 4
robots-blocked, 4 disabled dead/stale/empty, 1 intentional broken canary.
0 crashes over 13 real-scheduler runs in the closeout continuity window,
but 2 of those 13 runs were silently killed by Task Scheduler's
`ExecutionTimeLimit` with no crash/interrupt record — a confirmed,
unremediated gap in NFR-402's bookkeeping (see DECISIONS.md "Week 1
retrospective"). Full detail, defect list, and criteria verdicts in
DECISIONS.md.

## Standing rule: never edit the repo while the scheduled task is enabled

The 15-minute ArcLoom scheduled task reads `feeds.yaml`/`config.py`/
`pipeline/` and writes `arcloom.db` on every firing. Editing any of these
while the task is enabled risks a read/write race with a live run and has
been the source of every SIGINT in this project's history. Disable the
task first (`Disable-ScheduledTask -TaskName ArcLoom`), make the change,
then re-enable it — do not edit around a running cron.

## Current week: **Week 2 — Embedding and event clustering**

**Goal:** Articles collapse into event clusters. Quality unknown and that is fine this week.

**Time budget:** 10h (2h embed, 4h cluster, 2h simhash, 2h render)

**Deliverables**
- `pipeline/embed.py` — `bge-small-en-v1.5`, CPU, batched, L2-normalized, stored as `float32` BLOB with model tag
- `pipeline/simhash.py` — 64-bit over title trigrams, Hamming ≤ 3 bypass
- `pipeline/cluster.py` — incremental single-pass centroid matching over a 72h active window
- `cluster` and `article_cluster` tables
- `pipeline/render.py` — Jinja2 → one `index.html`, cards showing canonical title, source count, links

**Do NOT**
- Tune `TAU_EVENT` by eye. Set it to 0.75 and leave it. Tuning is week 3's job and requires the gold set.
- Add HDBSCAN, DBSCAN, or any batch clustering library — incremental is a hard requirement (FR-502)
- Style the HTML beyond legibility. No CSS framework.
- Add pagination, filtering, sorting or search
- Try a second embedding model "to compare" — that is a week 3 activity with a metric

**Done when**
- Every article has exactly one cluster
- `index.html` renders the last 72 hours
- A visibly wire-syndicated story shows a source count > 1
- Full pipeline run completes in under 5 minutes (NFR-101)

**Tripwires**
- Adjusting `TAU_EVENT` more than once → you are eye-tuning; stop
- Any time spent on CSS → cap at 30 minutes total
- Considering GPU setup → unnecessary at this volume
- Abandoned-run rate (`tools/health.py`) exceeds 60% during week 2 → stop
  and investigate the still-open week 1 kill-signature defect (DECISIONS.md
  "Week 1 retrospective" correction, 2026-08-01). Week 2 runs an order of
  magnitude longer (embedding pushes runs from ~4s to 30-60s), so a jump
  past 60% would indicate the cause is duration-correlated. If the rate
  stays near the ~42% historical baseline, it's startup-correlated and
  remains a host-environment issue deferred to the week 10 VPS migration.

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

## Banned dependencies — Week 2

Do not install or import: `hdbscan`, `scikit-learn`, `faiss`, any vector
database client (G3), plus week 1's list — `tenacity`, `celery`,
`sqlalchemy`, `pydantic`, `click`, `rich`, `scrapy`, `beautifulsoup4`,
`requests`.

**Newly allowed this week:** `sentence-transformers`, `numpy`.

**Allowed (carried forward):** `httpx`, `feedparser`, `pyyaml`, `python-dateutil`, `pytest`.
