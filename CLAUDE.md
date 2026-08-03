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

## Week 2 close state (recorded at handoff to week 3)

1,568 embedded articles collapsed into 886 event clusters (783 singletons,
88.4% — expected at 17 feeds per week 2's tripwire; the quantitative case
for feed expansion, not a clustering defect). Every article has exactly one
cluster (0 unclustered, 0 duplicate assignments). `index.html` rendered 120
clusters / 640 article links for the trailing 72h window. Full pipeline run
(fetch → simhash → embed → cluster → render) completed in ~19s, well under
the 5-minute NFR-101 budget. Feed count unchanged from week 1 at 17 alive
(still below the 60–80 target — same unremediated gap, tracked for the
week-12 feed-expansion track). Abandoned-run rate over the last 7 days is
24.2%, under week 2's 60% tripwire and below the ~42% historical baseline,
so no investigation was triggered.

One clustering anomaly to carry into week 3's labeling: a single cluster
grew to 363 members from only 2 distinct sources — a likely centroid-drift
artifact of running-mean incremental matching, where repeated near-duplicate
titles from one source drag the centroid until it starts absorbing
less-related content. Not remediated: `TAU_EVENT` stays frozen at 0.75 per
the week 2 tripwire, pending the week 3 gold-set sweep.

## Standing rule: never edit the repo while the scheduled task is enabled

The 15-minute ArcLoom scheduled task reads `feeds.yaml`/`config.py`/
`pipeline/` and writes `arcloom.db` on every firing. Editing any of these
while the task is enabled risks a read/write race with a live run and has
been the source of every SIGINT in this project's history. Disable the
task first (`Disable-ScheduledTask -TaskName ArcLoom`), make the change,
then re-enable it — do not edit around a running cron.

## Current week: **Week 3 — GATE: label, measure, tune**

**Goal:** A number. This is the most important week in the plan and the least enjoyable.

**Time budget:** 11h (2h export tooling, 3h labeling, 3h scorer, 2h sweep, 1h writeup)

**Deliverables**
- `evals/export_labeling.py` — dumps ~500 articles across 3 days to CSV, **sorted by embedding similarity** so duplicates are adjacent (FR-1201). This sort is what makes labeling take 90 minutes instead of 5 hours.
- `evals/gold_clusters.csv` — hand-labeled, committed to the repo
- `evals/score_clustering.py` — B-cubed precision/recall/F1
- Threshold sweep over `TAU_EVENT` ∈ [0.60, 0.90] step 0.01, and `ACTIVE_WINDOW_H` ∈ {24, 48, 72, 96}
- A threshold-vs-F1 plot, committed as PNG
- `config.py` with the winning values and a comment citing the number

**Do NOT**
- Label more than 600 articles. Diminishing returns are steep.
- Optimize past F1 = 0.85. **Stop at 0.85.** Further gains are not worth the weeks they cost.
- Build a labeling UI. CSV in a spreadsheet is correct.
- Start entity extraction, even if labeling finishes early. Use spare time to re-read the labels for consistency.

**GATE — do not proceed to week 4 until:**
- B-cubed F1 ≥ 0.80 on the gold set
- The plot exists and is committed
- `config.py` values are eval-justified

**If F1 < 0.70:** something structural is wrong. Change exactly one variable — the embedding model, or the embedding input construction (FR-402), or the domain. Not two. Re-measure. Budget one extra week for this and no more.

**If F1 is between 0.70 and 0.80:** ship it, record it honestly, proceed. The storyline layer is more interesting than the last 5 points here.

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
