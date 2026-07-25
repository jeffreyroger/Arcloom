# Arc Loom — Implementation Plan

**Duration:** 12 weeks
**Budget:** 8–12 hours/week. Hard ceiling 14.
**Structure:** Every week has a goal, a deliverable, an explicit prohibition list, a binary done-test, and tripwires.

---

## Global constraints

These apply to every week. Violating one is scope creep by definition, not a judgment call.

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

### The two failure modes this plan is designed against

**Building the fun parts first.** UI, notifications, mobile and accounts are all more enjoyable than tuning a similarity threshold. They are also all worthless if clustering is bad. The plan front-loads the unpleasant, load-bearing work.

**Infinite tuning.** Weeks 3 and 6 have both a floor (must hit) and a ceiling (stop when hit). Tuning past the ceiling is procrastination wearing a lab coat.

---

## Phase 1 — Foundation (weeks 1–3)

*Goal: a measured event-clustering pipeline. No AI, no users, no app.*

### Week 1 — Ingestion and schema

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

---

### Week 2 — Embedding and event clustering

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

---

### Week 3 — GATE: label, measure, tune

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

**Tripwires**
- Fourth consecutive tuning session → you are past the ceiling, move on
- Wanting to relabel because "the labels were wrong" → allowed exactly once

---

## Phase 2 — Storylines (weeks 4–6)

*Goal: clusters thread into narratives that persist for weeks.*

### Week 4 — Entity extraction

**Goal:** Every cluster carries a canonicalized, weighted entity set.

**Time budget:** 10h

**Deliverables**
- `pipeline/entities.py` — spaCy `en_core_web_sm` (upgrade to `_trf` only if measured necessary)
- `entity`, `entity_alias`, `cluster_entity` tables
- Canonicalization via alias table, hand-correctable
- Salience scoring: title occurrence weighted above snippet, times frequency across members
- Corpus-derived stoplist (FR-605) — entities above a document-frequency percentile within a pack

**Do NOT**
- Use an LLM for extraction this week (G2 still applies)
- Build entity-linking to Wikidata or any external knowledge base
- Attempt coreference resolution
- Hand-write the stoplist — derive it

**Done when**
- A spot check of 20 clusters shows correct entity sets for ≥16
- The alias table correctly collapses at least 5 real surface-form variants you can name
- Stoplist has removed the pack's obvious ubiquitous entities

**Tripwires**
- Spending time on entity types beyond the six required (FR-601)
- Building a UI to inspect entities — use `sqlite3` CLI

---

### Week 5 — Storyline linking

**Goal:** Clusters attach to storylines. This is the technical heart of the project.

**Time budget:** 12h (this week is allowed the ceiling)

**Deliverables**
- `pipeline/storylines.py` — incremental linking, live-storyline candidates only
- Scoring: `score = W_JACCARD·jaccard(entities) + W_EMBED·cosine(centroids) + W_RECENCY·decay(days)`, with `W_JACCARD` dominant
- `storyline`, `storyline_entity` tables
- Decayed union maintenance of storyline entity sets (FR-705)
- Dormancy transition at `STORYLINE_TTL_D`
- Operator merge/split CLI commands, recorded in `storyline_edit`

**Do NOT**
- Weight embeddings above entities. This is the single most likely mistake and it will quietly destroy quality — semantic similarity is uncorrelated with narrative identity.
- Reprocess history on each run
- Build a graph database or import `networkx` for what is a dict of sets
- Tune weights yet — that is week 6, with a gold set

**Done when**
- A storyline you can name by hand exists in the database with ≥3 clusters spanning ≥7 days
- Merge and split commands work and survive the next pipeline run
- Dormant storylines are excluded from candidate scoring, verified by a count in the run log

**Tripwires**
- Reaching for embeddings when linking looks wrong → the fix is almost always entity canonicalization, not weights
- More than 2h on the merge/split CLI

---

### Week 6 — GATE: storyline eval and timeline view

**Goal:** A second number, and the first view that shows the actual product idea.

**Time budget:** 11h (4h labeling, 3h scorer + ablation, 3h timeline view, 1h writeup)

**Deliverables**
- `evals/gold_storylines.csv` — cluster→storyline labels over a ≥30-day window
- `evals/score_storylines.py` — B-cubed plus pairwise link precision
- Ablation table: entity-only, embedding-only, and the blend. **Publish this** — it empirically justifies FR-702 and is the most interesting number in the repo.
- Sweep of `TAU_STORY` and the three weights
- `storyline.html` — chronological timeline per storyline

**Do NOT**
- Label more than 30 days of window
- Optimize past F1 = 0.75
- Add summaries to the timeline (that is week 7)
- Style the timeline beyond chronological legibility

**GATE — do not proceed to week 7 until:**
- Storyline B-cubed F1 ≥ 0.65
- The ablation table exists and is committed
- **You have read the timeline for a storyline you personally care about and formed an honest opinion**

**The real gate is that last bullet.** If the timeline is not interesting to read, the core premise of the project is wrong. That is a legitimate finding. Stop here, write up the architecture and the two eval numbers, and publish it as a negative result. That is a better artifact than a half-finished app and it will have cost 6 weeks, not 12.

---

## Phase 3 — Developments (weeks 7–8)

*Goal: the system knows the difference between news and noise.*

### Week 7 — Summarization and state summaries

**Goal:** First LLM integration. G2 lifts this week.

**Time budget:** 10h

**Deliverables**
- `pipeline/summarize.py` — one call per cluster, cached by cluster ID + member-set hash
- Two-sentence cap, generated from titles and feed snippets only
- Storyline `state_summary` maintenance with an 800-token budget
- Compaction call when the budget is exceeded, preserving resolved facts and open questions
- Cost telemetry: tokens and dollars per run, logged

**Do NOT**
- Summarize per user or per interest — this breaks cost amortization (NFR-202) and is the single most expensive possible mistake
- Use a frontier model. Use the cheapest model that passes a spot check.
- Build a prompt-management framework
- Add streaming, retries beyond one, or a queue

**Done when**
- 200 clusters summarize for under $0.20
- Re-running the pipeline makes zero redundant LLM calls
- A storyline's state summary reads as a coherent factual record
- Every rendered summary displays its outbound links (FR-806)

**Tripwires**
- Prompt iteration beyond 2h → good enough; week 8's eval will tell you what is actually wrong
- Monthly projected cost above $5 → stop and fix amortization before continuing

---

### Week 8 — GATE: development detection

**Goal:** The headline number and the feature that makes this project distinct.

**Time budget:** 12h

**Deliverables**
- Development detection folded into the week 7 call — structured delta or explicit null, no second call
- Null on any ambiguous or unparseable output (FR-902)
- `significance` scoring in [0, 1]
- `development` table with full detector input/output persisted
- `evals/gold_developments.csv` — ~200 attached clusters labeled development vs. rehash
- `evals/score_developments.py` — precision, recall, F1
- `evals/score_faithfulness.py` — LLM judge validated against ≥100 human-labeled items, agreement reported
- Timeline view marks which entries were developments

**Do NOT**
- Fine-tune anything
- Add a second LLM call (FR-903)
- Build a prompt A/B framework — run variants by hand, record results in a markdown table
- Chase recall at the cost of precision. Precision is the headline. Silence is the safe default.

**GATE — do not proceed to week 9 until:**
- Development detection precision ≥ 0.85 at recall ≥ 0.60
- Summary faithfulness ≥ 0.95
- All three eval scripts run from a clean clone

**If precision is below 0.80:** the state summary is probably too lossy. Increase the token budget before touching the prompt.

---

## Phase 4 — Reader experience (weeks 9–11)

*Goal: turn a pipeline into something usable. All the hard work is already done.*

### Week 9 — Interests, ranking, accounts

**Time budget:** 11h

**Deliverables**
- Minimal `reader` table, single-user, invite-only (no OAuth, no password reset flow)
- Free-text interests with embeddings; breadth dial in [0, 1]
- Explicit storyline following, bypassing interest filtering (FR-1004)
- ~15 starter packs that expand into editable interests, not opaque subscriptions
- Ranking function: interest similarity × significance × recency × source count, documented
- "Why am I seeing this" on every item
- Per-interest source count displayed (FR-1007)
- Feedback capture (persisted, not yet used for ranking)

**Do NOT**
- Build OAuth, email verification, or password reset
- Train any model on feedback
- Build a settings page beyond interests, breadth and budget
- Add a topic taxonomy as the primary input (FR-1001)

**Done when**
- Three interests at different breadth settings produce visibly different feeds
- Every item can explain its own presence
- A followed storyline surfaces its developments regardless of interest match

---

### Week 10 — Notification gate

**Time budget:** 10h

**Deliverables**
- Rolling 14-day significance distribution
- Percentile threshold auto-calibrated to hit `K` per day
- Hard daily budget enforcement
- Quiet hours with queueing
- Web Push via VAPID
- Notification history with significance and percentile at fire time
- "Why did I get this" on every notification

**Do NOT**
- Implement native mobile push
- Add notification categories, channels, or grouping
- Fire on topic matches — only developments notify (FR-1103)
- Raise the default budget above 2

**Done when**
- 14 consecutive days with budget never exceeded
- Quiet hours verifiably hold a notification and release it
- Every notification is explainable

---

### Week 11 — PWA and polish

**Time budget:** 10h. This is the week most likely to overrun. Watch it.

**Deliverables**
- Installable PWA: manifest, service worker, offline shell
- Feed view, storyline timeline, interests management — three screens, no more
- Actual visual design pass (first one permitted all project)
- Cost telemetry dashboard (a table is fine)
- Framework permitted this week if genuinely needed — but three static pages plus Jinja2 probably still is enough

**Do NOT**
- Add screens beyond the three
- Add dark mode, animations, onboarding flows, or empty-state illustrations
- Rewrite the pipeline "now that things are clearer"
- Exceed 12h. If the design is not done, ship it ugly.

---

## Phase 5 — Publication (week 12)

### Week 12 — The artifact

**Time budget:** 10h. Do not skip this week. It is where most of the portfolio value is realized.

**Deliverables**
- **README** with: the two-level architecture diagram, the problem statement, all eval numbers in one table, the ablation table, cost per reader per month, and honest limitations
- Every config constant documented with its justifying number (FR-1209, AC-9)
- `EVALS.md` — methodology, gold set construction, how to reproduce
- One-command reproduction path verified from a clean clone in a fresh venv
- A written post covering: why entity overlap beats embeddings for storyline linking, what the ablation showed, what the cost amortization argument is, and what did not work
- Live demo or recorded walkthrough
- Feed pack expansion to a second domain — proving the config-over-code claim (AC-7)

**Do NOT**
- Add features
- Hide bad numbers. A published F1 of 0.72 with honest methodology is more credible than 0.91 with none.
- Write a marketing README. Write an engineering one.

**Done when**
- A skilled stranger can clone, run, and reproduce every published number
- The limitations section is longer than the features section

---

## Summary table

| Wk | Phase | Focus | Gate | Budget |
|---|---|---|---|---|
| 1 | Foundation | Ingest + schema | | 10h |
| 2 | Foundation | Embed + cluster + render | | 10h |
| 3 | Foundation | **Label, measure, tune** | **F1 ≥ 0.80** | 11h |
| 4 | Storylines | Entity extraction | | 10h |
| 5 | Storylines | Storyline linking | | 12h |
| 6 | Storylines | **Eval + timeline** | **F1 ≥ 0.65 + honest read** | 11h |
| 7 | Developments | Summaries + state | | 10h |
| 8 | Developments | **Development detection** | **P ≥ 0.85 @ R ≥ 0.60** | 12h |
| 9 | Reader | Interests + ranking | | 11h |
| 10 | Reader | Notification gate | | 10h |
| 11 | Reader | PWA + polish | | 10h |
| 12 | Publication | README + writeup | | 10h |

**Total: 127 hours.**

## Slack policy

There is no slack week built in, deliberately — a slack week becomes a scope-expansion week. If you fall behind:

1. First, delete stretch goals from the current week.
2. Second, extend the current week and shift everything right. Do not compress a later week.
3. Third, if two weeks have slipped, cut week 11 entirely and ship without the PWA. The pipeline and the evals are the artifact; the PWA is not.

**Never cut weeks 3, 6, 8 or 12.** They are the entire value of the project.
