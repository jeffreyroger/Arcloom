# Decisions

## FR-204 vs NFR-602: description-as-body

**The conflict.** FR-204 permits storing a feed-provided summary/description.
NFR-602 and AC-10 prohibit storing article body text, with no length
exemption. These collide when a publisher puts the entire article inside the
RSS `<description>` field — observed for 103 articles, truncated only by the
prior 2000-char cap (`SNIPPET_MAX_CHARS` in `pipeline/ingest.py`). A shorter
cap does not resolve this: a 500-char slice of a body is still a body.

**The distinction.** The conflict is resolved semantically, not by length:

- A **summary** is text the publisher authored to stand in for the article —
  an abstract, a dek, a blurb.
- A **body** is the article itself, or its opening, placed in the
  description field.

When the description *is* the article, storing it is not permitted by
FR-204. When it's a genuine summary, FR-204 permits it.

**The resolution.** `derive_snippet()` in `pipeline/normalize.py` bounds
every stored snippet to at most 2 sentences or 300 characters — whichever is
shorter — ending on a sentence or word boundary, never mid-sentence with an
ellipsis. Two sentences of prose cannot constitute "the article body" under
any reasonable reading, regardless of what the publisher put in the field.
This favors NFR-602 (the non-negotiable constraint) while preserving FR-204's
intent: a real summary is still kept, and a body structurally cannot be.

`tools/migrate_snippets.py` re-bounds every previously stored snippet to the
same rule and `tools/check_ac10.py` reports any snippet over 400 chars as a
standing compliance check.

## Week 1 criterion 1: accelerated soak substitution

**The original criterion.** Week 1's done-criteria require the pipeline to
"run 24 hours unattended with zero crashes." A real 24-hour window was not
practical to run and verify within the week: it consumes a full day of
wall-clock time to learn about a process that, by design, restarts from
scratch every 15 minutes and holds no state in memory between runs. Waiting
a day to find out whether a one-shot script crashes is a poor use of that
day when the same evidence can be produced faster.

**The decomposition.** Not every property a 24-hour run would exercise is
actually wall-clock-dependent:

- **Run-count-dependent (provable by compression).** `pipeline/run.py` is a
  one-shot process — it exits after every invocation, so cross-run *memory*
  leaks are structurally impossible. What can degrade over many runs is
  persistent state: `arcloom.db` growth, WAL checkpointing behavior,
  `run_log` accumulation, and the `robots_cache` expiry-and-refetch path.
  All of these are a function of *how many times the pipeline has run*, not
  of how much real time has passed between runs. 120 runs compressed into
  under two hours exercises this state machine more thoroughly than 96 runs
  spread over a real day would — more cache-expiry cycles, more run_log
  rows, more chances for a leak to show up.
- **Wall-clock-dependent (not provable by compression).** Diurnal
  publishing rhythm (news volume differs by time of day and day of week),
  multi-day accumulation effects, and the *real* 24-hour `ROBOTS_CACHE_TTL_H`
  expiry path only occur by actually waiting a day. No amount of rerunning
  the pipeline faster proves these.

**The substitute evidence.**

1. **120 fixture runs** (`tools/accelerated_soak.py`, `tools/mock_feed_server.py`)
   — a deterministic local feed set, 120 runs at 20-second intervals, with
   `ROBOTS_CACHE_TTL_H` overridden to 18 seconds (shorter than the interval)
   specifically to force the cache-expiry-and-refetch path within the run
   rather than waiting on the real 24h TTL. Result: 0 crashes, 0
   unexplained `run_log` gaps, 0 articles inserted after run 1 (idempotency
   holds), duration and WAL size both flat run-to-run, and the deliberately
   broken feed correctly reached `status='degraded'`. Full detail in
   `logs/soak_run_log.jsonl`.
2. **Retroactive yield analysis** (`tools/yield_analysis.py`) — criterion 3
   (500–2,000 articles/day) measured from `published_at` against the real
   production database instead of a dedicated observation window, since
   publication date is independent of when we happened to poll.
3. **12 real-feed continuity runs — not yet run.** The plan calls for a real
   3-hour, 12-run window against the actual (expanded) `feeds.yaml` under
   the real Windows Task Scheduler, as a check that the accelerated soak's
   findings hold outside the mock server too. This is blocked on feed
   expansion (`feeds.yaml` is still at 21 feeds, not the 60–80 target) and
   has not been executed. This entry will be updated with that run's result
   once it happens.

**What this explicitly does not cover.** The accelerated soak proves the
run-count-dependent properties above and nothing more. It does not
demonstrate:

- Diurnal publishing patterns (news volume/rhythm across a real day and
  night).
- Multi-day accumulation effects beyond what 120 runs' worth of state
  growth can show.
- The real 24-hour `ROBOTS_CACHE_TTL_H` expiry path. The 18-second override
  exercises the *mechanism* (cache miss → refetch → cache hit again) but is
  not identical to observing the actual 24-hour timer elapse under real
  conditions — mitigated, not proven equivalent.

**The commitment.** A genuine multi-day soak is scheduled for week 10,
alongside the VPS migration (the first point the pipeline runs somewhere
that isn't a personal machine subject to sleep/power-management
interference). Criterion 1 will be re-verified there against a real
multi-day, real-TTL window, and this entry will be updated with that
result rather than left standing on the accelerated substitute alone.
