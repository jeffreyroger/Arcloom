# Arc Loom — Project Brief

**Working name:** Arc Loom
**Type:** Personal tool, open-sourced. Not a startup, not a hackathon entry.
**Owner:** Solo developer
**Target duration:** 12 weeks, 8–12 hours per week
**Status:** Pre-week-1

---

## 1. One-paragraph description

Arc is a news system that tracks **stories over time** rather than surfacing new articles. It ingests RSS and API feeds, collapses duplicate coverage into single events, links those events into long-running storylines using an entity graph, and detects when a storyline has genuinely *developed* versus merely been re-covered. It only notifies you when something new actually happened. Every stage of the pipeline is measured against a hand-labeled evaluation set, and those numbers are published.

## 2. The problem it solves

Every news aggregator is optimized for *novelty*. They answer "what is new right now." None of them answer the question people actually have:

> "That thing I read about three weeks ago — did anything happen with it?"

Consequences of the novelty bias:

- A story you care about gets covered heavily on day 1, then the resolution arrives on day 40 and you never see it.
- Reading five articles about the same event feels like five units of information. It is one.
- Push notifications are tuned for engagement, so the marginal notification is noise, so users disable notifications entirely, so the genuinely important one never arrives.

Throughline inverts all three.

## 3. What this is *not*

Explicitly out of scope for the lifetime of this project:

- A business. No monetization, no growth targets, no user acquisition.
- A general-interest news replacement. It complements a news app; it does not replace one.
- A social product. No comments, sharing, feeds-of-friends, or public profiles.
- A summarization service. Summaries exist to support the storyline; the storyline is the product.
- A content host. Every card links out. Nothing is republished.

## 4. Market position (honest)

The aggregator market is saturated and consolidated. Feedly, Flipboard and SmartNews hold roughly 45% of global revenue. Google News, Apple News, Ground News, Particle and a long tail of digest startups fill the rest. Artifact — a near-identical concept built by Instagram's co-founder with a strong team — shut down after roughly a year, citing a market that was too small.

**Therefore: this project's value is not competitive differentiation. It is (a) personal utility and (b) demonstrated engineering depth.**

The one genuine product gap it targets is storyline continuity. Ground News owns bias/blindspot analysis. Feedly owns source control. Particle owns fast catch-up. Nobody owns "follow the event, not the topic," and the reason is that it is the hardest of the four to build — which is exactly why it is worth building as a portfolio artifact.

### Differentiation summary

| Competitor | Owns | Throughline's angle |
|---|---|---|
| Google News | Scale, personalization | Continuity over novelty |
| Ground News | Bias & blindspot analysis | Not attempting; complementary |
| Feedly | RSS source control | Automatic storyline threading |
| Particle | Fast daily catch-up | Long-horizon tracking |
| Newsletter digests | Curation | Measured, tunable, self-hosted |

## 5. Design principles

These resolve arguments during implementation. When a decision is ambiguous, the higher-numbered principle yields to the lower.

1. **Measured beats clever.** No threshold ships without an eval number behind it. If a feature cannot be measured, it is deferred until it can be.
2. **Silence is a feature.** The system's default action on any new article is to say nothing. Every notification must justify itself against a budget.
3. **Coverage is visible.** Never imply completeness. Show the user how many sources are being watched for each interest.
4. **Link out, always.** The summary exists to help the user decide whether to click. It never replaces the click.
5. **Config over code.** Scope, feeds, thresholds and packs live in data files. Adding a domain must never require a code change.
6. **Boring infrastructure.** SQLite and a cron job until proven insufficient with a measurement. No service the developer would have to operate.

## 6. Architecture summary

```
feeds.yaml
    │
    ▼
 ingest ──── httpx + feedparser, ETag/If-Modified-Since aware
    │
    ▼
 normalize ── dedupe by URL, canonicalize timestamps, strip trackers
    │
    ▼
 simhash pre-filter ── kill syndicated wire copy before vector math
    │
    ▼
 embed ────── sentence-transformers, local, CPU, free
    │
    ▼
 EVENT CLUSTER ── incremental centroid matching, 72h active window
    │             (Level 1: same event, cosine similarity)
    ▼
 entity extract ── NER: people, orgs, places, case IDs
    │
    ▼
 STORYLINE ─── incremental entity-graph linking, weeks-to-months
    │           (Level 2: same narrative, Jaccard-dominant scoring)
    ▼
 state summary ── running "facts established so far" per storyline
    │
    ▼
 DEVELOPMENT ── LLM delta detection: new info, or null
    │
    ├──► feed ranking ─── user interest vectors × breadth dial
    │
    └──► notification gate ── daily budget + percentile threshold
```

**The load-bearing insight:** Level 1 and Level 2 require different algorithms. Level 1 is semantic similarity. Level 2 is *not* — embeddings actively mislead there, because two stories about the same organization can be unrelated narratives while two stories in the same narrative can be semantically distant. Level 2 is driven by entity overlap with embedding similarity as a secondary signal only.

## 7. Cost model

Target: **under $5/month at single-user scale, under $50/month at 500 users.**

| Component | Approach | Cost |
|---|---|---|
| Ingestion | httpx, conditional GET | $0 |
| Embedding | Local sentence-transformers on CPU | $0 |
| Event clustering | numpy dot products | $0 |
| NER | spaCy pre-filter + LLM for entity canonicalization | Low |
| Cluster summarization | One LLM call **per cluster**, not per user | Dominant cost |
| Development detection | Folded into the same call | Marginal |
| Storage | SQLite on disk | $0 |
| Hosting | Single small VPS or home machine | ~$5/mo |

The critical amortization: **LLM work scales with the number of story clusters, not the number of users.** 200 clusters/day serving 1 user costs the same as 200 clusters/day serving 5,000 users. Any design that breaks this property is rejected.

## 8. Legal posture

Publisher litigation against AI summarization is active and escalating: coalitions of US newspapers suing OpenAI and Microsoft, News Corp litigating against Brave specifically over near-verbatim summaries, publishers restricting the Internet Archive to limit indirect scraping.

Binding rules for this project:

- **RSS, Atom and official APIs only.** No HTML scraping of article bodies.
- **Honor `robots.txt`.** Identify with a truthful, contactable User-Agent. Never spoof.
- **Never store or display full article text.** Title, snippet as provided by the feed, and link. Nothing else.
- **Summaries are generated from headlines, feed-provided snippets and cross-source agreement**, and are capped at two sentences per cluster.
- **The outbound link is the most prominent element** on every card.
- **No public hosting with open registration.** Personal use or invite-only. Risk is near zero for a personal tool and materially non-zero for a public service.
- If a publisher requests removal, the feed is removed from `feeds.yaml` within 24 hours. Document this in the README.

## 9. Success criteria

Graded honestly at week 12. The project succeeds if **all three** hold:

**S1 — Personal utility.** The developer opens the storyline view voluntarily at least four days a week during weeks 10–12, without prompting themselves to do so.

**S2 — Measured quality.** Published eval numbers meet or exceed:
- Event clustering B-cubed F1 ≥ 0.80
- Storyline linking B-cubed F1 ≥ 0.65
- Development detection precision ≥ 0.85 at recall ≥ 0.60
- Summary faithfulness ≥ 0.95 of claims supported

**S3 — Legible artifact.** A stranger with relevant skills can clone the repo, read the README, understand the two-level architecture, see the eval methodology and reproduce the numbers.

### Failure signals (declare and stop)

- End of week 3: clustering F1 cannot be tuned above 0.70. → The domain choice or embedding model is wrong. Change one, not both.
- End of week 6: storyline timelines for a topic the developer genuinely cares about are not interesting to read. → The core premise is wrong. Stop; write up what was learned. This is a legitimate outcome and a better artifact than a half-built app.
- Any week: more than two consecutive weeks over the time budget. → Scope has escaped. Cut the current week's stretch goals entirely.

## 10. Glossary

| Term | Definition |
|---|---|
| **Article** | One item from one feed. The atomic unit of ingestion. |
| **Event cluster** | A set of articles covering the same discrete event within a 72-hour window. |
| **Storyline** | An ordered sequence of event clusters belonging to one ongoing narrative, spanning weeks to months. |
| **State summary** | A running record of facts established by a storyline so far. Input to development detection. |
| **Development** | A new event cluster that adds information not present in the storyline's state summary. |
| **Significance** | A scalar score on a development, used to rank and to gate notifications. |
| **Interest** | A free-text string supplied by the user, stored with its embedding. |
| **Breadth** | A per-interest scalar controlling the significance threshold for inclusion. |
| **Feed pack** | A named, tagged group of feeds in `feeds.yaml`. The unit of scope expansion. |
| **B-cubed** | A clustering metric computing precision/recall per element, then averaging. Robust to cluster-size skew. |
