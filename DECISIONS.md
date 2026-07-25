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
