"""FR-301 through FR-303: near-duplicate (syndication) filtering via SimHash.

FR-302 describes two things: attach a syndication duplicate directly to its
match's cluster, and skip embedding computation for it. The cluster half
needs `cluster`/`article_cluster` (FR-501 through FR-506, pipeline/cluster.py)
which don't exist yet -- this module implements the embedding-bypass half
only, which is fully self-contained: instead of running the model on a
near-duplicate title, copy the matching in-window article's already-computed
embedding. Once Level 1 clustering exists, an article carrying a copied
embedding of a near-identical title+snippet lands in the same cluster via
ordinary centroid matching anyway, so no special-casing is needed there.
"""

import hashlib
import logging
import re
import sqlite3
from datetime import datetime, timedelta, timezone

import config

_log = logging.getLogger("arcloom.simhash")

_HASH_BITS = 64
_MASK_64 = (1 << _HASH_BITS) - 1

_WHITESPACE_RE = re.compile(r"\s+")


def _normalize_title(title: str) -> str:
    return _WHITESPACE_RE.sub(" ", (title or "").strip().lower())


def _trigrams(text: str) -> list[str]:
    if len(text) < 3:
        return [text] if text else []
    return [text[i : i + 3] for i in range(len(text) - 2)]


def _trigram_hash(trigram: str) -> int:
    digest = hashlib.blake2b(trigram.encode("utf-8"), digest_size=8).digest()
    return int.from_bytes(digest, "big")


# SQLite INTEGER is signed 64-bit; a simhash with the top bit set overflows
# sqlite3's bind check unless converted to its twos-complement signed form
# first. hamming_distance masks back to unsigned 64 bits before comparing,
# so the sign conversion is transparent to callers.
def _to_signed_64(x: int) -> int:
    x &= _MASK_64
    return x - (1 << 64) if x >= (1 << 63) else x


# FR-301: 64-bit SimHash over title character trigrams.
def compute_simhash(title: str) -> int:
    trigrams = _trigrams(_normalize_title(title))
    if not trigrams:
        return 0

    weights = [0] * _HASH_BITS
    for trigram in trigrams:
        h = _trigram_hash(trigram)
        for bit in range(_HASH_BITS):
            weights[bit] += 1 if (h >> bit) & 1 else -1

    result = 0
    for bit in range(_HASH_BITS):
        if weights[bit] > 0:
            result |= 1 << bit
    return _to_signed_64(result)


def hamming_distance(a: int, b: int) -> int:
    return bin((a ^ b) & _MASK_64).count("1")


def _compute_pending_simhashes(conn: sqlite3.Connection) -> int:
    rows = conn.execute("SELECT id, title FROM article WHERE simhash IS NULL").fetchall()
    for article_id, title in rows:
        conn.execute("UPDATE article SET simhash = ? WHERE id = ?", (compute_simhash(title), article_id))
    if rows:
        conn.commit()
    return len(rows)


# FR-302 (embedding-bypass half): the first in-window, already-embedded
# article within SIMHASH_HAMMING of `simhash`. Candidates are restricted to
# embed_model matching the current config -- a stale-model embedding would
# just be immediately re-embedded by pipeline.embed.embed_pending anyway
# (FR-405), so copying it would be wasted work.
def _find_syndication_match(conn: sqlite3.Connection, article_id: int, simhash: int, cutoff_iso: str):
    rows = conn.execute(
        """
        SELECT id, simhash, embedding, embed_model FROM article
        WHERE id != ? AND simhash IS NOT NULL AND embedding IS NOT NULL
          AND embed_model = ? AND published_at >= ?
        """,
        (article_id, config.EMBED_MODEL_NAME, cutoff_iso),
    ).fetchall()
    for candidate_id, candidate_simhash, embedding, embed_model in rows:
        if hamming_distance(simhash, candidate_simhash) <= config.SIMHASH_HAMMING:
            return candidate_id, embedding, embed_model
    return None


# FR-301/FR-302/FR-303: backfill-and-incremental entry point. Computes
# simhash for any article missing one, then -- for articles still missing an
# embedding -- copies a matching in-window article's embedding instead of
# leaving it for the model. Must run before pipeline.embed.embed_pending so
# bypassed rows are already satisfied by the time embed_pending selects its
# pending set. FR-303: bypass rate is logged every call, not just when
# nonzero, so a healthy run's 0% is as visible as a syndication spike.
def apply_bypass(conn: sqlite3.Connection) -> dict:
    computed = _compute_pending_simhashes(conn)

    cutoff_iso = (datetime.now(timezone.utc) - timedelta(hours=config.ACTIVE_WINDOW_H)).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )
    candidates = conn.execute(
        "SELECT id, simhash FROM article WHERE embedding IS NULL AND simhash IS NOT NULL"
    ).fetchall()

    bypassed = 0
    for article_id, simhash in candidates:
        match = _find_syndication_match(conn, article_id, simhash, cutoff_iso)
        if match is None:
            continue
        _, embedding, embed_model = match
        conn.execute(
            "UPDATE article SET embedding = ?, embed_model = ? WHERE id = ?",
            (embedding, embed_model, article_id),
        )
        bypassed += 1
    if candidates:
        conn.commit()

    rate = bypassed / len(candidates) if candidates else 0.0
    _log.info(
        f"simhash: computed={computed} candidates={len(candidates)} bypassed={bypassed} rate={rate:.3f}",
        extra={"stage": "simhash"},
    )
    return {"computed": computed, "candidates": len(candidates), "bypassed": bypassed, "rate": rate}
