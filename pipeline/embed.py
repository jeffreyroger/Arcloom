"""FR-401 through FR-405: local CPU embedding of article title+snippet.

No embedding API calls (FR-401) -- week 3 re-embeds the corpus repeatedly
while tuning TAU_EVENT, and that has to be free. Input is title + snippet
(FR-402): `snippet` is already HTML-stripped and bounded to the first two
sentences of the feed description by pipeline.normalize.derive_snippet at
ingest time (see AC-10/DECISIONS.md "FR-204 vs NFR-602"), so this module
only combines it with the title -- it does not re-derive sentences from raw
HTML, which G9 wouldn't allow it to have anyway (only the bounded snippet is
ever stored, never the raw description). Title-only embedding is never a
design choice: headlines are too short and stylized for cross-source
matching, the entire basis of Level 1 event clustering. Vectors are
L2-normalized at write time (FR-403) so Level 1's cosine similarity is a
plain dot product, and stored as float32 BLOBs (FR-404) with the model name
that produced them (FR-405) -- a model change shows up as a mismatch and
gets recomputed rather than silently mixing vector spaces.

Lazy model loading: the ~2-5s model load is skipped entirely when there is
nothing pending, so an incremental run with a handful of new articles isn't
dominated by a load it didn't need.
"""

import logging
import os
import sqlite3
import time

import numpy as np

import config

_log = logging.getLogger("arcloom.embed")

_model = None  # lazy-loaded SentenceTransformer singleton, one per process


# FR-402: combine title and snippet for encoding. `snippet` may be empty
# (some feeds provide no description at all -- a real, unavoidable input,
# not a design choice to embed title-only), a single sentence, missing
# terminal punctuation, or any other shape derive_snippet can produce; all
# of those just concatenate cleanly.
def _build_input_text(title: str, snippet: str | None) -> str:
    title = (title or "").strip()
    snippet = (snippet or "").strip()
    if not snippet:
        return title
    return f"{title} {snippet}"


def _get_model():
    global _model
    if _model is None:
        # Progress bars write to sys.stderr unconditionally; under pythonw
        # (no console, see pipeline/run.py's guard) that raises AttributeError
        # on the None stream. Belt and braces: env vars here cover this
        # module's own model-loading progress bars (a separate code path from
        # encode()'s, which passes show_progress_bar=False explicitly below),
        # deferred until this point so importing pipeline.embed itself never
        # pays for it.
        os.environ.setdefault("HF_HUB_DISABLE_PROGRESS_BARS", "1")
        os.environ.setdefault("TQDM_DISABLE", "1")
        from sentence_transformers import SentenceTransformer

        t0 = time.perf_counter()
        _model = SentenceTransformer(config.EMBED_MODEL_NAME, device="cpu")
        load_s = time.perf_counter() - t0
        _log.info(
            f"model loaded model={config.EMBED_MODEL_NAME} load_s={load_s:.3f}",
            extra={"stage": "embed"},
        )
    return _model


# FR-401 through FR-405: embed every article missing a current embedding --
# embedding IS NULL (never embedded) or embed_model doesn't match
# config.EMBED_MODEL_NAME (FR-405: a model change invalidates and
# recomputes rather than mixing vector spaces). Returns stats for the
# caller to log/report; does not raise on an empty pending set -- that is
# the expected, common case for an incremental run.
def embed_pending(conn: sqlite3.Connection, batch_size: int | None = None, progress: bool = False) -> dict:
    batch_size = batch_size or config.EMBED_BATCH_SIZE

    rows = conn.execute(
        "SELECT id, title, snippet FROM article WHERE embedding IS NULL OR embed_model != ?",
        (config.EMBED_MODEL_NAME,),
    ).fetchall()

    stats = {"pending": len(rows), "embedded": 0, "model_loaded": False, "load_s": 0.0, "encode_s": 0.0}

    if not rows:
        _log.info("embed: nothing pending, model not loaded", extra={"stage": "embed"})
        return stats

    t0 = time.perf_counter()
    model = _get_model()
    stats["load_s"] = time.perf_counter() - t0
    stats["model_loaded"] = True

    t_encode = time.perf_counter()
    for start in range(0, len(rows), batch_size):
        batch = rows[start : start + batch_size]
        texts = [_build_input_text(row[1], row[2]) for row in batch]
        vectors = model.encode(
            texts, batch_size=batch_size, normalize_embeddings=True, show_progress_bar=False
        )
        vectors = np.asarray(vectors, dtype=np.float32)
        for row, vector in zip(batch, vectors):
            conn.execute(
                "UPDATE article SET embedding = ?, embed_model = ? WHERE id = ?",
                (vector.tobytes(), config.EMBED_MODEL_NAME, row[0]),
            )
        conn.commit()
        stats["embedded"] += len(batch)
        if progress:
            print(f"embedded {stats['embedded']}/{stats['pending']}")

    stats["encode_s"] = time.perf_counter() - t_encode
    rate = stats["embedded"] / stats["encode_s"] if stats["encode_s"] > 0 else float("inf")
    _log.info(
        f"embed: embedded={stats['embedded']} load_s={stats['load_s']:.3f} "
        f"encode_s={stats['encode_s']:.3f} rate={rate:.1f}/s",
        extra={"stage": "embed"},
    )
    return stats


# Manual backfill entry point (`python -m pipeline.embed [--db PATH]`), run
# from a real console -- progress output here is fine, unlike the scheduled
# pythonw run. Reports the metrics needed to sanity-check NFR-102 (>200
# articles/sec) and the ~2MB size estimate for 1,441 articles x 384 dims x 4
# bytes before trusting this at scale.
def main() -> int:
    import argparse
    import sys
    from pathlib import Path

    import db

    parser = argparse.ArgumentParser(description="Backfill embeddings for existing articles")
    parser.add_argument("--db", type=Path, default=None, help="Alternate database path (default: arcloom.db)")
    args = parser.parse_args()

    db_path = args.db or db.DB_PATH
    conn = db.get_connection(db_path)
    conn.row_factory = None

    size_before = Path(db_path).stat().st_size if Path(db_path).exists() else 0
    t0 = time.perf_counter()
    stats = embed_pending(conn, progress=True)
    wall_s = time.perf_counter() - t0
    conn.close()
    size_after = Path(db_path).stat().st_size if Path(db_path).exists() else 0

    print(f"\npending={stats['pending']} embedded={stats['embedded']}")
    print(f"model_load_s={stats['load_s']:.3f}")
    print(f"wall_s={wall_s:.3f}")
    if stats["embedded"] and stats["encode_s"] > 0:
        print(f"articles/sec={stats['embedded'] / stats['encode_s']:.1f} (NFR-102 requires >200)")
    delta = size_after - size_before
    print(f"db_size_delta={delta} bytes ({delta / 1e6:.2f} MB)")

    return 0


if __name__ == "__main__":
    import sys

    sys.exit(main())
