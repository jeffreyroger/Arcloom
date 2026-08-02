import sqlite3

import numpy as np

import config
import pipeline.embed as embed_module
from pipeline.embed import _build_input_text, embed_pending


def _make_conn():
    conn = sqlite3.connect(":memory:")
    conn.execute(
        """
        CREATE TABLE article (
            id INTEGER PRIMARY KEY,
            title TEXT NOT NULL,
            snippet TEXT,
            embedding BLOB,
            embed_model TEXT
        )
        """
    )
    return conn


def _insert(conn, title, snippet=None, embedding=None, embed_model=None):
    cur = conn.execute(
        "INSERT INTO article (title, snippet, embedding, embed_model) VALUES (?, ?, ?, ?)",
        (title, snippet, embedding, embed_model),
    )
    conn.commit()
    return cur.lastrowid


# FR-402: input construction -- title + snippet, where `snippet` arrives
# already HTML-stripped and bounded to two sentences by
# pipeline.normalize.derive_snippet at ingest time.


def test_build_input_text_combines_title_and_snippet():
    assert _build_input_text("Title", "Snippet sentence.") == "Title Snippet sentence."


def test_build_input_text_handles_empty_summary():
    assert _build_input_text("Title only", "") == "Title only"
    assert _build_input_text("Title only", None) == "Title only"


def test_build_input_text_handles_single_sentence():
    assert _build_input_text("Title", "One sentence") == "Title One sentence"


def test_build_input_text_handles_no_terminal_punctuation():
    assert _build_input_text("Title", "no ending punctuation here") == "Title no ending punctuation here"


def test_build_input_text_handles_already_html_stripped_text():
    snippet = "Plain text with no markup."
    result = _build_input_text("Title", snippet)
    assert "<" not in result and ">" not in result


def test_embeddings_are_unit_norm():
    conn = _make_conn()
    _insert(conn, "OpenAI ships GPT-5", "Early reviews are positive.")

    embed_pending(conn)

    row = conn.execute("SELECT embedding FROM article").fetchone()
    vector = np.frombuffer(row[0], dtype=np.float32)
    assert vector.shape == (384,)
    assert abs(float(np.linalg.norm(vector)) - 1.0) < 1e-4


def test_identical_input_is_deterministic():
    # Two rows with identical text land in the same encode() batch; batched
    # matmul's reduction order depends on row position, so the raw float32
    # bytes can differ by floating-point noise even though the vectors are
    # the same direction (cosine similarity 1.0) -- confirmed separately
    # that the *same* input across two separate encode() calls is
    # bit-identical. Cosine similarity, not byte equality, is the invariant
    # that matters: it's what Level 1 clustering actually compares.
    conn = _make_conn()
    _insert(conn, "Same title", "Same snippet.")
    _insert(conn, "Same title", "Same snippet.")

    embed_pending(conn)

    rows = conn.execute("SELECT embedding FROM article ORDER BY id").fetchall()
    v0 = np.frombuffer(rows[0][0], dtype=np.float32)
    v1 = np.frombuffer(rows[1][0], dtype=np.float32)
    assert float(np.dot(v0, v1)) > 1 - 1e-6


def test_embed_model_persists():
    conn = _make_conn()
    _insert(conn, "A title", "A snippet.")

    embed_pending(conn)

    row = conn.execute("SELECT embed_model FROM article").fetchone()
    assert row[0] == config.EMBED_MODEL_NAME


def test_lazy_load_skips_when_nothing_pending(monkeypatch):
    conn = _make_conn()
    _insert(conn, "Already embedded", "Snippet.", embedding=b"x" * 4, embed_model=config.EMBED_MODEL_NAME)

    def _boom():
        raise AssertionError("model should not be loaded when nothing is pending")

    monkeypatch.setattr(embed_module, "_get_model", _boom)

    stats = embed_pending(conn)

    assert stats["embedded"] == 0
    assert stats["model_loaded"] is False
