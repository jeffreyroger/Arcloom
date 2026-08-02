import sqlite3
from datetime import datetime, timedelta, timezone

import config
from pipeline.simhash import apply_bypass, compute_simhash, hamming_distance


def _make_conn():
    conn = sqlite3.connect(":memory:")
    conn.execute(
        """
        CREATE TABLE article (
            id INTEGER PRIMARY KEY,
            title TEXT NOT NULL,
            published_at TEXT NOT NULL,
            simhash INTEGER,
            embedding BLOB,
            embed_model TEXT
        )
        """
    )
    return conn


def _iso(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def _insert(conn, title, published_at, embedding=None, embed_model=None, simhash=None):
    cur = conn.execute(
        "INSERT INTO article (title, published_at, simhash, embedding, embed_model) VALUES (?, ?, ?, ?, ?)",
        (title, _iso(published_at), simhash, embedding, embed_model),
    )
    conn.commit()
    return cur.lastrowid


# FR-301: 64-bit SimHash over title character trigrams.


def test_identical_titles_have_zero_distance():
    a = compute_simhash("OpenAI ships GPT-5 today")
    b = compute_simhash("OpenAI ships GPT-5 today")
    assert hamming_distance(a, b) == 0


def test_one_word_difference_gives_small_distance():
    # A single word inserted into a realistic, wire-headline-length title --
    # the kind of minor rewording one outlet's syndication of another's copy
    # actually introduces. A short title (e.g. 4-5 words) is adversarial for
    # character-trigram SimHash: changing even one character there can flip
    # a large fraction of its ~20 total trigrams (expected SimHash math, not
    # a bug), so distance alone can't tell "near-duplicate" from "unrelated"
    # at that length.
    a = compute_simhash(
        "European Commission fines Google over antitrust violations in ad tech market"
    )
    b = compute_simhash(
        "European Commission fines Google over antitrust violations in the ad tech market"
    )
    assert 0 < hamming_distance(a, b) <= config.SIMHASH_HAMMING


def test_unrelated_titles_give_large_distance():
    a = compute_simhash("OpenAI ships GPT-5 today")
    b = compute_simhash("Local council approves new zoning ordinance")
    assert hamming_distance(a, b) > config.SIMHASH_HAMMING


# FR-302: active-window respect and the embedding-bypass copy.


def test_bypass_respects_72h_active_window():
    conn = _make_conn()
    now = datetime.now(timezone.utc)
    title = "Same headline word for word"

    # Outside the active window -- already embedded, but too old to be a
    # syndication match.
    _insert(
        conn,
        title,
        now - timedelta(hours=config.ACTIVE_WINDOW_H + 1),
        embedding=b"old-embedding-bytes",
        embed_model=config.EMBED_MODEL_NAME,
        simhash=compute_simhash(title),
    )
    # New arrival, same title, no embedding yet.
    new_id = _insert(conn, title, now)

    stats = apply_bypass(conn)

    row = conn.execute("SELECT embedding FROM article WHERE id = ?", (new_id,)).fetchone()
    assert row[0] is None  # too old to match -- not bypassed
    assert stats["bypassed"] == 0


def test_bypass_copies_embedding_from_in_window_match():
    conn = _make_conn()
    now = datetime.now(timezone.utc)
    title = "Regulator fines company over data breach"

    _insert(
        conn,
        title,
        now - timedelta(hours=1),
        embedding=b"original-embedding-bytes",
        embed_model=config.EMBED_MODEL_NAME,
        simhash=compute_simhash(title),
    )
    new_id = _insert(conn, title, now)

    stats = apply_bypass(conn)

    row = conn.execute("SELECT embedding, embed_model FROM article WHERE id = ?", (new_id,)).fetchone()
    assert row[0] == b"original-embedding-bytes"
    assert row[1] == config.EMBED_MODEL_NAME
    assert stats["bypassed"] == 1


def test_bypass_genuinely_skips_embedding(monkeypatch):
    # The point of FR-302's bypass is to avoid running the model at all for
    # a syndication duplicate. Patch pipeline.embed's model loader so the
    # test fails loudly if embed_pending ever tries to load it for an
    # article apply_bypass already satisfied.
    import pipeline.embed as embed_module

    def _boom():
        raise AssertionError("model should never load for a bypassed article")

    monkeypatch.setattr(embed_module, "_get_model", _boom)

    conn = _make_conn()
    conn.execute("ALTER TABLE article ADD COLUMN snippet TEXT")
    now = datetime.now(timezone.utc)
    title = "Central bank raises interest rates by half a point"

    _insert(
        conn,
        title,
        now - timedelta(hours=1),
        embedding=b"original-embedding-bytes",
        embed_model=config.EMBED_MODEL_NAME,
        simhash=compute_simhash(title),
    )
    _insert(conn, title, now)

    apply_bypass(conn)
    stats = embed_module.embed_pending(conn)  # must find nothing left pending

    assert stats["pending"] == 0
    assert stats["model_loaded"] is False
