"""One-time migration: bring existing article.snippet rows into compliance
with the FR-204/NFR-602/AC-10 resolution (see DECISIONS.md).

We no longer have the raw feed description for already-ingested articles —
only the previously stored (2000-char-capped) snippet. Re-deriving from a
stored snippet is not processing a stored body: applying the same 2-sentence
/ 300-char bound further shortens what's already there and cannot reconstruct
or preserve more of the original text. This does not undo prior storage of
excess text; it purges the excess going forward.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import db  # noqa: E402
from pipeline.normalize import derive_snippet  # noqa: E402


def main() -> int:
    conn = db.get_connection()

    rows = conn.execute("SELECT id, snippet, title FROM article WHERE snippet IS NOT NULL").fetchall()
    updated = 0

    for article_id, snippet, title in rows:
        new_snippet = derive_snippet(snippet, title) or None
        if new_snippet != snippet:
            conn.execute("UPDATE article SET snippet = ? WHERE id = ?", (new_snippet, article_id))
            updated += 1

    conn.commit()
    conn.close()

    print(f"Checked {len(rows)} snippets; shortened {updated}.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
