"""AC-10 compliance check: article.snippet must never read as body text.
Read-only, standalone. Reports snippets over 400 chars (well above the
2-sentence/300-char bound in derive_snippet) and samples the longest.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import db  # noqa: E402

_FLAG_THRESHOLD = 400


def main() -> int:
    conn = db.get_connection()

    rows = conn.execute(
        "SELECT id, LENGTH(snippet) AS len FROM article WHERE snippet IS NOT NULL AND LENGTH(snippet) > ?",
        (_FLAG_THRESHOLD,),
    ).fetchall()

    max_len_row = conn.execute("SELECT MAX(LENGTH(snippet)) FROM article").fetchone()
    max_len = max_len_row[0] if max_len_row else None

    print(f"Snippets over {_FLAG_THRESHOLD} chars: {len(rows)}")
    print(f"Max snippet length overall: {max_len}")

    longest = conn.execute(
        "SELECT id, snippet FROM article WHERE snippet IS NOT NULL "
        "ORDER BY LENGTH(snippet) DESC LIMIT 3"
    ).fetchall()

    print("\nSample of the 3 longest snippets:")
    for article_id, snippet in longest:
        print(f"  id={article_id} len={len(snippet)}: {snippet!r}")

    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
