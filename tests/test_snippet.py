from pipeline.normalize import derive_snippet


def test_genuine_short_summary_passes_through_unchanged():
    raw = "OpenAI released GPT-5 today. Early reviews are positive."
    assert derive_snippet(raw, "OpenAI ships GPT-5") == raw


def test_full_article_body_reduced_to_two_sentences():
    raw = (
        "Sentence one here. Sentence two here. "
        "Sentence three here. Sentence four here."
    )
    assert derive_snippet(raw, "title") == "Sentence one here. Sentence two here."


def test_html_laden_description_tags_stripped():
    raw = "<p>Breaking: <b>Company X</b> announced today.</p> <p>More details soon.</p>"
    result = derive_snippet(raw, "title")
    assert "<" not in result and ">" not in result
    assert result == "Breaking: Company X announced today. More details soon."


def test_single_800_char_sentence_cut_at_word_boundary_near_300():
    words = ["word{}".format(i) for i in range(150)]
    raw = " ".join(words)
    assert len(raw) > 800

    result = derive_snippet(raw, "title")

    assert len(result) <= 300
    assert not result.endswith(" ")
    assert raw.startswith(result)
    # cut lands on a word boundary: the next character in the source (if any)
    # after the snippet is a space, not a mid-word continuation
    assert raw[len(result):len(result) + 1] in (" ", "")


def test_empty_description_returns_empty_string():
    assert derive_snippet("", "title") == ""
    assert derive_snippet(None, "title") == ""
