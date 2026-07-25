from datetime import datetime, timezone

from pipeline.normalize import canonicalize_url, normalize_timestamp

FETCHED_AT = "2026-07-23T12:00:00+00:00"


def test_canonicalize_strips_utm_and_fragment():
    url = "https://Example.com/Article-Name?utm_source=twitter&utm_medium=social&utm_campaign=launch#section2"
    assert canonicalize_url(url) == "https://example.com/Article-Name"


def test_canonicalize_identical_across_sources():
    url1 = "https://Example.com/Article-Name?utm_source=twitter&utm_medium=social&fbclid=abc"
    url2 = "https://example.com/Article-Name/?gclid=xyz&utm_campaign=summer"
    assert canonicalize_url(url1) == canonicalize_url(url2)


def test_canonicalize_root_path_preserved():
    assert canonicalize_url("https://Example.com/") == "https://example.com/"


def test_canonicalize_preserves_ref_as_load_bearing_param():
    # FR-207 only lists utm_*, fbclid, gclid, fragment for stripping. `ref`
    # is load-bearing on some sites (e.g. distinguishing content sections),
    # so stripping it can collapse genuinely distinct URLs and cause
    # false-positive dedup.
    url1 = "https://example.com/article?ref=homepage"
    url2 = "https://example.com/article?ref=newsletter"
    assert canonicalize_url(url1) != canonicalize_url(url2)


def test_canonicalize_google_news_wrapper():
    wrapped = (
        "https://news.google.com/rss/articles/CBMirest?"
        "url=https%3A%2F%2Fwww.example.com%2Fnews%2Fstory%3Futm_source%3Dgoogle"
        "&hl=en-US&gl=US&ceid=US:en"
    )
    assert canonicalize_url(wrapped) == "https://www.example.com/news/story"


def test_normalize_timestamp_with_offset():
    entry = {"published": "Mon, 01 Jan 2024 10:00:00 +0530"}
    ts, inferred = normalize_timestamp(entry, FETCHED_AT)
    assert ts == "2024-01-01T04:30:00Z"
    assert inferred is False


def test_normalize_timestamp_malformed_falls_back():
    entry = {"published": "not a real date"}
    ts, inferred = normalize_timestamp(entry, FETCHED_AT)
    assert ts == "2026-07-23T12:00:00Z"
    assert inferred is True


def test_normalize_timestamp_future_falls_back():
    entry = {"published": "2029-07-23T12:00:00Z"}
    ts, inferred = normalize_timestamp(entry, FETCHED_AT)
    assert ts == "2026-07-23T12:00:00Z"
    assert inferred is True


def test_normalize_timestamp_missing_field_falls_back():
    entry = {}
    ts, inferred = normalize_timestamp(entry, FETCHED_AT)
    assert ts == "2026-07-23T12:00:00Z"
    assert inferred is True
