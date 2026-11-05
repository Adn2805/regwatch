from dataclasses import FrozenInstanceError
from datetime import datetime, timezone
from pathlib import Path

import pytest

from app.crawl.adapters.base import (
    AdapterParseError,
    Publication,
    SourceAdapter,
    canonicalize_url,
)
from app.crawl.adapters.rbi import MAX_FEED_BYTES, RbiAdapter

BASE_URL = "https://www.rbi.org.in/notifications_rss.xml"
FIXTURE = Path(__file__).parents[1] / "fixtures" / "rbi_notifications.xml"


def feed(item: str) -> bytes:
    return f'<rss version="2.0"><channel>{item}</channel></rss>'.encode()


def test_rbi_fixture_through_generic_interface() -> None:
    adapter: SourceAdapter = RbiAdapter()
    publications = adapter.discover(FIXTURE.read_bytes(), base_url=BASE_URL)
    assert publications == [
        Publication(
            canonical_url="https://www.rbi.org.in/Scripts/NotificationUser.aspx?Id=10001&Mode=0",
            title="Example KYC & reporting notification",
            published_at=datetime(2026, 11, 5, 4, 30, tzinfo=timezone.utc),
        ),
        Publication(
            canonical_url="https://www.rbi.org.in/Scripts/NotificationUser.aspx?Id=10002",
            title="Example second notification",
        ),
    ]


def test_publication_is_immutable() -> None:
    publication = Publication(canonical_url=BASE_URL, title="Example")
    with pytest.raises(FrozenInstanceError):
        setattr(publication, "title", "Changed")


def test_empty_valid_feed_has_no_publications() -> None:
    assert RbiAdapter().discover(feed(""), base_url=BASE_URL) == []


def test_default_namespace_and_unicode_title() -> None:
    content = (
        '<rss xmlns="urn:fixture:rss" version="2.0"><channel><item>'
        "<title> सूचना\n  Example </title><link>/notice</link>"
        "</item></channel></rss>"
    ).encode()
    assert RbiAdapter().discover(content, base_url=BASE_URL)[0].title == "सूचना Example"


@pytest.mark.parametrize(
    "item",
    [
        "<item><title>A</title></item>",
        "<item><link>/a</link></item>",
        "<item><title> </title><link>/a</link></item>",
        "<item><title>A</title><link>/a</link><link>/b</link></item>",
        "<item><title>A</title><link>/a</link><pubDate>yesterday</pubDate></item>",
        "<item><title>A</title><link>/a</link><pubDate>05 Nov 2026 10:00:00</pubDate></item>",
    ],
)
def test_invalid_item_is_not_silently_reported_as_empty(item: str) -> None:
    with pytest.raises(AdapterParseError):
        RbiAdapter().discover(feed(item), base_url=BASE_URL)


@pytest.mark.parametrize(
    "content",
    [
        b"",
        b"<rss>",
        b"<html>Service unavailable</html>",
        b'<?xml version="1.0" encoding="unknown-encoding"?><rss version="2.0"><channel/></rss>',
        b'<rss version="2.0"/>',
        b'<rss version="2.0"><channel/><channel/></rss>',
        b'<rss version="1.0"><channel/></rss>',
        b'<!DOCTYPE rss [<!ENTITY title "expanded">]><rss version="2.0"><channel/></rss>',
        b'<!DOCTYPE rss SYSTEM "https://example.invalid/external.dtd"><rss version="2.0"/>',
    ],
)
def test_invalid_or_unsafe_feed(content: bytes) -> None:
    with pytest.raises(AdapterParseError):
        RbiAdapter().discover(content, base_url=BASE_URL)


def test_feed_size_limit() -> None:
    with pytest.raises(AdapterParseError, match="2 MiB"):
        RbiAdapter().discover(b" " * (MAX_FEED_BYTES + 1), base_url=BASE_URL)


@pytest.mark.parametrize(
    "link",
    [
        "https://rbi.org.in.attacker.invalid/a",
        "https://example.invalid/a",
        "javascript:alert(1)",
        "file:///a",
        "https://user:password@www.rbi.org.in/a",
        "https://www.rbi.org.in:invalid/a",
        "https://www.rbi.org.in:99999/a",
        "https://www.rbi.org.in/a b",
        "//attacker.invalid/a",
    ],
)
def test_rbi_rejects_unsupported_or_untrusted_links(link: str) -> None:
    content = feed(f"<item><title>A</title><link>{link}</link></item>")
    with pytest.raises(AdapterParseError):
        RbiAdapter().discover(content, base_url=BASE_URL)


def test_non_rbi_base_url_is_rejected() -> None:
    with pytest.raises(AdapterParseError):
        RbiAdapter().discover(feed(""), base_url="https://example.invalid/feed")


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("../Notice?Id=AB&b=2&a=1#section", "https://example.org/Notice?Id=AB&b=2&a=1"),
        ("https://EXAMPLE.ORG:443", "https://example.org/"),
        ("http://EXAMPLE.ORG:80/a", "http://example.org/a"),
        ("https://EXAMPLE.ORG:8443/a", "https://example.org:8443/a"),
        ("https://[::1]:443/a", "https://[::1]/a"),
    ],
)
def test_generic_url_normalization(value: str, expected: str) -> None:
    assert canonicalize_url(value, base_url="https://example.org/list/feed") == expected
