"""Fixture-tested RBI RSS discovery. Live feed compatibility is not yet verified."""

from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from urllib.parse import urlsplit
from xml.etree.ElementTree import Element, ParseError

from defusedxml import ElementTree  # type: ignore[import-untyped]
from defusedxml.common import DefusedXmlException  # type: ignore[import-untyped]

from app.crawl.adapters.base import AdapterParseError, Publication, canonicalize_url

MAX_FEED_BYTES = 2 * 1024 * 1024


def _name(element: Element) -> str:
    return element.tag.rsplit("}", 1)[-1]


def _children(element: Element, name: str) -> list[Element]:
    return [child for child in element if _name(child) == name]


def _field(item: Element, name: str) -> str:
    fields = _children(item, name)
    if len(fields) > 1:
        raise AdapterParseError(f"RSS item contains multiple {name} fields")
    if not fields:
        return ""
    return " ".join("".join(fields[0].itertext()).split())


def _rbi_url(value: str, *, base_url: str) -> str:
    url = canonicalize_url(value, base_url=base_url)
    host = urlsplit(url).hostname
    if host is None or not (host == "rbi.org.in" or host.endswith(".rbi.org.in")):
        raise AdapterParseError("RBI publication URL must use an RBI hostname")
    return url


def _published_at(value: str) -> datetime | None:
    if not value:
        return None
    try:
        date = parsedate_to_datetime(value)
        if date.tzinfo is None:
            raise ValueError("Publication date needs a timezone")
        return date.astimezone(timezone.utc)
    except (ValueError, TypeError, OverflowError) as error:
        raise AdapterParseError("Invalid RSS publication date") from error


class RbiAdapter:
    def discover(self, content: bytes, *, base_url: str) -> list[Publication]:
        """Return unique publications; invalid entries fail the whole listing."""
        base_url = _rbi_url(base_url, base_url=base_url)
        if not content or len(content) > MAX_FEED_BYTES:
            raise AdapterParseError("RSS feed must contain between 1 byte and 2 MiB")
        try:
            root: Element = ElementTree.fromstring(content, forbid_dtd=True)
        except (ParseError, DefusedXmlException, LookupError) as error:
            raise AdapterParseError("Malformed or unsafe RSS XML") from error
        if _name(root) != "rss" or root.get("version") != "2.0":
            raise AdapterParseError("Expected an RSS 2.0 feed")
        channels = _children(root, "channel")
        if len(channels) != 1:
            raise AdapterParseError("RSS feed must contain exactly one channel")

        publications: list[Publication] = []
        seen: set[str] = set()
        for item in _children(channels[0], "item"):
            title = _field(item, "title")
            link = _field(item, "link")
            if not title or not link:
                raise AdapterParseError("RSS item requires a title and link")
            publication = Publication(
                canonical_url=_rbi_url(link, base_url=base_url),
                title=title,
                published_at=_published_at(_field(item, "pubDate")),
            )
            if publication.canonical_url not in seen:
                publications.append(publication)
                seen.add(publication.canonical_url)
        return publications
