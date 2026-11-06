"""SEBI first-page HTML discovery; parsing is local and performs no I/O."""

from datetime import datetime, timedelta, timezone
from urllib.parse import urlsplit

from bs4 import BeautifulSoup, Tag

from app.crawl.adapters.base import AdapterParseError, Publication, canonicalize_url

MAX_LISTING_BYTES = 2 * 1024 * 1024
INDIA_TIME = timezone(timedelta(hours=5, minutes=30))


def _sebi_url(value: str, *, base_url: str) -> str:
    url = canonicalize_url(value, base_url=base_url)
    host = urlsplit(url).hostname
    if host is None or not (host == "sebi.gov.in" or host.endswith(".sebi.gov.in")):
        raise AdapterParseError("SEBI publication URL must use a SEBI hostname")
    return url


class SebiAdapter:
    def __init__(self) -> None:
        # Diagnostic for the most recent discover call, not a durable crawl-run field.
        self.skipped_rows = 0

    def discover(self, content: bytes, *, base_url: str) -> list[Publication]:
        self.skipped_rows = 0
        base_url = _sebi_url(base_url, base_url=base_url)
        if not content or len(content) > MAX_LISTING_BYTES:
            raise AdapterParseError("SEBI listing must contain between 1 byte and 2 MiB")
        try:
            text = content.decode("utf-8-sig")
        except UnicodeError:
            raise AdapterParseError("SEBI listing must be valid UTF-8") from None
        soup = BeautifulSoup(text, "html.parser")
        table = soup.find("table", id="sample_1")
        if not isinstance(table, Tag):
            raise AdapterParseError("SEBI listing table sample_1 is missing")
        body = table.find("tbody", recursive=False)
        parent = body if isinstance(body, Tag) else table
        publications: list[Publication] = []
        seen: set[str] = set()
        for row in parent.find_all("tr", recursive=False):
            if not isinstance(row, Tag) or row.find("th") is not None:
                continue
            try:
                publication = self._row(row, base_url)
            except (ValueError, AdapterParseError):
                self.skipped_rows += 1
                continue
            if publication.canonical_url in seen:
                self.skipped_rows += 1
                continue
            seen.add(publication.canonical_url)
            publications.append(publication)
        return publications

    def _row(self, row: Tag, base_url: str) -> Publication:
        cells = [cell for cell in row.find_all("td", recursive=False) if isinstance(cell, Tag)]
        if len(cells) != 2:
            raise ValueError("SEBI row needs date and title cells")
        anchor = cells[1].find("a", href=True)
        if not isinstance(anchor, Tag):
            raise ValueError("SEBI row needs a publication link")
        href = anchor.get("href")
        if not isinstance(href, str):
            raise ValueError("SEBI row link must be a string")
        title_attribute = anchor.get("title")
        raw_title = (
            title_attribute
            if isinstance(title_attribute, str) and title_attribute.strip()
            else anchor.get_text(" ", strip=True)
        )
        title = " ".join(raw_title.replace("\u200b", "").replace("\ufeff", "").split())
        if not title:
            raise ValueError("SEBI row needs a non-empty title")
        # A date label has no time: midnight India is a stated approximation.
        published_at = datetime.strptime(cells[0].get_text(" ", strip=True), "%b %d, %Y")
        published_at = published_at.replace(tzinfo=INDIA_TIME).astimezone(timezone.utc)
        return Publication(
            canonical_url=_sebi_url(href, base_url=base_url), title=title, published_at=published_at
        )
