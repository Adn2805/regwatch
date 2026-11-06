from datetime import datetime, timezone
from html import escape
from pathlib import Path

import pytest
from bs4 import BeautifulSoup

from app.crawl.adapters.base import AdapterParseError, SourceAdapter
from app.crawl.adapters.sebi import SebiAdapter

BASE = "https://www.sebi.gov.in/sebiweb/home/HomeAction.do?doListing=yes&sid=1&ssid=7&smid=0"
FIXTURE = Path(__file__).parents[1] / "fixtures" / "sebi_circulars_trimmed.html"


def row(
    date: str = "Nov 05, 2026",
    title: str = "Example circular",
    link: str = "/legal/circulars/example.html",
) -> str:
    return f'<tr><td>{escape(date)}</td><td><a href="{escape(link, quote=True)}">{escape(title)}</a></td></tr>'


def listing(rows: str) -> bytes:
    return (
        '<html><body><table id="sample_1"><thead><tr><th>Date</th><th>Title</th></tr></thead>'
        f"<tbody>{rows}</tbody></table></body></html>"
    ).encode()


def test_real_trimmed_fixture_through_existing_interface() -> None:
    adapter = SebiAdapter()
    generic: SourceAdapter = adapter
    publications = generic.discover(FIXTURE.read_bytes(), base_url=BASE)
    assert len(publications) == 5 and adapter.skipped_rows == 0
    assert publications[0].title == "Review of the framework for Accredited Investors"
    assert publications[0].canonical_url.endswith(
        "review-of-the-framework-for-accredited-investors_105618.html"
    )
    assert publications[0].published_at == datetime(2026, 11, 4, 18, 30, tzinfo=timezone.utc)
    assert publications[3].published_at == datetime(2026, 11, 3, 18, 30, tzinfo=timezone.utc)
    assert publications[4].title.endswith("Units of REITs")


def test_fixture_keeps_only_five_metadata_rows() -> None:
    content = FIXTURE.read_bytes()
    assert len(content) < 4096
    soup = BeautifulSoup(content, "html.parser")
    assert soup.select_one("div#ajax_cat table#sample_1") is not None
    assert [cell.get_text(strip=True) for cell in soup.select("thead th")] == ["Date", "Title"]
    assert len(soup.select("tbody tr")) == 5
    assert soup.find(["script", "form", "input", "iframe", "img"]) is None
    assert b"jsessionid" not in content.lower()


def test_empty_table_and_skip_count_reset() -> None:
    adapter = SebiAdapter()
    adapter.discover(listing(row(date="bad date")), base_url=BASE)
    assert adapter.skipped_rows == 1
    assert adapter.discover(listing(""), base_url=BASE) == []
    assert adapter.skipped_rows == 0


def test_missing_table_is_an_explicit_page_failure() -> None:
    with pytest.raises(AdapterParseError, match="sample_1 is missing"):
        SebiAdapter().discover(b"<html>not the expected listing</html>", base_url=BASE)


def test_bad_date_skips_only_the_bad_row() -> None:
    adapter = SebiAdapter()
    publications = adapter.discover(
        listing(row(date="not a date") + row(title="Good row")), base_url=BASE
    )
    assert [publication.title for publication in publications] == ["Good row"]
    assert adapter.skipped_rows == 1


@pytest.mark.parametrize(
    "link",
    [
        "https://outside.invalid/example.html",
        "https://sebi.gov.in.attacker.invalid/a",
        "javascript:alert(1)",
        "file:///notice",
        "https://user:password@www.sebi.gov.in/a",
    ],
)
def test_untrusted_link_is_skipped_without_losing_good_rows(link: str) -> None:
    adapter = SebiAdapter()
    publications = adapter.discover(listing(row(link=link) + row(title="Good row")), base_url=BASE)
    assert [publication.title for publication in publications] == ["Good row"]
    assert adapter.skipped_rows == 1


def test_canonical_duplicate_keeps_first_row_and_counts_skip() -> None:
    adapter = SebiAdapter()
    first = row(title="First", link="https://WWW.SEBI.GOV.IN:443/legal/circulars/a.html#section")
    duplicate = row(title="Duplicate", link="https://www.sebi.gov.in/legal/circulars/a.html")
    publications = adapter.discover(listing(first + duplicate), base_url=BASE)
    assert len(publications) == 1 and publications[0].title == "First"
    assert publications[0].canonical_url == "https://www.sebi.gov.in/legal/circulars/a.html"
    assert adapter.skipped_rows == 1


@pytest.mark.parametrize(
    "link",
    [
        "/legal/circulars/a.html?Id=AB&Mode=0#section",
        "../../legal/circulars/a.html?Id=AB&Mode=0#section",
    ],
)
def test_relative_links_preserve_query_and_remove_fragment(link: str) -> None:
    adapter = SebiAdapter()
    publications = adapter.discover(listing(row(link=link)), base_url=BASE)
    assert (
        publications[0].canonical_url
        == "https://www.sebi.gov.in/legal/circulars/a.html?Id=AB&Mode=0"
    )
    assert adapter.skipped_rows == 0


@pytest.mark.parametrize(
    "bad",
    [
        "<tr><td>Nov 05, 2026</td></tr>",
        "<tr><td>Nov 05, 2026</td><td>No link</td></tr>",
        row(title=" "),
    ],
)
def test_malformed_rows_are_counted_and_do_not_crash(bad: str) -> None:
    adapter = SebiAdapter()
    assert len(adapter.discover(listing(bad + row()), base_url=BASE)) == 1
    assert adapter.skipped_rows == 1


def test_offsite_base_url_is_rejected() -> None:
    with pytest.raises(AdapterParseError, match="SEBI hostname"):
        SebiAdapter().discover(listing(row()), base_url="https://outside.invalid/list")
