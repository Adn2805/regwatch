import argparse
from pathlib import Path

import pytest
from pydantic import SecretStr

from app.core.config import FetchSettings
from app.crawl.adapters.rbi import RbiAdapter
from app.crawl.cli import FEED_URL, document_limit, require_real_contact, save_feed_sample


@pytest.mark.parametrize("value", ["0", "101", "no", "-1"])
def test_bad_document_limit_is_rejected(value: str) -> None:
    with pytest.raises(argparse.ArgumentTypeError):
        document_limit(value)


def test_document_limit_boundaries() -> None:
    assert document_limit("1") == 1
    assert document_limit("100") == 100


def test_placeholder_contact_prevents_live_run() -> None:
    with pytest.raises(ValueError, match="non-placeholder"):
        require_real_contact(
            FetchSettings(crawl_contact_email=SecretStr("contact@example.invalid"))
        )


def test_feed_trimming_keeps_rss_structure(tmp_path: Path) -> None:
    fixture = Path(__file__).parents[1] / "fixtures" / "rbi_notifications.xml"
    destination = tmp_path / "feed.xml"
    save_feed_sample(fixture.read_bytes(), destination, 1)
    publications = RbiAdapter().discover(destination.read_bytes(), base_url=FEED_URL)
    assert len(publications) == 1
    assert publications[0].title == "Example KYC & reporting notification"


@pytest.mark.parametrize(
    "url",
    [
        "https://www.sebi.gov.in/sebi_data/attachdocs/test.pdf",
        "https://www.sebi.gov.in/legal/circulars/test.pdf",
        "https://outside.invalid/legal/circulars/test.html",
    ],
)
def test_live_sebi_blocks_unapproved_destinations(url: str) -> None:
    import httpx
    from app.crawl.cli import check_sebi_request
    from app.crawl.errors import FetchError

    with pytest.raises(FetchError):
        check_sebi_request(httpx.URL(url))


def test_live_sebi_allows_only_listing_robots_and_circular_html() -> None:
    import httpx
    from app.crawl.cli import SEBI_URL, check_sebi_request

    for url in [
        SEBI_URL,
        "https://www.sebi.gov.in/robots.txt",
        "https://www.sebi.gov.in/legal/circulars/test.html",
    ]:
        check_sebi_request(httpx.URL(url))
