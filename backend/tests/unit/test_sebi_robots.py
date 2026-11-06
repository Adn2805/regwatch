"""Use SEBI's exact captured policy; these tests never request remote resources."""

from pathlib import Path

import pytest

from app.crawl.robots import parse_robots

ORIGIN = "https://www.sebi.gov.in"
FIXTURE = Path(__file__).parents[1] / "fixtures" / "sebi_robots.txt"


@pytest.mark.parametrize(
    ("path", "allowed"),
    [
        ("/legal.html", True),
        ("/sebi_data/attachdocs/example-circular.pdf", True),
        ("/js/x", False),
        ("/css/x", False),
        ("/hindi/js/x", False),
        ("/hindi/css/x", False),
    ],
)
def test_exact_sebi_robots_empty_disallow_and_restricted_prefixes(path: str, allowed: bool) -> None:
    content = FIXTURE.read_bytes()
    assert b"Disallow:" in [line.strip() for line in content.splitlines()]
    policy = parse_robots(content)
    assert policy.allows(ORIGIN + path) is allowed


@pytest.mark.parametrize(
    "path", ["/legal.html", "/sebi_data/attachdocs/example-circular.pdf", "/js/x"]
)
def test_empty_disallow_by_itself_allows_every_path(path: str) -> None:
    policy = parse_robots(b"User-agent: *\nDisallow:\n")
    assert policy.allows(ORIGIN + path)
