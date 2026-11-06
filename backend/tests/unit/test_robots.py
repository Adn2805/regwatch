import asyncio
import httpx
import pytest

from app.crawl.errors import RobotsDisallowed, SourcePaused
from app.crawl.robots import parse_robots
from conftest import RigFactory

URL = "https://regulator.example/notice"


def reply(status: int, body: bytes = b"") -> httpx.Response:
    return httpx.Response(status, stream=httpx.ByteStream(body))


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [404, 410])
async def test_missing_robots_allows_content(make_rig: RigFactory, status: int) -> None:
    rig = make_rig(
        lambda request: reply(status if request.url.path == "/robots.txt" else 200), None
    )
    assert (await rig.fetcher.fetch(URL)).http_status == 200
    assert [request.url.path for request, _ in rig.calls] == ["/robots.txt", "/notice"]


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [401, 403, 500, 502, 503, 504, 400, 304])
async def test_unavailable_robots_pauses_without_fetching_document(
    make_rig: RigFactory,
    status: int,
) -> None:
    rig = make_rig(lambda request: reply(status), None)
    with pytest.raises(SourcePaused):
        await rig.fetcher.fetch(URL)
    assert all(request.url.path == "/robots.txt" for request, _ in rig.calls)
    expected = 3 if status >= 500 else 1
    assert len(rig.calls) == expected


@pytest.mark.asyncio
@pytest.mark.parametrize("error", [httpx.ReadTimeout, httpx.ConnectError])
async def test_robots_timeout_or_network_error_pauses(
    make_rig: RigFactory,
    error: type[httpx.TransportError],
) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise error("Transport failed", request=request)

    rig = make_rig(handler, None)
    with pytest.raises(SourcePaused):
        await rig.fetcher.fetch(URL)
    assert len(rig.calls) == 3
    assert all(request.url.path == "/robots.txt" for request, _ in rig.calls)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "body",
    [
        b"<html>service error</html>",
        b"nonsense",
        b"\xff",
        b"Disallow: /",
        b"User-agent: *\nCrawl-delay: nan",
        b"User-agent: *\nCrawl-delay: -1",
    ],
)
async def test_unparseable_robots_pauses(make_rig: RigFactory, body: bytes) -> None:
    rig = make_rig(lambda request: reply(200, body), None)
    with pytest.raises(SourcePaused):
        await rig.fetcher.fetch(URL)
    assert len(rig.calls) == 1


@pytest.mark.asyncio
async def test_disallow_prevents_document_request(make_rig: RigFactory) -> None:
    rig = make_rig(lambda request: reply(200, b"User-agent: *\nDisallow: /notice"), None)
    with pytest.raises(RobotsDisallowed):
        await rig.fetcher.fetch(URL)
    assert len(rig.calls) == 1


@pytest.mark.asyncio
async def test_policy_cache_reuses_then_refreshes_at_ttl(make_rig: RigFactory) -> None:
    policies = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal policies
        if request.url.path == "/robots.txt":
            policies += 1
            return reply(200, b"User-agent: *\nDisallow: /" if policies == 2 else b"")
        return reply(200)

    rig = make_rig(handler, {"robots_cache_ttl_s": 10})
    await rig.fetcher.fetch(URL)
    await rig.fetcher.fetch(URL)
    assert policies == 1
    rig.clock.time = 12
    with pytest.raises(RobotsDisallowed):
        await rig.fetcher.fetch(URL)
    assert policies == 2


@pytest.mark.asyncio
async def test_paused_policy_is_cached_then_recovered(make_rig: RigFactory) -> None:
    available = False
    rig = make_rig(lambda request: reply(200 if available else 403), {"robots_cache_ttl_s": 10})
    for _ in range(2):
        with pytest.raises(SourcePaused):
            await rig.fetcher.fetch(URL)
    assert len(rig.calls) == 1
    available = True
    rig.clock.time = 10
    await rig.fetcher.fetch(URL)
    assert len(rig.calls) == 3


@pytest.mark.asyncio
async def test_cache_isolated_by_scheme_host_and_port(make_rig: RigFactory) -> None:
    rig = make_rig(lambda request: reply(200), None)
    for url in [
        URL,
        "http://regulator.example/notice",
        "https://other.example/notice",
        "https://regulator.example:8443/notice",
    ]:
        await rig.fetcher.fetch(url)
    policies = [request for request, _ in rig.calls if request.url.path == "/robots.txt"]
    assert len(policies) == 4


@pytest.mark.asyncio
@pytest.mark.parametrize(("configured", "advertised", "expected"), [(2, "0.5", 2), (2, "4.5", 4.5)])
async def test_larger_crawl_delay_controls_spacing(
    make_rig: RigFactory,
    configured: float,
    advertised: str,
    expected: float,
) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return reply(200, f"User-agent: *\nCrawl-delay: {advertised}".encode())

    rig = make_rig(handler, {"crawl_delay_s": configured})
    await rig.fetcher.fetch(URL)
    await rig.fetcher.fetch(URL)
    times = [time for _, time in rig.calls]
    assert times == [0, expected, 2 * expected]


@pytest.mark.asyncio
async def test_oversized_robots_pauses(make_rig: RigFactory) -> None:
    rig = make_rig(lambda request: reply(200, b"#" * 20), {"crawl_max_response_bytes": 10})
    with pytest.raises(SourcePaused):
        await rig.fetcher.fetch(URL)
    assert len(rig.calls) == 1


def test_matching_precedence_wildcards_and_specific_agent() -> None:
    policy = parse_robots(
        b"User-agent: *\nDisallow: /\nUser-agent: RegWatch\n"
        b"Disallow: /private/*\nAllow: /private/public$\nCrawl-delay: 3.5"
    )
    assert policy.allows("https://regulator.example/notice")
    assert not policy.allows("https://regulator.example/private/secret")
    assert policy.allows("https://regulator.example/private/public")
    assert policy.crawl_delay_s == 3.5


def test_empty_or_comment_only_policy_allows() -> None:
    assert parse_robots(b"# nothing restricted\n").allows(URL)


@pytest.mark.asyncio
async def test_concurrent_fetches_share_policy_load_and_host_pacing(make_rig: RigFactory) -> None:
    rig = make_rig(lambda request: reply(200), None)
    await asyncio.gather(rig.fetcher.fetch(URL), rig.fetcher.fetch(URL))
    assert [request.url.path for request, _ in rig.calls].count("/robots.txt") == 1
    assert [time for _, time in rig.calls] == [0, 2, 4]


@pytest.mark.asyncio
async def test_retry_rechecks_policy_after_cache_expiry(make_rig: RigFactory) -> None:
    policies = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal policies
        if request.url.path == "/robots.txt":
            policies += 1
            return reply(200, b"User-agent: *\nDisallow: /" if policies > 1 else b"")
        return httpx.Response(503, headers={"Retry-After": "10"}, stream=httpx.ByteStream(b""))

    rig = make_rig(handler, {"robots_cache_ttl_s": 5})
    with pytest.raises(RobotsDisallowed):
        await rig.fetcher.fetch(URL)
    assert policies == 2
    assert [request.url.path for request, _ in rig.calls].count("/notice") == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [code for code in range(400, 500) if code not in {404, 410}])
async def test_all_other_4xx_pause_with_cached_status_reason(
    make_rig: RigFactory, status: int
) -> None:
    rig = make_rig(lambda request: reply(status), {"crawl_max_retries": 0})
    for _ in range(2):
        with pytest.raises(SourcePaused) as captured:
            await rig.fetcher.fetch(URL)
        assert str(captured.value) == f"Source paused: robots.txt returned HTTP {status}"
    assert [request.url.path for request, _ in rig.calls] == ["/robots.txt"]


@pytest.mark.asyncio
@pytest.mark.parametrize("disallow", [False, True])
async def test_robots_only_load_never_fetches_a_content_page(
    make_rig: RigFactory, disallow: bool
) -> None:
    body = b"User-agent: *\nDisallow: /" if disallow else b"User-agent: *\nAllow: /"
    rig = make_rig(
        lambda request: reply(200, body), {"crawl_max_retries": 0, "crawl_max_redirects": 0}
    )
    policy = await rig.fetcher.robots_policy(URL)
    assert policy.allows(URL) is not disallow
    assert [request.url.path for request, _ in rig.calls] == ["/robots.txt"]
