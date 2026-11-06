from collections.abc import AsyncIterator

import httpx
import pytest

from app.core.config import FetchSettings
from app.crawl.conditional import Validators
from app.crawl.errors import FetchError
from app.crawl.fetcher import Fetcher
from conftest import FakeClock

URL = "https://regulator.example/notice"
ETAG = 'W/"weak-value"'
MODIFIED = "Thu, 05 Nov 2026 00:00:00 GMT"


@pytest.mark.asyncio
@pytest.mark.parametrize("stored_url", ["https://other.example/notice", URL + "?different=1", None])
async def test_mismatched_validator_url_sends_no_conditional_headers(
    stored_url: str | None,
) -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(
            404 if request.url.path == "/robots.txt" else 200, stream=httpx.ByteStream(b"ok")
        )

    clock = FakeClock()
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        fetcher = Fetcher(
            client, FetchSettings(crawl_max_retries=0), clock=clock.now, sleep=clock.sleep
        )
        await fetcher.fetch(URL, validators=Validators(stored_url, ETAG, MODIFIED))
    assert all(
        "if-none-match" not in r.headers and "if-modified-since" not in r.headers for r in seen
    )


@pytest.mark.asyncio
async def test_redirect_does_not_forward_validators_to_another_url() -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        if request.url.path == "/robots.txt":
            return httpx.Response(404)
        if str(request.url) == URL:
            return httpx.Response(302, headers={"Location": "/other"})
        return httpx.Response(200, stream=httpx.ByteStream(b"ok"))

    clock = FakeClock()
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        fetcher = Fetcher(
            client, FetchSettings(crawl_max_retries=0), clock=clock.now, sleep=clock.sleep
        )
        await fetcher.fetch(URL, validators=Validators(URL, ETAG, MODIFIED))
    notice = next(r for r in seen if str(r.url) == URL)
    assert notice.headers["if-none-match"] == ETAG
    assert notice.headers["if-modified-since"] == MODIFIED
    assert all("if-none-match" not in r.headers for r in seen if str(r.url) != URL)


@pytest.mark.asyncio
async def test_unsolicited_304_is_an_error() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(404 if request.url.path == "/robots.txt" else 304)

    clock = FakeClock()
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        fetcher = Fetcher(
            client, FetchSettings(crawl_max_retries=0), clock=clock.now, sleep=clock.sleep
        )
        with pytest.raises(FetchError, match="304.*conditional"):
            await fetcher.fetch(URL)


@pytest.mark.asyncio
async def test_matching_weak_etag_returns_304_without_reading_a_body() -> None:
    class ForbiddenBody(httpx.AsyncByteStream):
        async def __aiter__(self) -> AsyncIterator[bytes]:
            raise AssertionError("304 body must never be read")
            yield b""

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/robots.txt":
            assert "if-none-match" not in request.headers
            return httpx.Response(404)
        assert request.headers["if-none-match"] == ETAG
        return httpx.Response(304, headers={"ETag": ETAG}, stream=ForbiddenBody())

    clock = FakeClock()
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        fetcher = Fetcher(
            client, FetchSettings(crawl_max_retries=0), clock=clock.now, sleep=clock.sleep
        )
        result = await fetcher.fetch(URL, validators=Validators(URL, ETAG, None))
    assert result.http_status == 304 and result.content == b""
