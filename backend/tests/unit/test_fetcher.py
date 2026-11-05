import logging
import traceback
from collections.abc import AsyncIterator
from datetime import datetime, timezone

import httpx
import pytest
from pydantic import SecretStr, ValidationError

from app.core.config import FetchSettings
from app.crawl.errors import FetchError, ResponseTooLarge, RobotsDisallowed, SourcePaused
from app.crawl.fetcher import retry_after_seconds
from conftest import RigFactory

URL = "https://regulator.example/notice"


def reply(status: int = 200, body: bytes = b"", **headers: str) -> httpx.Response:
    return httpx.Response(status, stream=httpx.ByteStream(body), headers=headers)


@pytest.mark.asyncio
async def test_success_user_agent_timeout_and_pacing(make_rig: RigFactory) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return reply(404) if request.url.path == "/robots.txt" else reply(200, b"circular")

    rig = make_rig(handler, None)
    result = await rig.fetcher.fetch(URL)
    assert result.content == b"circular"
    assert result.url == URL
    assert result.http_status == 200
    assert [time for _, time in rig.calls] == [0, 2]
    for request, _ in rig.calls:
        assert request.headers["user-agent"] == "RegWatch/0.1 (+mailto:tester@example.invalid)"
        assert request.headers["accept-encoding"] == "identity"
        assert set(request.extensions["timeout"].values()) == {3.0}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "error", [httpx.ReadTimeout, httpx.ConnectError, httpx.RemoteProtocolError]
)
async def test_transport_failure_retries_then_succeeds(
    make_rig: RigFactory,
    error: type[httpx.TransportError],
) -> None:
    attempts = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        if request.url.path == "/robots.txt":
            return reply(404)
        attempts += 1
        if attempts < 3:
            raise error("failure", request=request)
        return reply(200, b"ok")

    rig = make_rig(handler, None)
    assert (await rig.fetcher.fetch(URL)).content == b"ok"
    assert attempts == 3
    times = [time for request, time in rig.calls if request.url.path != "/robots.txt"]
    assert all(right - left >= 2 for left, right in zip(times, times[1:]))


@pytest.mark.asyncio
async def test_timeout_exhaustion_is_bounded(make_rig: RigFactory) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/robots.txt":
            return reply(404)
        raise httpx.ReadTimeout("failure", request=request)

    rig = make_rig(handler, None)
    with pytest.raises(FetchError, match="retry budget"):
        await rig.fetcher.fetch(URL)
    assert len(rig.calls) == 4  # One robots request and exactly three content attempts.


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [429, 500, 502, 503, 504])
async def test_retryable_status_success(make_rig: RigFactory, status: int) -> None:
    attempts = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        if request.url.path == "/robots.txt":
            return reply(404)
        attempts += 1
        return reply(status if attempts == 1 else 200)

    rig = make_rig(handler, None)
    assert (await rig.fetcher.fetch(URL)).http_status == 200
    assert attempts == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [429, 503])
async def test_status_exhaustion_is_bounded(make_rig: RigFactory, status: int) -> None:
    rig = make_rig(
        lambda request: reply(404 if request.url.path == "/robots.txt" else status), None
    )
    with pytest.raises(FetchError, match="retry budget"):
        await rig.fetcher.fetch(URL)
    assert len(rig.calls) == 4


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [429, 503])
@pytest.mark.parametrize(
    ("header", "wait"),
    [
        ("7", 7),
        ("999", 10),
        ("Fri, 06 Nov 2026 00:00:08 GMT", 8),
        ("invalid", 2),
    ],
)
async def test_retry_after_header_and_cap(
    make_rig: RigFactory,
    status: int,
    header: str,
    wait: float,
) -> None:
    attempts = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        if request.url.path == "/robots.txt":
            return reply(404)
        attempts += 1
        return reply(status, b"", **{"Retry-After": header}) if attempts == 1 else reply()

    rig = make_rig(handler, {"crawl_retry_after_cap_s": 10})
    await rig.fetcher.fetch(URL)
    times = [time for request, time in rig.calls if request.url.path == "/notice"]
    assert times[1] - times[0] == wait


@pytest.mark.asyncio
async def test_non_retryable_status_fails_once(make_rig: RigFactory) -> None:
    rig = make_rig(lambda request: reply(404), None)
    with pytest.raises(FetchError, match="status 404"):
        await rig.fetcher.fetch(URL)
    assert len(rig.calls) == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [301, 302, 303, 307, 308])
async def test_redirect_success(make_rig: RigFactory, status: int) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/robots.txt":
            return reply(404)
        return (
            reply(status, location="/new") if request.url.path == "/notice" else reply(200, b"new")
        )

    rig = make_rig(handler, None)
    result = await rig.fetcher.fetch(URL)
    assert result.url == "https://regulator.example/new"
    assert result.content == b"new"
    assert [request.url.path for request, _ in rig.calls] == ["/robots.txt", "/notice", "/new"]


@pytest.mark.asyncio
async def test_redirect_loop_is_stopped(make_rig: RigFactory) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/robots.txt":
            return reply(404)
        return reply(302, location="/other" if request.url.path == "/notice" else "/notice")

    rig = make_rig(handler, None)
    with pytest.raises(FetchError, match="Redirect loop"):
        await rig.fetcher.fetch(URL)
    assert len(rig.calls) == 3


@pytest.mark.asyncio
async def test_too_many_redirects_are_stopped(make_rig: RigFactory) -> None:
    count = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal count
        if request.url.path == "/robots.txt":
            return reply(404)
        count += 1
        return reply(302, location=f"/hop-{count}")

    rig = make_rig(handler, {"crawl_max_redirects": 2})
    with pytest.raises(FetchError, match="Too many redirects"):
        await rig.fetcher.fetch(URL)
    assert count == 3


@pytest.mark.asyncio
async def test_redirect_to_disallowed_host_never_fetches_content(make_rig: RigFactory) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/robots.txt":
            return reply(
                200, b"User-agent: *\nDisallow: /" if request.url.host == "other.example" else b""
            )
        assert request.url.host == "regulator.example"
        return reply(302, location="https://other.example/secret")

    rig = make_rig(handler, None)
    with pytest.raises(RobotsDisallowed):
        await rig.fetcher.fetch(URL)
    assert [(request.url.host, request.url.path) for request, _ in rig.calls] == [
        ("regulator.example", "/robots.txt"),
        ("regulator.example", "/notice"),
        ("other.example", "/robots.txt"),
    ]


@pytest.mark.asyncio
async def test_same_host_redirect_is_rechecked_against_policy(make_rig: RigFactory) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/robots.txt":
            return reply(200, b"User-agent: *\nDisallow: /secret")
        return reply(302, location="/secret")

    rig = make_rig(handler, None)
    with pytest.raises(RobotsDisallowed):
        await rig.fetcher.fetch(URL)
    assert len(rig.calls) == 2


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "location", ["file:///secret", "https://user:password@other.example/a", ""]
)
async def test_invalid_redirect_is_rejected(make_rig: RigFactory, location: str) -> None:
    rig = make_rig(
        lambda request: reply(404)
        if request.url.path == "/robots.txt"
        else reply(302, location=location),
        None,
    )
    with pytest.raises(FetchError):
        await rig.fetcher.fetch(URL)
    assert len(rig.calls) == 2


@pytest.mark.asyncio
async def test_robots_redirect_bootstrap_applies_to_original_origin(make_rig: RigFactory) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.host == "regulator.example":
            assert request.url.path == "/robots.txt"
            return reply(302, location="https://policy.example/rules")
        return reply(200, b"User-agent: *\nDisallow: /")

    rig = make_rig(handler, None)
    with pytest.raises(RobotsDisallowed):
        await rig.fetcher.fetch(URL)
    assert len(rig.calls) == 2


@pytest.mark.asyncio
async def test_robots_redirect_loop_pauses_source(make_rig: RigFactory) -> None:
    rig = make_rig(lambda request: reply(302, location="/robots.txt"), None)
    with pytest.raises(SourcePaused):
        await rig.fetcher.fetch(URL)
    assert len(rig.calls) == 1


class ChunkedBody(httpx.AsyncByteStream):
    def __init__(self) -> None:
        self.reads = 0
        self.closed = False

    async def __aiter__(self) -> AsyncIterator[bytes]:
        for _ in range(100):
            self.reads += 1
            yield b"12345678"

    async def aclose(self) -> None:
        self.closed = True


@pytest.mark.asyncio
@pytest.mark.parametrize("declared", [None, "1", "800"])
async def test_size_limit_closes_stream_early(
    make_rig: RigFactory,
    declared: str | None,
) -> None:
    stream = ChunkedBody()

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/robots.txt":
            return reply(404)
        headers = {"Content-Length": declared} if declared is not None else {}
        return httpx.Response(200, stream=stream, headers=headers)

    rig = make_rig(handler, {"crawl_max_response_bytes": 10})
    with pytest.raises(ResponseTooLarge):
        await rig.fetcher.fetch(URL)
    assert stream.closed
    assert stream.reads == (0 if declared == "800" else 2)
    assert len(rig.calls) == 2


@pytest.mark.asyncio
async def test_exact_size_limit_succeeds(make_rig: RigFactory) -> None:
    rig = make_rig(
        lambda request: reply(404)
        if request.url.path == "/robots.txt"
        else reply(200, b"1234567890"),
        {"crawl_max_response_bytes": 10},
    )
    assert (await rig.fetcher.fetch(URL)).content == b"1234567890"


@pytest.mark.asyncio
async def test_unexpected_compression_is_rejected(make_rig: RigFactory) -> None:
    rig = make_rig(
        lambda request: reply(404)
        if request.url.path == "/robots.txt"
        else reply(200, b"not read", **{"Content-Encoding": "gzip"}),
        None,
    )
    with pytest.raises(FetchError, match="content encoding"):
        await rig.fetcher.fetch(URL)


@pytest.mark.asyncio
async def test_logs_and_exception_text_do_not_expose_secrets(
    make_rig: RigFactory,
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.DEBUG)

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/robots.txt":
            return reply(404)
        raise httpx.ReadTimeout("tester@example.invalid private-token", request=request)

    rig = make_rig(handler, None)
    destination = URL + "?key=private-token"
    with pytest.raises(FetchError) as captured:
        await rig.fetcher.fetch(destination)
    text = caplog.text + "".join(traceback.format_exception(captured.value))
    assert "tester@example.invalid" not in text
    assert "private-token" not in text
    assert "tester@example.invalid" not in repr(FetchSettings())


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (None, None),
        ("5", 5),
        ("9999", 60),
        ("-1", None),
        ("1.5", None),
        ("bad", None),
        ("Thu, 05 Nov 2026 00:00:00 GMT", 0),
    ],
)
def test_retry_after_parsing(value: str | None, expected: float | None) -> None:
    assert retry_after_seconds(value, datetime(2026, 11, 6, tzinfo=timezone.utc), 60) == expected


@pytest.mark.parametrize("value", ["", "name", "a@b\nX-Secret: value", "a@b;c", "a@b\x7f"])
def test_invalid_contact_fails_without_disclosing_input(value: str) -> None:
    with pytest.raises(ValidationError) as captured:
        FetchSettings(crawl_contact_email=SecretStr(value))
    assert "input_value" not in str(captured.value)


def test_contact_and_settings_are_loaded_from_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("CRAWL_CONTACT_EMAIL", "owner@example.invalid")
    monkeypatch.setenv("ROBOTS_CACHE_TTL_S", "120")
    settings = FetchSettings()
    assert settings.user_agent() == "RegWatch/0.1 (+mailto:owner@example.invalid)"
    assert settings.robots_cache_ttl_s == 120
    assert "owner@example.invalid" not in repr(settings)
