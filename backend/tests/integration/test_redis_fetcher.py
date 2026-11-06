import asyncio
import os
import time
from collections.abc import AsyncIterator
from uuid import uuid4

import httpx
import pytest
import pytest_asyncio
from redis.asyncio import Redis

from app.core.config import FetchSettings, RedisSettings
from app.core.redis import CoordinationUnavailable, make_redis_client
from app.crawl.concurrency import RedisConcurrencySlots, RedisRequestGate
from app.crawl.errors import FetchError, SourcePaused
from app.crawl.fetcher import Fetcher

pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.skipif("POSTGRES_DB" not in os.environ, reason="run in Compose for real Redis"),
]


@pytest_asyncio.fixture
async def shared() -> AsyncIterator[tuple[Redis, RedisSettings]]:
    settings = RedisSettings(
        redis_key_prefix="test-" + uuid4().hex, rate_interval_s=0.05, coordination_wait_s=3
    )
    client = make_redis_client(settings)
    try:
        yield client, settings
    finally:
        keys = [key async for key in client.scan_iter(match=settings.redis_key_prefix + ":*")]
        if keys:
            await client.delete(*keys)
        await client.aclose()


def fetch_settings(**values: float | int) -> FetchSettings:
    return FetchSettings.model_validate(
        {"crawl_delay_s": 0.001, "crawl_max_retries": 0, "crawl_total_timeout_s": 1, **values}
    )


async def test_multiple_fetchers_share_rate_for_robots_and_content(
    shared: tuple[Redis, RedisSettings],
) -> None:
    redis, settings = shared
    starts: list[float] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        starts.append(time.monotonic())
        await asyncio.sleep(0.01)
        return httpx.Response(
            404 if request.url.path == "/robots.txt" else 200, stream=httpx.ByteStream(b"ok")
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        fetchers = [
            Fetcher(
                client,
                fetch_settings(),
                coordination=RedisRequestGate(redis, settings, request_timeout_s=1),
            )
            for _ in range(4)
        ]
        results = await asyncio.gather(
            *(fetcher.fetch("https://one.example/notice") for fetcher in fetchers)
        )
    assert all(result.content == b"ok" for result in results)
    assert len(starts) == 8  # Process-local robots cache until step 3.
    assert all(b - a >= 0.045 for a, b in zip(starts, starts[1:]))


async def test_concurrency_cap_holds_until_body_is_closed(
    shared: tuple[Redis, RedisSettings],
) -> None:
    redis, settings = shared
    settings = settings.model_copy(
        update={"rate_capacity": 100, "rate_interval_s": 0.001, "domain_concurrency": 2}
    )
    active = 0
    maximum = 0

    class Body(httpx.AsyncByteStream):
        async def __aiter__(self) -> AsyncIterator[bytes]:
            await asyncio.sleep(0.03)
            yield b"ok"

        async def aclose(self) -> None:
            nonlocal active
            active -= 1

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal active, maximum
        if request.url.path == "/robots.txt":
            return httpx.Response(404)
        active += 1
        maximum = max(maximum, active)
        return httpx.Response(200, stream=Body())

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        fetchers = [
            Fetcher(
                client,
                fetch_settings(),
                coordination=RedisRequestGate(redis, settings, request_timeout_s=1),
            )
            for _ in range(6)
        ]
        await asyncio.gather(*(fetcher.fetch("https://one.example/notice") for fetcher in fetchers))
    assert maximum == 2 and active == 0
    assert await redis.zcard(RedisConcurrencySlots(redis, settings).key("one.example")) == 0


async def test_permits_are_owned_and_abandoned_ones_expire(
    shared: tuple[Redis, RedisSettings],
) -> None:
    redis, settings = shared
    slots = RedisConcurrencySlots(redis, settings)
    assert (await slots.admit("one.example", "old-owner", lease_s=0.05)).allowed
    await slots.release("one.example", "wrong-owner")
    assert not (await slots.admit("one.example", "new-owner", lease_s=1)).allowed
    await asyncio.sleep(0.07)
    assert (await slots.admit("one.example", "new-owner", lease_s=1)).allowed
    await slots.release("one.example", "old-owner")
    assert await redis.zcard(slots.key("one.example")) == 1
    await slots.release("one.example", "new-owner")


async def test_cancelled_http_attempt_releases_permit(shared: tuple[Redis, RedisSettings]) -> None:
    redis, settings = shared
    entered = asyncio.Event()

    async def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/robots.txt":
            return httpx.Response(404)
        entered.set()
        await asyncio.Event().wait()
        raise AssertionError("unreachable")

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        fetcher = Fetcher(
            client,
            fetch_settings(),
            coordination=RedisRequestGate(redis, settings, request_timeout_s=1),
        )
        task = asyncio.create_task(fetcher.fetch("https://one.example/notice"))
        await asyncio.wait_for(entered.wait(), 2)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    assert await redis.zcard(RedisConcurrencySlots(redis, settings).key("one.example")) == 0


async def test_total_request_deadline_releases_permit(shared: tuple[Redis, RedisSettings]) -> None:
    redis, settings = shared

    async def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/robots.txt":
            return httpx.Response(404)
        await asyncio.Event().wait()
        raise AssertionError("unreachable")

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        fetcher = Fetcher(
            client,
            fetch_settings(crawl_total_timeout_s=0.05),
            coordination=RedisRequestGate(redis, settings, request_timeout_s=0.05),
        )
        with pytest.raises(FetchError, match="retry budget"):
            await fetcher.fetch("https://one.example/notice")
    assert await redis.zcard(RedisConcurrencySlots(redis, settings).key("one.example")) == 0


async def test_retry_redirect_and_crawl_delay_all_use_gate(
    shared: tuple[Redis, RedisSettings],
) -> None:
    redis, settings = shared
    paths: list[str] = []
    starts: list[float] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        paths.append(request.url.path)
        starts.append(time.monotonic())
        if request.url.path == "/robots.txt":
            return httpx.Response(
                200, stream=httpx.ByteStream(b"User-agent: *\nCrawl-delay: 0.08\n")
            )
        if paths.count("/notice") == 1:
            return httpx.Response(503)
        if request.url.path == "/notice":
            return httpx.Response(302, headers={"location": "/final"})
        return httpx.Response(200, stream=httpx.ByteStream(b"ok"))

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        fetcher = Fetcher(
            client,
            fetch_settings(crawl_max_retries=1, crawl_backoff_s=0.001),
            coordination=RedisRequestGate(redis, settings, request_timeout_s=1),
        )
        assert (await fetcher.fetch("https://one.example/notice")).content == b"ok"
    assert paths == ["/robots.txt", "/notice", "/notice", "/final"]
    assert all(b - a >= 0.075 for a, b in zip(starts, starts[1:]))


async def test_redis_failure_prevents_any_http() -> None:
    settings = RedisSettings(redis_host="127.0.0.1", redis_port=1, redis_timeout_s=0.05)
    redis = make_redis_client(settings)
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(200)

    try:
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            fetcher = Fetcher(
                client,
                fetch_settings(),
                coordination=RedisRequestGate(redis, settings, request_timeout_s=1),
            )
            with pytest.raises(SourcePaused):
                await fetcher.fetch("https://one.example/notice")
        assert calls == 0
    finally:
        await redis.aclose()


async def test_coordination_wait_is_bounded(shared: tuple[Redis, RedisSettings]) -> None:
    redis, settings = shared
    settings = settings.model_copy(update={"coordination_wait_s": 0.05})
    slots = RedisConcurrencySlots(redis, settings)
    await slots.admit("one.example", "blocked-owner", lease_s=2)
    gate = RedisRequestGate(redis, settings, request_timeout_s=1)
    with pytest.raises(CoordinationUnavailable, match="wait deadline"):
        async with gate.slot("one.example", minimum_interval_s=0):
            raise AssertionError("no request permitted")
