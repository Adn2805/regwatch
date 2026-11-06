import asyncio
import json
import os
import time
from collections.abc import AsyncIterator
from uuid import uuid4

import httpx
import pytest
import pytest_asyncio
from redis.asyncio import Redis

from app.core.config import FetchSettings, RedisSettings
from app.core.redis import make_redis_client
from app.crawl.concurrency import RedisRequestGate
from app.crawl.errors import FetchError, SourcePaused
from app.crawl.fetcher import Fetcher
from app.crawl.robots_shared import RedisRobotsCache

pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.skipif("POSTGRES_DB" not in os.environ, reason="run in Compose for real Redis"),
]
URL = "https://regulator.example/notice"


@pytest_asyncio.fixture
async def shared() -> AsyncIterator[tuple[Redis, RedisSettings]]:
    settings = RedisSettings(
        redis_key_prefix="test-" + uuid4().hex,
        rate_interval_s=0.01,
        coordination_wait_s=2,
        robots_refresh_lease_s=0.09,
    )
    client = make_redis_client(settings)
    try:
        yield client, settings
    finally:
        keys = [key async for key in client.scan_iter(match=settings.redis_key_prefix + ":*")]
        if keys:
            await client.delete(*keys)
        await client.aclose()


@pytest.mark.parametrize("capacity", [1, 10])
async def test_two_fetchers_share_one_robots_load_and_delay(
    shared: tuple[Redis, RedisSettings],
    capacity: int,
) -> None:
    redis, settings = shared
    settings = settings.model_copy(update={"rate_capacity": capacity})
    calls: list[str] = []
    starts: list[float] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request.url.path)
        starts.append(time.monotonic())
        body = (
            b"User-agent: *\nDisallow: /blocked\nCrawl-delay: 0.02\n"
            if request.url.path == "/robots.txt"
            else b"ok"
        )
        return httpx.Response(200, stream=httpx.ByteStream(body))

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        fetchers = [
            Fetcher(
                client,
                FetchSettings(crawl_delay_s=0.001, crawl_max_retries=0),
                coordination=RedisRequestGate(redis, settings, request_timeout_s=1),
                robots_cache=RedisRobotsCache(redis, settings, ttl_s=1),
            )
            for _ in range(2)
        ]
        results = await asyncio.gather(*(f.fetch(URL) for f in fetchers))
        assert all(r.content == b"ok" for r in results)
        policy = await fetchers[1].robots_policy(URL)
        assert policy.crawl_delay_s == 0.02 and not policy.allows(
            "https://regulator.example/blocked"
        )
    assert calls.count("/robots.txt") == 1 and calls.count("/notice") == 2
    assert all(b - a >= 0.015 for a, b in zip(starts, starts[1:]))


async def test_refresh_lease_is_renewed_for_slow_load(shared: tuple[Redis, RedisSettings]) -> None:
    redis, settings = shared
    calls = 0

    async def load(url: str) -> tuple[int, bytes]:
        nonlocal calls
        calls += 1
        await asyncio.sleep(0.3)
        return 200, b"User-agent: *\nDisallow:\n"

    caches = [RedisRobotsCache(redis, settings, ttl_s=1) for _ in range(6)]
    policies = await asyncio.gather(*(cache.policy(URL, load) for cache in caches))
    assert all(policy.allows(URL) for policy in policies) and calls == 1


@pytest.mark.parametrize("status", [200, 404, 410, 401, 403, 418, 429, 503])
async def test_positive_and_paused_records_are_shared_until_expiry(
    shared: tuple[Redis, RedisSettings], status: int
) -> None:
    redis, settings = shared
    calls = 0

    async def load(url: str) -> tuple[int, bytes]:
        nonlocal calls
        calls += 1
        return status, b"User-agent: *\nDisallow:\n"

    for _ in range(2):
        cache = RedisRobotsCache(redis, settings, ttl_s=0.08)
        if status in {200, 404, 410}:
            assert (await cache.policy(URL, load)).allows(URL)
        else:
            with pytest.raises(SourcePaused, match=f"HTTP {status}"):
                await cache.policy(URL, load)
    assert calls == 1
    await asyncio.sleep(0.1)
    cache = RedisRobotsCache(redis, settings, ttl_s=1)
    if status in {200, 404, 410}:
        await cache.policy(URL, load)
    else:
        with pytest.raises(SourcePaused):
            await cache.policy(URL, load)
    assert calls == 2


@pytest.mark.parametrize("failure", ["malformed", "transport"])
async def test_unavailable_policy_is_cached_without_exception_details(
    shared: tuple[Redis, RedisSettings], failure: str
) -> None:
    redis, settings = shared
    calls = 0

    async def load(url: str) -> tuple[int, bytes]:
        nonlocal calls
        calls += 1
        if failure == "transport":
            raise FetchError("details-must-not-be-cached")
        return 200, b"<html>challenge</html>"

    cache = RedisRobotsCache(redis, settings, ttl_s=1)
    for _ in range(2):
        with pytest.raises(SourcePaused, match="unavailable or unparseable"):
            await cache.policy(URL, load)
    assert calls == 1
    raw = await redis.get(cache.keys(URL)[0])
    assert raw is not None and "details-must-not-be-cached" not in raw


async def test_corrupt_cache_fails_closed_and_origins_are_isolated(
    shared: tuple[Redis, RedisSettings],
) -> None:
    redis, settings = shared
    cache = RedisRobotsCache(redis, settings, ttl_s=1)
    assert cache.keys(URL) != cache.keys("http://regulator.example/notice")
    assert cache.keys(URL) != cache.keys("https://regulator.example:8443/notice")
    await redis.set(cache.keys(URL)[0], "not-json")

    async def forbidden_load(url: str) -> tuple[int, bytes]:
        raise AssertionError("corrupt cached policy must not trigger HTTP")

    with pytest.raises(SourcePaused, match="unavailable or unparseable"):
        await cache.policy(URL, forbidden_load)


async def test_lost_owner_cannot_publish_or_delete_successor_lock(
    shared: tuple[Redis, RedisSettings],
) -> None:
    redis, settings = shared
    cache = RedisRobotsCache(redis, settings, ttl_s=1)
    entered = asyncio.Event()
    finish = asyncio.Event()

    async def load(url: str) -> tuple[int, bytes]:
        entered.set()
        await finish.wait()
        return 404, b""

    task = asyncio.create_task(cache.policy(URL, load))
    await entered.wait()
    key, lock = cache.keys(URL)
    await redis.set(lock, "successor", px=1000)
    finish.set()
    with pytest.raises(SourcePaused, match="coordination unavailable"):
        await task
    assert await redis.get(key) is None
    assert await redis.get(lock) == "successor"


async def test_cancelled_refresh_releases_lock(shared: tuple[Redis, RedisSettings]) -> None:
    redis, settings = shared
    cache = RedisRobotsCache(redis, settings, ttl_s=1)
    entered = asyncio.Event()

    async def load(url: str) -> tuple[int, bytes]:
        entered.set()
        await asyncio.Event().wait()
        raise AssertionError("unreachable")

    task = asyncio.create_task(cache.policy(URL, load))
    await entered.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert await redis.exists(cache.keys(URL)[1]) == 0


async def test_redis_outage_and_stuck_lock_pause_without_loading(
    shared: tuple[Redis, RedisSettings],
) -> None:
    redis, settings = shared
    settings = settings.model_copy(update={"coordination_wait_s": 0.05})
    cache = RedisRobotsCache(redis, settings, ttl_s=1)
    await redis.set(cache.keys(URL)[1], "abandoned", px=200)

    async def forbidden_load(url: str) -> tuple[int, bytes]:
        raise AssertionError("no HTTP permitted")

    with pytest.raises(SourcePaused, match="wait deadline"):
        await cache.policy(URL, forbidden_load)
    bad = make_redis_client(
        RedisSettings(redis_host="127.0.0.1", redis_port=1, redis_timeout_s=0.05)
    )
    try:
        with pytest.raises(SourcePaused, match="coordination unavailable"):
            await RedisRobotsCache(bad, settings, ttl_s=1).policy(URL, forbidden_load)
    finally:
        await bad.aclose()


async def test_expired_refresh_lock_can_be_recovered(shared: tuple[Redis, RedisSettings]) -> None:
    redis, settings = shared
    cache = RedisRobotsCache(redis, settings, ttl_s=1)
    await redis.set(cache.keys(URL)[1], "abandoned", px=30)

    async def load(url: str) -> tuple[int, bytes]:
        return 404, b""

    assert (await cache.policy(URL, load)).allows(URL)
    assert json.loads(await redis.get(cache.keys(URL)[0]))["status"] == 404
