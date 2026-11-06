import asyncio
import os
from collections.abc import AsyncIterator
from uuid import uuid4

import pytest
import pytest_asyncio
from redis.asyncio import Redis

from app.core.config import RedisSettings
from app.core.redis import CoordinationUnavailable, make_redis_client
from app.crawl.ratelimit import RedisTokenBucket, host_key

pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.skipif("POSTGRES_DB" not in os.environ, reason="run in Compose for real Redis"),
]


@pytest_asyncio.fixture
async def coordination() -> AsyncIterator[tuple[Redis, RedisSettings]]:
    settings = RedisSettings(redis_key_prefix="test-" + uuid4().hex, rate_interval_s=0.05)
    client = make_redis_client(settings)
    assert await client.ping()
    try:
        yield client, settings
    finally:
        keys = [key async for key in client.scan_iter(match=settings.redis_key_prefix + ":*")]
        if keys:
            await client.delete(*keys)
        await client.aclose()


async def test_atomic_admission_across_clients(coordination: tuple[Redis, RedisSettings]) -> None:
    client, settings = coordination
    # Long refill period guarantees this wave cannot refill even on slow CI runners.
    settings = settings.model_copy(update={"rate_interval_s": 60})
    other = make_redis_client(settings)
    try:
        buckets = [RedisTokenBucket(client, settings), RedisTokenBucket(other, settings)]
        results = await asyncio.gather(*(buckets[i % 2].admit("example.test") for i in range(80)))
        assert sum(r.allowed for r in results) == 1
        assert all(0 < r.retry_after_s <= 60 for r in results if not r.allowed)
        assert (await buckets[0].admit("other.test")).allowed
    finally:
        await other.aclose()


async def test_refill_and_safe_idle_expiry(coordination: tuple[Redis, RedisSettings]) -> None:
    client, settings = coordination
    bucket = RedisTokenBucket(client, settings)
    assert (await bucket.admit("example.test")).allowed
    denied = await bucket.admit("example.test")
    assert not denied.allowed
    ttl = await client.pttl(host_key(settings.redis_key_prefix, "example.test") + ":bucket")
    assert ttl >= 900
    await asyncio.sleep(denied.retry_after_s + 0.01)
    assert (await bucket.admit("example.test")).allowed
    await asyncio.sleep(1.1)
    assert await client.exists(host_key(settings.redis_key_prefix, "example.test") + ":bucket") == 0
    assert (await bucket.admit("example.test")).allowed


async def test_stricter_delay_and_burst_capacity(coordination: tuple[Redis, RedisSettings]) -> None:
    client, settings = coordination
    bucket = RedisTokenBucket(client, settings.model_copy(update={"rate_capacity": 2}))
    assert (await bucket.admit("burst.test")).allowed
    assert (await bucket.admit("burst.test")).allowed
    assert not (await bucket.admit("burst.test")).allowed
    assert (await bucket.admit("example.test", minimum_interval_s=60)).allowed
    denied = await bucket.admit("example.test")
    assert not denied.allowed and denied.retry_after_s > 50


async def test_redis_unavailable_fails_closed() -> None:
    client = make_redis_client(
        RedisSettings(redis_host="127.0.0.1", redis_port=1, redis_timeout_s=0.1)
    )
    try:
        with pytest.raises(CoordinationUnavailable, match="no request permitted"):
            await RedisTokenBucket(client, RedisSettings()).admit("example.test")
    finally:
        await client.aclose()
