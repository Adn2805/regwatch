"""Shared request permits, held only while one bounded HTTP attempt is in flight."""

import asyncio
import math
from collections.abc import AsyncIterator, Awaitable
from contextlib import asynccontextmanager
from importlib.resources import files
from typing import cast
from uuid import uuid4

from redis.asyncio import Redis
from redis.exceptions import RedisError

from app.core.config import RedisSettings
from app.core.redis import CoordinationUnavailable
from app.crawl.ratelimit import Admission, RedisTokenBucket, host_key

SCRIPT = files("app.crawl").joinpath("lua/concurrency.lua").read_text(encoding="utf-8")


class RedisConcurrencySlots:
    def __init__(self, client: Redis, settings: RedisSettings) -> None:
        self._client, self._settings = client, settings

    def key(self, host: str) -> str:
        return host_key(self._settings.redis_key_prefix, host) + ":requests"

    async def admit(self, host: str, owner: str, *, lease_s: float) -> Admission:
        if not math.isfinite(lease_s) or lease_s <= 0:
            raise ValueError("Request lease must be finite and positive")
        try:
            result = await cast(
                Awaitable[list[int]],
                self._client.eval(
                    SCRIPT,
                    1,
                    self.key(host),
                    str(self._settings.domain_concurrency),
                    str(math.ceil(lease_s * 1000)),
                    owner,
                ),
            )
            allowed, wait_ms, now_ms = result
            return Admission(bool(allowed), wait_ms / 1000, now_ms)
        except (RedisError, OSError, TypeError, ValueError):
            raise CoordinationUnavailable(
                "Redis concurrency admission unavailable; no request permitted"
            ) from None

    async def release(self, host: str, owner: str) -> None:
        try:
            # Removing this unique member cannot release another worker's permit.
            await self._client.zrem(self.key(host), owner)
        except (RedisError, OSError):
            raise CoordinationUnavailable("Redis request-permit release unavailable") from None


class RedisRequestGate:
    def __init__(self, client: Redis, settings: RedisSettings, *, request_timeout_s: float) -> None:
        if not math.isfinite(request_timeout_s) or request_timeout_s <= 0:
            raise ValueError("Request timeout must be finite and positive")
        self.request_timeout_s = request_timeout_s
        self._settings = settings
        self._slots = RedisConcurrencySlots(client, settings)
        self._bucket = RedisTokenBucket(client, settings)
        # Cover admission calls and cleanup as well as the entire HTTP attempt.
        self._lease_s = request_timeout_s + 2 * settings.redis_timeout_s + 5

    @asynccontextmanager
    async def slot(self, host: str, *, minimum_interval_s: float) -> AsyncIterator[None]:
        owner: str | None = None
        try:
            try:
                async with asyncio.timeout(self._settings.coordination_wait_s):
                    while True:
                        owner = uuid4().hex
                        permit = await self._slots.admit(host, owner, lease_s=self._lease_s)
                        if permit.allowed:
                            admission = await self._bucket.admit(
                                host, minimum_interval_s=minimum_interval_s
                            )
                            if admission.allowed:
                                break
                            await self._slots.release(host, owner)
                            owner = None
                            await asyncio.sleep(max(0.001, admission.retry_after_s))
                        else:
                            owner = None
                            # Poll because a healthy request can release before lease expiry.
                            await asyncio.sleep(min(0.1, max(0.001, permit.retry_after_s)))
            except TimeoutError:
                raise CoordinationUnavailable(
                    "Redis request-admission wait deadline exceeded"
                ) from None
            yield
        finally:
            if owner is not None:
                # Finish release even when the caller cancels during the HTTP body.
                task = asyncio.create_task(self._slots.release(host, owner))
                try:
                    await asyncio.shield(task)
                except asyncio.CancelledError:
                    await task
                    raise
