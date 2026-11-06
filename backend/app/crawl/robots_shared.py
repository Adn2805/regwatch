"""Origin-scoped robots policies shared in Redis, including paused outcomes."""

import asyncio
import hashlib
import json
import math
from collections.abc import Awaitable
from importlib.resources import files
from typing import cast
from uuid import uuid4

from redis.asyncio import Redis
from redis.exceptions import RedisError

from app.core.config import RedisSettings
from app.core.redis import CoordinationUnavailable
from app.crawl.errors import FetchError, SourcePaused
from app.crawl.robots import RobotsLoader, RobotsPolicy, origin, parse_robots

LOCK_SCRIPT = files("app.crawl").joinpath("lua/robots_lock.lua").read_text(encoding="utf-8")
PUBLISH_SCRIPT = files("app.crawl").joinpath("lua/robots_publish.lua").read_text(encoding="utf-8")


class RedisRobotsCache:
    def __init__(self, client: Redis, settings: RedisSettings, *, ttl_s: float) -> None:
        if not math.isfinite(ttl_s) or ttl_s <= 0:
            raise ValueError("Robots TTL must be finite and positive")
        self._client, self._settings = client, settings
        self._ttl_ms = max(1, math.ceil(ttl_s * 1000))
        self._lease_ms = math.ceil(settings.robots_refresh_lease_s * 1000)

    def keys(self, url: str) -> tuple[str, str]:
        digest = hashlib.sha256(origin(url).encode()).hexdigest()
        base = f"{self._settings.redis_key_prefix}:{{{digest}}}:robots:v1"
        return base + ":cache", base + ":lock"

    async def policy(self, url: str, load: RobotsLoader) -> RobotsPolicy:
        cache_key, lock_key = self.keys(url)
        try:
            async with asyncio.timeout(self._settings.coordination_wait_s):
                while True:
                    raw = await self._client.get(cache_key)
                    if raw is not None:
                        return self._decode(raw)
                    owner = uuid4().hex
                    acquired = await self._client.set(lock_key, owner, nx=True, px=self._lease_ms)
                    if acquired:
                        # Recheck after taking the lock: another refresher may just have published.
                        raw = await self._client.get(cache_key)
                        if raw is not None:
                            await self._lock(lock_key, owner, "release")
                            return self._decode(raw)
                        return await self._refresh(url, load, cache_key, lock_key, owner)
                    await asyncio.sleep(min(0.05, self._settings.robots_refresh_lease_s / 3))
        except TimeoutError:
            raise SourcePaused(
                "Source paused: shared robots refresh wait deadline exceeded"
            ) from None
        except (RedisError, OSError, CoordinationUnavailable):
            raise SourcePaused("Source paused: shared robots coordination unavailable") from None

    def _decode(self, raw: str) -> RobotsPolicy:
        try:
            data = json.loads(raw)
            if (
                not isinstance(data, dict)
                or set(data) != {"schema", "status", "body"}
                or data["schema"] != 1
            ):
                raise ValueError("Invalid cache record")
            status, body = data["status"], data["body"]
            if not isinstance(body, str) or (
                status is not None and (type(status) is not int or not 100 <= status <= 599)
            ):
                raise ValueError("Invalid cache fields")
            if status in {404, 410}:
                return RobotsPolicy()
            if status == 200:
                return parse_robots(body.encode("utf-8"))
            if status is not None:
                raise SourcePaused(f"Source paused: robots.txt returned HTTP {status}")
        except (ValueError, TypeError, UnicodeError):
            raise SourcePaused(
                "Source paused: shared robots policy unavailable or unparseable"
            ) from None
        raise SourcePaused("Source paused: robots policy unavailable or unparseable")

    async def _payload(self, url: str, load: RobotsLoader) -> str:
        status: int | None = None
        body = ""
        try:
            status, content = await load(origin(url) + "/robots.txt")
            if status == 200:
                parse_robots(content)
                body = content.decode("utf-8-sig")
        except FetchError as error:
            status = error.status_code
        except (ValueError, UnicodeError):
            status = None
        # Never cache arbitrary exception messages, headers or contact values.
        return json.dumps({"schema": 1, "status": status, "body": body})

    async def _lock(self, key: str, owner: str, operation: str) -> bool:
        return bool(
            await cast(Awaitable[int], self._client.eval(LOCK_SCRIPT, 1, key, owner, operation))
        )

    async def _maintain(self, key: str, owner: str) -> None:
        while True:
            await asyncio.sleep(self._settings.robots_refresh_lease_s / 3)
            if not await self._lock(key, owner, str(self._lease_ms)):
                raise CoordinationUnavailable("Robots refresh ownership lost")

    async def _refresh(
        self, url: str, load: RobotsLoader, cache_key: str, lock_key: str, owner: str
    ) -> RobotsPolicy:
        loading = asyncio.create_task(self._payload(url, load))
        maintaining = asyncio.create_task(self._maintain(lock_key, owner))
        try:
            done, _ = await asyncio.wait(
                {loading, maintaining}, return_when=asyncio.FIRST_COMPLETED
            )
            if maintaining in done:
                await maintaining  # Propagate lost ownership or Redis failure; cancel HTTP below.
                raise CoordinationUnavailable("Robots refresh stopped unexpectedly")
            payload = await loading
            published = await cast(
                Awaitable[int],
                self._client.eval(
                    PUBLISH_SCRIPT, 2, lock_key, cache_key, owner, payload, str(self._ttl_ms)
                ),
            )
            if not published:
                raise CoordinationUnavailable("Robots refresh ownership lost before publication")
            return self._decode(payload)
        finally:
            loading.cancel()
            maintaining.cancel()
            await asyncio.gather(loading, maintaining, return_exceptions=True)
            release = asyncio.create_task(self._lock(lock_key, owner, "release"))
            try:
                await asyncio.shield(release)
            except asyncio.CancelledError:
                await release
                raise
