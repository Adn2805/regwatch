"""Atomic distributed token buckets. Denials return a delay; Redis never sleeps."""

import hashlib
import ipaddress
import math
import re
from dataclasses import dataclass
from collections.abc import Awaitable
from typing import cast
from importlib.resources import files

from redis.asyncio import Redis
from redis.exceptions import RedisError

from app.core.config import RedisSettings
from app.core.redis import CoordinationUnavailable

SCRIPT = files("app.crawl").joinpath("lua/token_bucket.lua").read_text(encoding="utf-8")


def normalized_host(host: str) -> str:
    host = host.rstrip(".").lower()
    try:
        return ipaddress.ip_address(host).compressed
    except ValueError:
        pass
    host = host.encode("idna").decode("ascii")
    if not host or len(host) > 253 or not re.fullmatch(r"[a-z0-9.-]+", host):
        raise ValueError("Invalid limiter hostname")
    if any(
        not label or len(label) > 63 or label.startswith("-") or label.endswith("-")
        for label in host.split(".")
    ):
        raise ValueError("Invalid limiter hostname")
    return host


def host_key(prefix: str, host: str) -> str:
    digest = hashlib.sha256(normalized_host(host).encode()).hexdigest()
    return f"{prefix}:{{{digest}}}"


@dataclass(frozen=True)
class Admission:
    allowed: bool
    retry_after_s: float
    redis_time_ms: int


class RedisTokenBucket:
    def __init__(self, client: Redis, settings: RedisSettings) -> None:
        self._client, self._settings = client, settings

    async def admit(self, host: str, *, minimum_interval_s: float = 0) -> Admission:
        if not math.isfinite(minimum_interval_s) or minimum_interval_s < 0:
            raise ValueError("Minimum interval must be finite and nonnegative")
        key = host_key(self._settings.redis_key_prefix, host) + ":bucket"
        interval = max(self._settings.rate_interval_s, minimum_interval_s)
        try:
            # redis-py's shared command annotation includes its synchronous client;
            # this owned client is always redis.asyncio.Redis.
            result = await cast(
                Awaitable[list[int]],
                self._client.eval(
                    SCRIPT, 1, key, str(interval * 1000), str(self._settings.rate_capacity)
                ),
            )
            allowed, wait_ms, now_ms = result
            return Admission(bool(int(allowed)), float(wait_ms) / 1000, int(now_ms))
        except (RedisError, OSError, TypeError, ValueError):
            raise CoordinationUnavailable(
                "Redis token-bucket admission unavailable; no request permitted"
            ) from None
