"""Robots syntax validation, matching, and a process-local origin-scoped TTL cache."""

import asyncio
import math
import re
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Protocol, cast
from urllib.parse import urlsplit

from protego import Protego  # type: ignore[import-untyped]

from app.crawl.errors import FetchError, SourcePaused


class RobotRules(Protocol):
    def can_fetch(self, url: str, user_agent: str) -> bool: ...
    def crawl_delay(self, user_agent: str) -> float | None: ...


@dataclass(frozen=True)
class RobotsPolicy:
    rules: RobotRules | None = None
    crawl_delay_s: float = 0.0

    def allows(self, url: str) -> bool:
        return self.rules is None or self.rules.can_fetch(url, "RegWatch")


def parse_robots(content: bytes) -> RobotsPolicy:
    # Matching libraries deliberately tolerate bad lines. Validate first because
    # RegWatch must pause on an unparseable file, including HTML error pages.
    text = content.decode("utf-8-sig")
    has_agent = False
    for raw in text.splitlines():
        line = raw.split("#", 1)[0].strip()
        if not line:
            continue
        key, separator, value = line.partition(":")
        key, value = key.strip().lower(), value.strip()
        if not separator or not re.fullmatch(r"[a-z][a-z0-9_-]*", key):
            raise ValueError("Invalid robots directive")
        if any(ord(character) < 32 and character != "\t" for character in line):
            raise ValueError("Invalid robots characters")
        if key == "user-agent":
            if not re.fullmatch(r"[a-zA-Z0-9_.*-]+", value):
                raise ValueError("Invalid robots user agent")
            has_agent = True
        elif key in {"allow", "disallow", "crawl-delay"}:
            if not has_agent:
                raise ValueError("Robots rule has no user agent")
            if key == "crawl-delay":
                delay = float(value)
                if not math.isfinite(delay) or delay < 0:
                    raise ValueError("Invalid crawl delay")
            elif value and not value.startswith(("/", "*")):
                raise ValueError("Invalid robots path")
        # Unknown, well-formed extension records are allowed by the protocol.
    rules = cast(RobotRules, Protego.parse(text))
    crawl_delay = rules.crawl_delay("RegWatch")
    return RobotsPolicy(rules=rules, crawl_delay_s=crawl_delay if crawl_delay is not None else 0.0)


def origin(url: str) -> str:
    parts = urlsplit(url)
    return f"{parts.scheme}://{parts.netloc}"


@dataclass(frozen=True)
class CacheEntry:
    policy: RobotsPolicy | None  # None caches an unavailable policy, never permission.
    expires_at: float
    reason: str | None = None


RobotsLoader = Callable[[str], Awaitable[tuple[int, bytes]]]


class PolicyCache(Protocol):
    async def policy(self, url: str, load: RobotsLoader) -> RobotsPolicy: ...


class RobotsCache:
    def __init__(self, ttl_s: float, clock: Callable[[], float]) -> None:
        self._ttl_s = ttl_s
        self._clock = clock
        self._entries: dict[str, CacheEntry] = {}
        self._locks: dict[str, asyncio.Lock] = {}

    async def policy(self, url: str, load: RobotsLoader) -> RobotsPolicy:
        key = origin(url)
        async with self._locks.setdefault(key, asyncio.Lock()):
            entry = self._entries.get(key)
            if entry is None or entry.expires_at <= self._clock():
                reason = None
                try:
                    status, body = await load(f"{key}/robots.txt")
                    if status in {404, 410}:
                        policy: RobotsPolicy | None = RobotsPolicy()
                    elif status == 200:
                        policy = parse_robots(body)
                    else:
                        policy = None
                        reason = f"Source paused: robots.txt returned HTTP {status}"
                except (FetchError, ValueError, UnicodeError) as error:
                    policy = None
                    if isinstance(error, FetchError) and error.status_code is not None:
                        reason = f"Source paused: robots.txt returned HTTP {error.status_code}"
                    else:
                        reason = "Source paused: robots policy unavailable or unparseable"
                entry = CacheEntry(policy, self._clock() + self._ttl_s, reason)
                self._entries[key] = entry
            if entry.policy is None:
                raise SourcePaused(
                    entry.reason or "Source paused: robots policy unavailable or unparseable"
                )
            return entry.policy
