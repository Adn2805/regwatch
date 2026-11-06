"""Polite GET requests. Robots checks cannot be bypassed through the public fetch method."""

import asyncio
import logging
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from urllib.parse import urlsplit

import httpx

from app.core.config import FetchSettings
from app.crawl.adapters.base import AdapterParseError, canonicalize_url
from app.crawl.errors import FetchError, ResponseTooLarge, RobotsDisallowed
from app.crawl.robots import RobotsCache

REDIRECT_STATUSES = {301, 302, 303, 307, 308}


@dataclass(frozen=True)
class FetchResult:
    url: str = field(repr=False)
    http_status: int
    headers: dict[str, str] = field(repr=False)
    content: bytes = field(repr=False)


def retry_after_seconds(value: str | None, now: datetime, cap_s: float) -> float | None:
    if value is None:
        return None
    try:
        if value.strip().isdigit():
            seconds = float(int(value.strip()))
        else:
            date = parsedate_to_datetime(value)
            if date.tzinfo is None:
                return None
            seconds = (date - now).total_seconds()
        return max(0.0, min(seconds, cap_s))
    except (ValueError, TypeError, OverflowError):
        return None


def _url(value: str, base: str) -> str:
    try:
        return canonicalize_url(value, base_url=base)
    except (AdapterParseError, ValueError):
        raise FetchError("Invalid HTTP(S) request destination") from None


class Fetcher:
    def __init__(
        self,
        client: httpx.AsyncClient,
        settings: FetchSettings | None = None,
        *,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        now: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
    ) -> None:
        self._client = client  # Caller owns and closes the injected client.
        self._settings = settings if settings is not None else FetchSettings()
        self._clock, self._sleep, self._now = clock, sleep, now
        self._robots = RobotsCache(self._settings.robots_cache_ttl_s, clock)
        self._locks: dict[str, asyncio.Lock] = {}
        self._finished: dict[str, float] = {}
        self._delays: dict[str, float] = {}
        # Third-party INFO/debug wire logs can expose URLs and request headers.
        # Application errors below carry only fixed messages and status codes.
        for name in ("httpx", "httpcore", "protego"):
            logging.getLogger(name).setLevel(logging.WARNING)

    async def fetch(self, url: str) -> FetchResult:
        result = await self._follow_redirects(_url(url, url), check_robots=True)
        if not 200 <= result.http_status < 300:
            raise FetchError(f"HTTP request failed with status {result.http_status}")
        return result

    async def _load_robots(self, url: str) -> tuple[int, bytes]:
        # Robots bootstrap (including its redirects) cannot recursively require
        # another robots fetch. All ordinary document hops do require a check.
        result = await self._follow_redirects(url, check_robots=False)
        return result.http_status, result.content

    async def _follow_redirects(self, url: str, *, check_robots: bool) -> FetchResult:
        visited: set[str] = set()
        for hop in range(self._settings.crawl_max_redirects + 1):
            if url in visited:
                raise FetchError("Redirect loop detected")
            visited.add(url)
            result = await self._request(url, check_robots=check_robots)
            if result.http_status not in REDIRECT_STATUSES:
                return result
            if hop == self._settings.crawl_max_redirects:
                raise FetchError("Too many redirects")
            location = result.headers.get("location")
            if not location:
                raise FetchError("Redirect response has no destination")
            url = _url(location, url)
        raise AssertionError("Redirect loop must return or raise")

    async def _request(self, url: str, *, check_robots: bool) -> FetchResult:
        for attempt in range(self._settings.crawl_max_retries + 1):
            if check_robots:
                policy = await self._robots.policy(url, self._load_robots)
                host = urlsplit(url).hostname or ""
                self._delays[host] = max(
                    self._settings.crawl_delay_s, policy.crawl_delay_s, self._delays.get(host, 0.0)
                )
                if not policy.allows(url):
                    raise RobotsDisallowed("Robots policy disallows request")
            retry_after: str | None = None
            try:
                result = await self._once(url)
                if result.http_status != 429 and not 500 <= result.http_status <= 599:
                    return result
                if result.http_status in {429, 503}:
                    retry_after = result.headers.get("retry-after")
            except httpx.TransportError:
                pass  # Retry transport failures, without exposing request/exception data.
            if attempt == self._settings.crawl_max_retries:
                raise FetchError("HTTP retry budget exhausted") from None
            cap = self._settings.crawl_retry_after_cap_s
            backoff = min(cap, self._settings.crawl_backoff_s * 2**attempt)
            requested = retry_after_seconds(retry_after, self._now(), cap)
            await self._sleep(max(backoff, requested if requested is not None else 0.0))
        raise AssertionError("Retry loop must return or raise")

    async def _once(self, url: str) -> FetchResult:
        host = urlsplit(url).hostname or ""
        async with self._locks.setdefault(host, asyncio.Lock()):
            delay = self._delays.get(host, self._settings.crawl_delay_s)
            if host in self._finished:
                remaining = self._finished[host] + delay - self._clock()
                if remaining > 0:
                    await self._sleep(remaining)
            try:
                async with self._client.stream(
                    "GET",
                    url,
                    follow_redirects=False,
                    timeout=httpx.Timeout(self._settings.crawl_timeout_s),
                    headers={
                        "User-Agent": self._settings.user_agent(),
                        "Accept-Encoding": "identity",
                    },
                ) as response:
                    body = bytearray()
                    if 200 <= response.status_code < 300:
                        if (
                            response.headers.get("content-encoding", "identity").lower()
                            != "identity"
                        ):
                            raise FetchError(
                                "Unexpected response content encoding; expected identity"
                            )
                        limit = self._settings.crawl_max_response_bytes
                        declared = response.headers.get("content-length")
                        if declared is not None:
                            try:
                                length = int(declared)
                            except ValueError:
                                raise FetchError("Invalid response content length") from None
                            if length < 0:
                                raise FetchError("Invalid response content length")
                            if length > limit:
                                raise ResponseTooLarge("Response exceeds configured size limit")
                        async for chunk in response.aiter_raw():
                            if len(body) + len(chunk) > limit:
                                raise ResponseTooLarge("Response exceeds configured size limit")
                            body.extend(chunk)
                    return FetchResult(
                        str(response.url), response.status_code, dict(response.headers), bytes(body)
                    )
            finally:
                self._finished[host] = self._clock()
