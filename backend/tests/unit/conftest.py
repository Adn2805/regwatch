import asyncio
from collections.abc import AsyncIterator, Callable
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import httpx
import pytest_asyncio

from app.core.config import FetchSettings
from app.crawl.fetcher import Fetcher

Handler = Callable[[httpx.Request], httpx.Response]


class FakeClock:
    def __init__(self) -> None:
        self.time = 0.0
        self.sleeps: list[float] = []

    def now(self) -> float:
        return self.time

    async def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.time += seconds
        await asyncio.sleep(0)


@dataclass
class Rig:
    fetcher: Fetcher
    clock: FakeClock
    calls: list[tuple[httpx.Request, float]]


RigFactory = Callable[[Handler, dict[str, Any] | None], Rig]


@pytest_asyncio.fixture
async def make_rig() -> AsyncIterator[RigFactory]:
    clients: list[httpx.AsyncClient] = []

    def make(handler: Handler, overrides: dict[str, Any] | None = None) -> Rig:
        clock = FakeClock()
        calls: list[tuple[httpx.Request, float]] = []

        def record(request: httpx.Request) -> httpx.Response:
            calls.append((request, clock.time))
            return handler(request)

        client = httpx.AsyncClient(transport=httpx.MockTransport(record), trust_env=False)
        clients.append(client)
        values: dict[str, Any] = {
            "crawl_contact_email": "tester@example.invalid",
            "crawl_timeout_s": 3,
            "crawl_delay_s": 2,
            "crawl_max_retries": 2,
            "crawl_backoff_s": 1,
            "crawl_retry_after_cap_s": 60,
            "crawl_max_redirects": 5,
            "crawl_max_response_bytes": 1024,
            "robots_cache_ttl_s": 3600,
        }
        values.update(overrides or {})
        fetcher = Fetcher(
            client,
            FetchSettings(**values),
            jitter=lambda low, high: high,  # Existing cases exercise the upper backoff bound.
            clock=clock.now,
            sleep=clock.sleep,
            now=lambda: datetime(2026, 11, 6, tzinfo=timezone.utc),
        )
        return Rig(fetcher, clock, calls)

    yield make
    for client in clients:
        await client.aclose()
