import httpx
import pytest

from app.core.config import FetchSettings
from app.crawl.fetcher import Fetcher
from conftest import FakeClock


@pytest.mark.asyncio
@pytest.mark.parametrize("retry_after", [None, "6"])
async def test_full_jitter_uses_capped_window_and_honors_retry_after(
    retry_after: str | None,
) -> None:
    clock = FakeClock()
    bounds: list[tuple[float, float]] = []
    attempts = 0

    def jitter(low: float, high: float) -> float:
        bounds.append((low, high))
        return high / 4

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        if request.url.path == "/robots.txt":
            return httpx.Response(404)
        attempts += 1
        headers = {"Retry-After": retry_after} if retry_after is not None and attempts == 1 else {}
        return httpx.Response(
            503 if attempts < 3 else 200, headers=headers, stream=httpx.ByteStream(b"ok")
        )

    settings = FetchSettings(
        crawl_delay_s=0.001, crawl_backoff_s=8, crawl_retry_after_cap_s=10, crawl_max_retries=2
    )
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        fetcher = Fetcher(client, settings, clock=clock.now, sleep=clock.sleep, jitter=jitter)
        assert (await fetcher.fetch("https://regulator.example/notice")).content == b"ok"
    assert bounds == [(0, 8), (0, 10)]
    assert clock.sleeps == pytest.approx([0.001, 6 if retry_after else 2, 2.5])
