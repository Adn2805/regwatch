"""Arq executor with DB fencing and heartbeat; repeated completed claims are no-ops."""

import asyncio
from collections.abc import Awaitable, Callable
from typing import Any, TypeVar, cast
from uuid import UUID

import httpx
from arq.worker import Worker
from sqlalchemy.orm import Session, sessionmaker

from app.core.config import (
    FetchSettings,
    LeaseSettings,
    RedisSettings,
    StorageSettings,
    WorkerSettings,
)
from app.core.db import make_engine
from app.core.redis import make_redis_client
from app.core.s3 import SnapshotStore, make_s3_client
from app.crawl.concurrency import RedisRequestGate
from app.crawl.fetcher import Fetcher
from app.crawl.robots_shared import RedisRobotsCache
from app.crawl.service import CrawlService, safe_error
from app.schedule.leases import LeaseClaim, LeaseLost, LeaseRepository
from app.schedule.scheduler import open_queue

T = TypeVar("T")


async def execute_with_heartbeat(
    sessions: sessionmaker[Session],
    claim: LeaseClaim,
    settings: LeaseSettings,
    operation: Callable[[], Awaitable[T]],
) -> T:
    leases = LeaseRepository(sessions, lease_s=settings.lease_duration_s)
    await asyncio.to_thread(leases.heartbeat, claim)

    async def maintain() -> None:
        while True:
            await asyncio.sleep(settings.heartbeat_interval_s)
            await asyncio.to_thread(leases.heartbeat, claim)

    async def invoke() -> T:
        return await operation()

    running, heartbeat = asyncio.create_task(invoke()), asyncio.create_task(maintain())
    try:
        done, _ = await asyncio.wait({running, heartbeat}, return_when=asyncio.FIRST_COMPLETED)
        if running in done:
            return await running
        try:
            await heartbeat
        except LeaseLost:
            # Completion can clear the lease immediately before the heartbeat UPDATE.
            # Only this claim's committed completion (not recovery) makes that benign.
            if await asyncio.to_thread(leases.completed_by_owner, claim):
                return await running
            raise
        raise LeaseLost("Heartbeat stopped unexpectedly")
    finally:
        running.cancel()
        heartbeat.cancel()
        await asyncio.gather(running, heartbeat, return_exceptions=True)


async def startup(ctx: dict[str, Any]) -> None:
    redis_settings = RedisSettings()
    storage = StorageSettings()
    ctx["engine"] = make_engine()
    ctx["sessions"] = sessionmaker(ctx["engine"])
    ctx["s3"] = make_s3_client(storage)
    ctx["store"] = SnapshotStore(ctx["s3"], storage.snapshot_bucket)
    ctx["coordination"] = make_redis_client(redis_settings)
    ctx["redis_settings"], ctx["fetch_settings"] = redis_settings, FetchSettings()
    ctx["lease_settings"] = LeaseSettings()


async def shutdown(ctx: dict[str, Any]) -> None:
    try:
        if "coordination" in ctx:
            await ctx["coordination"].aclose()
    finally:
        try:
            if "s3" in ctx:
                ctx["s3"].close()
        finally:
            if "engine" in ctx:
                ctx["engine"].dispose()


async def crawl_source(
    ctx: dict[str, Any], source_id: str, token: str, run_id: str
) -> dict[str, Any]:
    sessions = cast(sessionmaker[Session], ctx["sessions"])
    settings = cast(LeaseSettings, ctx["lease_settings"])
    claim = LeaseClaim(UUID(source_id), UUID(token), UUID(run_id))
    leases = LeaseRepository(sessions, lease_s=settings.lease_duration_s)
    if await asyncio.to_thread(leases.completed_by_owner, claim):
        return {"status": "already_completed"}

    async def operation() -> dict[str, Any]:
        fetch = cast(FetchSettings, ctx["fetch_settings"])
        if ctx.get("http_transport") is None:
            fetch.require_real_contact()
        redis_settings = cast(RedisSettings, ctx["redis_settings"])
        async with httpx.AsyncClient(transport=ctx.get("http_transport"), trust_env=False) as http:
            fetcher = Fetcher(
                http,
                fetch,
                coordination=RedisRequestGate(
                    ctx["coordination"],
                    redis_settings,
                    request_timeout_s=fetch.crawl_total_timeout_s,
                ),
                robots_cache=RedisRobotsCache(
                    ctx["coordination"], redis_settings, ttl_s=fetch.robots_cache_ttl_s
                ),
            )
            service = CrawlService(
                sessions, fetcher, ctx["store"], adapters=ctx.get("adapters"), claim=claim
            )
            summary = await service.run(
                claim.source_id, max_documents=WorkerSettings().worker_max_documents
            )
            return {
                "status": summary.status,
                "counts": summary.counts.values(),
                "run_id": str(summary.run_id),
            }

    try:
        return await execute_with_heartbeat(sessions, claim, settings, operation)
    except LeaseLost:
        return {"status": "stale_claim"}
    except Exception as error:
        raise RuntimeError("Worker crawl failed: " + safe_error(error)) from None


async def main() -> None:
    settings = WorkerSettings()
    redis = await open_queue(RedisSettings())
    worker = Worker(
        [crawl_source],
        redis_pool=redis,
        queue_name=settings.queue_name,
        on_startup=startup,
        on_shutdown=shutdown,
        max_jobs=settings.worker_max_jobs,
        queue_read_limit=max(100, settings.worker_max_jobs * 5),
        job_timeout=settings.worker_job_timeout_s,
        max_tries=3,
        keep_result=60,
        log_results=False,
    )
    try:
        await worker.async_run()
    finally:
        await worker.close()
        await redis.aclose()


if __name__ == "__main__":
    asyncio.run(main())
