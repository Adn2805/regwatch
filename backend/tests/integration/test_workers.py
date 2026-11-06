"""Arq subprocesses use real Postgres, Redis and MinIO; HTTP is always mocked."""

import asyncio
import logging
import multiprocessing
import os
import threading
from collections.abc import AsyncIterator
from typing import Any
from uuid import uuid4

import httpx
import pytest
import pytest_asyncio
from arq import create_pool
from arq.connections import ArqRedis
from arq.worker import Worker
from sqlalchemy import func, select
from sqlalchemy.orm import sessionmaker

from app.core.config import FetchSettings, LeaseSettings, RedisSettings
from app.core.s3 import Snapshot, SnapshotStore
from app.crawl.adapters.base import Publication
from app.crawl.fetcher import FetchResult
from app.crawl.snapshot import record_version
from app.models.schema import Change, CrawlRun, Document, DocumentVersion, Source
from app.schedule.leases import LeaseClaim, LeaseLost, LeaseRepository
from app.schedule.scheduler import Scheduler, arq_settings
from app.schedule.worker import crawl_source, execute_with_heartbeat, shutdown, startup
from test_leases import LeaseEnvironment, leased as shared_leased, schema_engine
from test_snapshots import StorageFixture, storage as shared_storage

leased = shared_leased
storage = shared_storage
pytestmark = pytest.mark.skipif("POSTGRES_DB" not in os.environ, reason="requires real services")


@pytest_asyncio.fixture
async def queue() -> AsyncIterator[tuple[ArqRedis, str]]:
    name = "worker_test_" + uuid4().hex
    redis = await create_pool(arq_settings(RedisSettings()))
    try:
        yield redis, name
    finally:
        keys = [key async for key in redis.scan_iter(match="*" + name + "*")]
        if keys:
            await redis.delete(*keys)
        await redis.aclose()


def schedule_process(schema: str, name: str, barrier: Any, results: Any) -> None:
    async def run() -> None:
        engine = schema_engine(schema)
        redis = await create_pool(arq_settings(RedisSettings()))
        try:
            barrier.wait(timeout=15)
            claims = await Scheduler(sessionmaker(engine), redis, queue_name=name).tick()
            results.put([str(claim.source_id) for claim in claims])
        finally:
            await redis.aclose()
            engine.dispose()

    asyncio.run(run())


@pytest.mark.asyncio
async def test_two_scheduler_processes_enqueue_one_source(
    leased: LeaseEnvironment, queue: tuple[ArqRedis, str]
) -> None:
    redis, name = queue
    ctx = multiprocessing.get_context("spawn")
    barrier, results = ctx.Barrier(3), ctx.Queue()
    processes = [
        ctx.Process(target=schedule_process, args=(leased.schema, name, barrier, results))
        for _ in range(2)
    ]
    try:
        for process in processes:
            process.start()
        barrier.wait(timeout=15)
        batches = [results.get(timeout=20), results.get(timeout=20)]
        for process in processes:
            process.join(timeout=10)
            assert process.exitcode == 0
        assert [source for batch in batches for source in batch] == [str(leased.source_id)]
        assert await redis.zcard(name) == 1
    finally:
        for process in processes:
            if process.is_alive():
                process.kill()
                process.join(timeout=5)
            process.close()
        results.close()
        results.join_thread()


@pytest.mark.asyncio
async def test_enqueue_failure_recovers_without_losing_source(
    leased: LeaseEnvironment, queue: tuple[ArqRedis, str]
) -> None:
    redis, name = queue
    bad = ArqRedis(host="127.0.0.1", port=1, socket_connect_timeout=0.1, socket_timeout=0.1)
    try:
        with pytest.raises(RuntimeError, match="enqueue failed"):
            await Scheduler(leased.sessions, bad, queue_name=name).tick()
    finally:
        await bad.aclose()
    with leased.sessions() as session:
        source = session.get(Source, leased.source_id)
        assert source is not None and source.lease_token is not None
    leased.expire()
    claims = await Scheduler(leased.sessions, redis, queue_name=name).tick()
    assert len(claims) == 1 and await redis.zcard(name) == 1
    with leased.sessions() as session:
        assert (
            session.scalar(
                select(func.count()).select_from(CrawlRun).where(CrawlRun.status == "failed")
            )
            == 1
        )


@pytest.mark.asyncio
async def test_heartbeat_loss_aborts_worker_operation(leased: LeaseEnvironment) -> None:
    claim = LeaseRepository(leased.sessions, lease_s=0.3).claim_due()[0]
    entered, cancelled = asyncio.Event(), asyncio.Event()

    async def operation() -> dict[str, str]:
        entered.set()
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.set()
        return {"status": "unreachable"}

    task = asyncio.create_task(
        execute_with_heartbeat(
            leased.sessions, claim, LeaseSettings(lease_duration_s=0.3), operation
        )
    )
    await entered.wait()
    leased.expire()
    replacement = LeaseRepository(leased.sessions).claim_due()[0]
    with pytest.raises(LeaseLost):
        await asyncio.wait_for(task, 3)
    assert cancelled.is_set()
    LeaseRepository(leased.sessions).heartbeat(replacement)


class FixtureAdapter:
    def discover(self, content: bytes, *, base_url: str) -> list[Publication]:
        return [
            Publication("https://fixture.invalid/a", "A"),
            Publication("https://fixture.invalid/b", "B"),
        ]


class BlockingStore(SnapshotStore):
    def __init__(self, client: Any, bucket: str, uploaded: Any) -> None:
        super().__init__(client, bucket)
        self.uploaded = uploaded

    def put(self, content: bytes, *, content_type: str = "application/octet-stream") -> Snapshot:
        snapshot = super().put(content, content_type=content_type)
        self.uploaded.set()
        # Parent SIGKILLs this process; the event is never signalled in normal flow.
        if not threading.Event().wait(timeout=30):
            raise RuntimeError("Parent did not kill the blocked worker")
        return snapshot


def worker_process(schema: str, name: str, bucket: str, ready: Any, uploaded: Any | None) -> None:
    logging.basicConfig(level=logging.WARNING)

    async def run() -> None:
        redis = await create_pool(arq_settings(RedisSettings()))

        async def initialize(ctx: dict[str, Any]) -> None:
            await startup(ctx)
            ctx["engine"].dispose()
            engine = schema_engine(schema)
            ctx["engine"], ctx["sessions"] = engine, sessionmaker(engine)
            ctx["lease_settings"] = LeaseSettings(lease_duration_s=3)
            ctx["fetch_settings"] = FetchSettings(
                crawl_delay_s=0.001, crawl_max_retries=0, crawl_total_timeout_s=2
            )
            ctx["redis_settings"] = RedisSettings(redis_key_prefix=name, rate_interval_s=0.001)
            ctx["store"] = (
                SnapshotStore(ctx["s3"], bucket)
                if uploaded is None
                else BlockingStore(ctx["s3"], bucket, uploaded)
            )
            ctx["adapters"] = {"fixture": FixtureAdapter()}

            def handler(request: httpx.Request) -> httpx.Response:
                assert request.url.host == "fixture.invalid", "All HTTP must stay mocked"
                body = (
                    b"User-agent: *\nAllow: /"
                    if request.url.path == "/robots.txt"
                    else (
                        b"listing"
                        if request.url.path == "/feed"
                        else ("new " + request.url.path).encode()
                    )
                )
                return httpx.Response(
                    200, headers={"content-type": "text/html"}, stream=httpx.ByteStream(body)
                )

            ctx["http_transport"] = httpx.MockTransport(handler)
            ready.set()

        worker = Worker(
            [crawl_source],
            redis_pool=redis,
            queue_name=name,
            on_startup=initialize,
            on_shutdown=shutdown,
            poll_delay=0.01,
            max_jobs=2,
            queue_read_limit=20,
            job_timeout=60,
            keep_result=60,
            log_results=False,
        )
        try:
            await worker.async_run()
        finally:
            await worker.close()
            await redis.aclose()

    asyncio.run(run())


@pytest.mark.asyncio
async def test_hard_kill_arq_worker_recovers_without_duplicate_versions_or_changes(
    leased: LeaseEnvironment, queue: tuple[ArqRedis, str], storage: StorageFixture
) -> None:
    redis, name = queue
    with leased.sessions.begin() as session:
        second = Document(
            source_id=leased.source_id, canonical_url="https://fixture.invalid/b", title="B"
        )
        session.add(second)
        session.flush()
        ids = [leased.document_id, second.id]
    for identity, path in zip(ids, ["a", "b"]):
        record_version(
            leased.sessions,
            storage.store,
            identity,
            FetchResult("https://fixture.invalid/" + path, 200, {}, ("old " + path).encode()),
            normalized_text="",
            sections=[],
        )
    ctx = multiprocessing.get_context("spawn")
    ready, uploaded = ctx.Event(), ctx.Event()
    first = ctx.Process(
        target=worker_process, args=(leased.schema, name, storage.bucket, ready, uploaded)
    )
    second_worker = None
    try:
        first.start()
        assert await asyncio.to_thread(ready.wait, 15)
        claim = (await Scheduler(leased.sessions, redis, queue_name=name, lease_s=3).tick())[0]
        assert await asyncio.to_thread(uploaded.wait, 15)
        first.kill()
        first.join(timeout=5)
        assert first.exitcode is not None and first.exitcode < 0
        ready2 = ctx.Event()
        second_worker = ctx.Process(
            target=worker_process, args=(leased.schema, name, storage.bucket, ready2, None)
        )
        second_worker.start()
        assert await asyncio.to_thread(ready2.wait, 15)
        # Poll real scheduler/DB state until Postgres observes natural lease expiry.
        # This is the functionality under test, not a retry of a failed assertion.
        async with asyncio.timeout(20):
            replacement: list[LeaseClaim] = []
            while not replacement:
                replacement = await Scheduler(
                    leased.sessions, redis, queue_name=name, lease_s=3
                ).tick()
                if not replacement:
                    await asyncio.sleep(0.02)
            while True:
                with leased.sessions() as session:
                    finished = session.get(CrawlRun, replacement[0].run_id)
                    if finished is not None and finished.status != "running":
                        assert finished.status == "succeeded" and finished.new_versions == 2
                        break
                await asyncio.sleep(0.02)
        with leased.sessions() as session:
            assert session.scalar(select(func.count()).select_from(DocumentVersion)) == 4
            assert session.scalar(select(func.count()).select_from(Change)) == 2
            abandoned = session.get(CrawlRun, claim.run_id)
            assert abandoned is not None and abandoned.status == "failed"
            source = session.get(Source, leased.source_id)
            assert source is not None and source.lease_token is None
        assert storage.client.list_objects_v2(Bucket=storage.bucket)["KeyCount"] == 4
        replay = await crawl_source(
            {"sessions": leased.sessions, "lease_settings": LeaseSettings()},
            str(replacement[0].source_id),
            str(replacement[0].token),
            str(replacement[0].run_id),
        )
        assert replay["status"] == "already_completed"
        assert await Scheduler(leased.sessions, redis, queue_name=name).tick() == []
    finally:
        for process in [first, second_worker]:
            if process is not None:
                if process.is_alive():
                    process.terminate()
                    process.join(timeout=5)
                if process.is_alive():
                    process.kill()
                    process.join(timeout=5)
                process.close()


@pytest.mark.asyncio
async def test_heartbeat_keeps_operation_owned_past_initial_expiry(
    leased: LeaseEnvironment,
) -> None:
    settings = LeaseSettings(lease_duration_s=0.6)
    claim = LeaseRepository(leased.sessions, lease_s=settings.lease_duration_s).claim_due()[0]
    with leased.sessions() as session:
        initial = session.scalar(
            select(Source.lease_expires_at).where(Source.id == leased.source_id)
        )
    entered, release = asyncio.Event(), asyncio.Event()

    async def operation() -> str:
        entered.set()
        await release.wait()
        return "finished"

    task = asyncio.create_task(execute_with_heartbeat(leased.sessions, claim, settings, operation))
    try:
        await entered.wait()
        async with asyncio.timeout(5):
            while True:
                with leased.sessions() as session:
                    if session.scalar(select(func.now() > initial)):
                        assert (
                            session.scalar(
                                select(Source.lease_expires_at > func.now()).where(
                                    Source.id == leased.source_id
                                )
                            )
                            is True
                        )
                        break
                await asyncio.sleep(0.02)  # Observe real DB expiry, not a test retry.
        assert not task.done()
        assert LeaseRepository(leased.sessions).claim_due() == []
        release.set()
        assert await task == "finished"
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
