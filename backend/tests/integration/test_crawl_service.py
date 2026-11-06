from __future__ import annotations

import asyncio
import multiprocessing
import os
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any
from uuid import UUID, uuid4

import httpx
import pytest
import pytest_asyncio
from sqlalchemy import create_engine, delete, event, func, select
from sqlalchemy.engine import Connection
from sqlalchemy.orm import Session, sessionmaker

from app.core.config import FetchSettings, Settings
from app.core.s3 import Snapshot, SnapshotStore, make_s3_client
from app.crawl.fetcher import Fetcher
from app.crawl.service import CrawlService, CrawlSummary
from app.diff.hashing import content_hash
from app.models.schema import Change, CrawlRun, Document, DocumentVersion, Source

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client

FEED_URL = "https://www.rbi.org.in/notifications_rss.xml"
FEED_PATH = Path(__file__).parents[1] / "fixtures" / "rbi_notifications.xml"

pytestmark = pytest.mark.skipif(
    "MINIO_ROOT_PASSWORD" not in os.environ or "POSTGRES_DB" not in os.environ,
    reason="run in Compose for crawl integration tests",
)


class Clock:
    def __init__(self) -> None:
        self.time = 0.0

    def now(self) -> float:
        return self.time

    async def sleep(self, seconds: float) -> None:
        self.time += seconds
        await asyncio.sleep(0)


@dataclass
class Regulator:
    feed: bytes = field(default_factory=FEED_PATH.read_bytes)
    bodies: dict[str, bytes] = field(
        default_factory=lambda: {
            "10001": b"%PDF-synthetic-first",
            "10002": b"%PDF-synthetic-second",
        }
    )
    failing: set[str] = field(default_factory=set)
    robots_status: int = 200
    pause_after_first: bool = False
    requests: list[str] = field(default_factory=list)
    response_headers: dict[str, str] = field(default_factory=dict)
    captured: list[httpx.Request] = field(default_factory=list)
    not_modified: bool = False

    def respond(self, request: httpx.Request) -> httpx.Response:
        assert request.url.host == "www.rbi.org.in", "Unexpected mock destination"
        self.requests.append(str(request.url))
        self.captured.append(request)
        if request.url.path == "/robots.txt":
            return httpx.Response(
                self.robots_status, stream=httpx.ByteStream(b"User-agent: *\nAllow: /")
            )
        if request.url.path == "/notifications_rss.xml":
            return httpx.Response(200, stream=httpx.ByteStream(self.feed))
        assert request.url.path == "/Scripts/NotificationUser.aspx", "Unexpected mock path"
        identity = request.url.params["Id"]
        if identity in self.failing:
            return httpx.Response(503, stream=httpx.ByteStream(b""))
        if self.pause_after_first and identity == "10001":
            self.robots_status = 503
        return httpx.Response(
            304 if self.not_modified else 200,
            stream=httpx.ByteStream(b"" if self.not_modified else self.bodies[identity]),
            headers={"content-type": "application/pdf", **self.response_headers},
        )


def fetcher(client: httpx.AsyncClient, *, ttl: float = 3600) -> Fetcher:
    clock = Clock()
    return Fetcher(
        client,
        FetchSettings(crawl_max_retries=0, crawl_delay_s=2, robots_cache_ttl_s=ttl),
        clock=clock.now,
        sleep=clock.sleep,
    )


@dataclass
class CrawlEnvironment:
    sessions: sessionmaker[Session]
    source_id: UUID
    store: SnapshotStore
    s3: S3Client
    bucket: str
    regulator: Regulator
    client: httpx.AsyncClient
    service: CrawlService

    def totals(self) -> tuple[int, int, int]:
        with self.sessions() as session:
            docs = select(Document.id).where(Document.source_id == self.source_id)
            return (
                session.scalar(
                    select(func.count())
                    .select_from(Document)
                    .where(Document.source_id == self.source_id)
                )
                or 0,
                session.scalar(
                    select(func.count())
                    .select_from(DocumentVersion)
                    .where(DocumentVersion.document_id.in_(docs))
                )
                or 0,
                session.scalar(
                    select(func.count()).select_from(Change).where(Change.document_id.in_(docs))
                )
                or 0,
            )

    def run(self, identity: UUID) -> CrawlRun:
        with self.sessions() as session:
            run = session.get(CrawlRun, identity)
            assert run is not None
            return run


@pytest_asyncio.fixture
async def env() -> AsyncIterator[CrawlEnvironment]:
    engine = create_engine(Settings().database_url(), connect_args={"connect_timeout": 5})
    sessions = sessionmaker(engine)
    s3 = make_s3_client()
    bucket = f"regwatch-test-{uuid4().hex}"
    s3.create_bucket(Bucket=bucket)
    with sessions.begin() as session:
        source = Source(
            name=f"crawl-test-{uuid4().hex}", base_url=FEED_URL, adapter="rbi", status="active"
        )
        session.add(source)
        session.flush()
        source_id = source.id
    regulator = Regulator()
    try:
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(regulator.respond), trust_env=False
        ) as client:
            store = SnapshotStore(s3, bucket)
            yield CrawlEnvironment(
                sessions,
                source_id,
                store,
                s3,
                bucket,
                regulator,
                client,
                CrawlService(sessions, fetcher(client), store),
            )
    finally:
        # Remove only this fixture's rows and uniquely named object bucket.
        with sessions.begin() as session:
            docs = select(Document.id).where(Document.source_id == source_id)
            session.execute(delete(Change).where(Change.document_id.in_(docs)))
            session.execute(delete(DocumentVersion).where(DocumentVersion.document_id.in_(docs)))
            session.execute(delete(Document).where(Document.source_id == source_id))
            session.execute(delete(CrawlRun).where(CrawlRun.source_id == source_id))
            session.execute(delete(Source).where(Source.id == source_id))
        for item in s3.list_objects_v2(Bucket=bucket).get("Contents", []):
            s3.delete_object(Bucket=bucket, Key=item["Key"])
        s3.delete_bucket(Bucket=bucket)
        s3.close()
        engine.dispose()


def assert_counts(summary: CrawlSummary, **values: int) -> None:
    expected = {
        "discovered": 0,
        "fetched": 0,
        "new_versions": 0,
        "unchanged": 0,
        "failed": 0,
        "paused": 0,
    }
    expected.update(values)
    assert summary.counts.values() == expected


@pytest.mark.asyncio
async def test_first_crawl_creates_versions_without_changes(env: CrawlEnvironment) -> None:
    summary = await env.service.run(env.source_id)
    assert summary.status == "succeeded"
    assert_counts(summary, discovered=2, fetched=2, new_versions=2)
    assert env.totals() == (2, 2, 0)
    run = env.run(summary.run_id)
    assert run.status == "succeeded" and run.error is None and run.finished_at is not None
    assert run.discovered == 2 and run.fetched == 2 and run.new_versions == 2
    assert run.duration_ms is not None and run.duration_ms >= 0
    with env.sessions() as session:
        source = session.get(Source, env.source_id)
        assert source is not None and source.last_success_at is not None


@pytest.mark.asyncio
async def test_repeat_crawl_is_unchanged_without_new_versions_or_changes(
    env: CrawlEnvironment,
) -> None:
    await env.service.run(env.source_id)
    summary = await env.service.run(env.source_id)
    assert summary.status == "succeeded"
    assert_counts(summary, discovered=2, fetched=2, unchanged=2)
    assert env.totals() == (2, 2, 0)
    assert env.s3.list_objects_v2(Bucket=env.bucket)["KeyCount"] == 2
    assert env.run(summary.run_id).unchanged == 2


@pytest.mark.asyncio
async def test_changed_bytes_create_one_linked_naive_change(env: CrawlEnvironment) -> None:
    await env.service.run(env.source_id)
    old_hash = content_hash(env.regulator.bodies["10001"])
    env.regulator.bodies["10001"] = b"%PDF-synthetic-first updated"
    summary = await env.service.run(env.source_id)
    assert_counts(summary, discovered=2, fetched=2, new_versions=1, unchanged=1)
    assert env.totals() == (2, 3, 1)
    with env.sessions() as session:
        change = session.scalar(
            select(Change).join(Document).where(Document.source_id == env.source_id)
        )
        assert change is not None and not change.is_cosmetic
        old = session.get(DocumentVersion, change.from_version_id)
        new = session.get(DocumentVersion, change.to_version_id)
        assert old is not None and new is not None
        assert old.content_hash == old_hash
        assert new.content_hash == content_hash(env.regulator.bodies["10001"])
        assert old.document_id == new.document_id == change.document_id
        assert change.diff == {
            "kind": "raw_content_changed",
            "from_hash": old.content_hash,
            "to_hash": new.content_hash,
        }
    repeat = await env.service.run(env.source_id)
    assert_counts(repeat, discovered=2, fetched=2, unchanged=2)
    assert env.totals() == (2, 3, 1)


@pytest.mark.asyncio
async def test_one_document_failing_does_not_stop_the_other(env: CrawlEnvironment) -> None:
    env.regulator.failing.add("10001")
    summary = await env.service.run(env.source_id)
    assert summary.status == "failed"
    assert_counts(summary, discovered=2, fetched=1, new_versions=1, failed=1)
    assert env.totals() == (2, 1, 0)
    assert env.run(summary.run_id).error == "Document processing failed: FetchError"
    env.regulator.failing.clear()
    retry = await env.service.run(env.source_id)
    assert_counts(retry, discovered=2, fetched=2, new_versions=1, unchanged=1)
    assert env.totals() == (2, 2, 0)


@pytest.mark.asyncio
async def test_robots_paused_source_stops_and_records_reason(env: CrawlEnvironment) -> None:
    env.regulator.robots_status = 503
    summary = await env.service.run(env.source_id)
    assert summary.status == "paused"
    assert_counts(summary, paused=1)
    assert env.totals() == (0, 0, 0)
    assert env.run(summary.run_id).error == "Source paused: robots.txt returned HTTP 503"
    with env.sessions() as session:
        source = session.get(Source, env.source_id)
        assert source is not None and source.status == "paused"
    requests = len(env.regulator.requests)
    next_run = await env.service.run(env.source_id)
    assert next_run.status == "paused" and "reactivation required" in (next_run.reason or "")
    assert len(env.regulator.requests) == requests


@pytest.mark.asyncio
async def test_robots_paused_mid_run_preserves_progress_and_stops(env: CrawlEnvironment) -> None:
    env.regulator.pause_after_first = True
    service = CrawlService(env.sessions, fetcher(env.client, ttl=3), env.store)
    summary = await service.run(env.source_id)
    assert summary.status == "paused"
    assert_counts(summary, discovered=2, fetched=1, new_versions=1, paused=1)
    assert env.totals() == (2, 1, 0)
    assert not any("Id=10002" in request for request in env.regulator.requests)


@pytest.mark.asyncio
async def test_malformed_discovery_is_recorded_as_failed(env: CrawlEnvironment) -> None:
    env.regulator.feed = b"<html>not a regulator feed</html>"
    summary = await env.service.run(env.source_id)
    assert summary.status == "failed"
    assert_counts(summary, failed=1)
    assert env.totals() == (0, 0, 0)
    assert "AdapterParseError" in (summary.reason or "")


class ExitAfterUpload(SnapshotStore):
    def put(self, content: bytes, *, content_type: str = "application/octet-stream") -> Snapshot:
        super().put(content, content_type=content_type)
        os._exit(17)  # Test-only hard process exit: no finally block or DB insert can run.


def crash_child(source_id: str, bucket: str, feed: bytes, bodies: dict[str, bytes]) -> None:
    engine = create_engine(Settings().database_url(), connect_args={"connect_timeout": 5})
    client = make_s3_client()
    regulator = Regulator(feed=feed, bodies=bodies)

    async def run() -> None:
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(regulator.respond), trust_env=False
        ) as http:
            service = CrawlService(
                sessionmaker(engine), fetcher(http), ExitAfterUpload(client, bucket)
            )
            await service.run(UUID(source_id))

    asyncio.run(run())


@pytest.mark.asyncio
async def test_hard_crash_after_upload_retries_without_duplicates(env: CrawlEnvironment) -> None:
    process = multiprocessing.get_context("spawn").Process(
        target=crash_child,
        args=(str(env.source_id), env.bucket, env.regulator.feed, env.regulator.bodies),
    )
    process.start()
    try:
        process.join(timeout=30)
        assert not process.is_alive(), "Crash-test child did not finish in time"
        assert process.exitcode == 17
    finally:
        if process.is_alive():
            process.terminate()
            process.join(timeout=5)
        process.close()
    assert env.totals() == (1, 0, 0)
    assert env.s3.list_objects_v2(Bucket=env.bucket)["KeyCount"] == 1
    with env.sessions() as session:
        abandoned = session.scalar(select(CrawlRun).where(CrawlRun.source_id == env.source_id))
        assert abandoned is not None and abandoned.status == "running"
        assert abandoned.discovered == 2 and abandoned.fetched == 1
    retry = await env.service.run(env.source_id)
    assert_counts(retry, discovered=2, fetched=2, new_versions=2)
    assert env.totals() == (2, 2, 0)
    assert env.s3.list_objects_v2(Bucket=env.bucket)["KeyCount"] == 2
    repeat = await env.service.run(env.source_id)
    assert_counts(repeat, discovered=2, fetched=2, unchanged=2)
    assert env.totals() == (2, 2, 0)


@pytest.mark.asyncio
async def test_version_and_change_roll_back_together(env: CrawlEnvironment) -> None:
    await env.service.run(env.source_id)
    env.regulator.bodies["10001"] = b"%PDF-changed-before-injected-DB-failure"
    with env.sessions() as session:
        engine = session.get_bind()

    def reject_change(
        connection: Connection,
        cursor: Any,
        statement: str,
        parameters: Any,
        context: Any,
        executemany: bool,
    ) -> None:
        if statement.startswith("INSERT INTO changes"):
            raise RuntimeError("Injected change insertion failure")

    event.listen(engine, "before_cursor_execute", reject_change)
    try:
        failed = await env.service.run(env.source_id)
    finally:
        event.remove(engine, "before_cursor_execute", reject_change)
    assert failed.status == "failed"
    assert_counts(failed, discovered=2, fetched=2, unchanged=1, failed=1)
    assert env.totals() == (2, 2, 0)
    assert env.s3.list_objects_v2(Bucket=env.bucket)["KeyCount"] == 3
    retry = await env.service.run(env.source_id)
    assert_counts(retry, discovered=2, fetched=2, new_versions=1, unchanged=1)
    assert env.totals() == (2, 3, 1)


@pytest.mark.asyncio
async def test_document_limit_processes_only_first_unique_document(env: CrawlEnvironment) -> None:
    first = await env.service.run(env.source_id, max_documents=1)
    assert_counts(first, discovered=2, fetched=1, new_versions=1)
    assert env.totals() == (1, 1, 0)
    assert not any("Id=10002" in request for request in env.regulator.requests)
    repeat = await env.service.run(env.source_id, max_documents=1)
    assert_counts(repeat, discovered=2, fetched=1, unchanged=1)
    assert env.totals() == (1, 1, 0)


@pytest.mark.asyncio
async def test_bad_document_limit_has_no_crawl_or_http_side_effect(env: CrawlEnvironment) -> None:
    with pytest.raises(ValueError, match="max_documents"):
        await env.service.run(env.source_id, max_documents=0)
    assert env.regulator.requests == []
    with env.sessions() as session:
        assert (
            session.scalar(
                select(func.count())
                .select_from(CrawlRun)
                .where(CrawlRun.source_id == env.source_id)
            )
            == 0
        )


@pytest.mark.asyncio
async def test_discovery_failure_exposes_exact_sanitized_error(env: CrawlEnvironment) -> None:
    env.regulator.feed = b"<html>not an RSS feed</html>"
    summary = await env.service.run(env.source_id, max_documents=1)
    assert summary.status == "failed"
    assert summary.errors == ("AdapterParseError: Expected an RSS 2.0 feed",)


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [400, 401, 403, 418, 429, 451, 499])
async def test_robots_4xx_persists_paused_source_and_exact_reason(
    env: CrawlEnvironment, status: int
) -> None:
    env.regulator.robots_status = status
    summary = await env.service.run(env.source_id)
    assert summary.status == "paused"
    assert_counts(summary, paused=1)
    expected = f"Source paused: robots.txt returned HTTP {status}"
    assert summary.reason == expected
    assert env.run(summary.run_id).error == expected
    with env.sessions() as session:
        source = session.get(Source, env.source_id)
        assert source is not None and source.status == "paused"
    assert env.regulator.requests == ["https://www.rbi.org.in/robots.txt"]
    assert env.totals() == (0, 0, 0)


WEAK_ETAG = 'W/"preserve-this-value"'
LAST_MODIFIED = "Thu, 05 Nov 2026 00:00:00 GMT"


def document_validators(env: CrawlEnvironment) -> tuple[str | None, str | None, str | None]:
    with env.sessions() as session:
        document = session.scalar(select(Document).where(Document.source_id == env.source_id))
        assert document is not None
        return document.etag, document.last_modified, document.validator_url


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failure_sql", ["INSERT INTO document_versions", "UPDATE documents SET etag"]
)
async def test_validator_state_rolls_back_with_version_failure(
    env: CrawlEnvironment, failure_sql: str
) -> None:
    env.regulator.response_headers = {"etag": WEAK_ETAG, "last-modified": LAST_MODIFIED}
    await env.service.run(env.source_id, max_documents=1)
    old = document_validators(env)
    env.regulator.bodies["10001"] = b"changed but transaction must fail"
    env.regulator.response_headers = {"etag": '"new-value"'}
    with env.sessions() as session:
        engine = session.get_bind()

    def reject(
        connection: Connection,
        cursor: Any,
        statement: str,
        parameters: Any,
        context: Any,
        executemany: bool,
    ) -> None:
        if statement.startswith(failure_sql):
            raise RuntimeError("Injected validator transaction failure")

    event.listen(engine, "before_cursor_execute", reject)
    try:
        summary = await env.service.run(env.source_id, max_documents=1)
    finally:
        event.remove(engine, "before_cursor_execute", reject)
    assert summary.status == "failed"
    assert document_validators(env) == old
    assert env.totals() == (1, 1, 0)


@pytest.mark.asyncio
async def test_304_success_keeps_one_version_and_preserves_weak_etag(env: CrawlEnvironment) -> None:
    env.regulator.response_headers = {"etag": WEAK_ETAG, "last-modified": LAST_MODIFIED}
    first = await env.service.run(env.source_id, max_documents=1)
    assert first.status == "succeeded"
    stored = document_validators(env)
    assert stored == (
        WEAK_ETAG,
        LAST_MODIFIED,
        next(r for r in env.regulator.requests if "Id=10001" in r),
    )
    env.regulator.not_modified = True
    second = await env.service.run(env.source_id, max_documents=1)
    assert second.status == "succeeded"
    assert_counts(second, discovered=2, fetched=1, unchanged=1)
    assert env.totals() == (1, 1, 0)
    assert env.s3.list_objects_v2(Bucket=env.bucket)["KeyCount"] == 1
    assert document_validators(env) == stored
    request = env.regulator.captured[-1]
    assert request.headers["if-none-match"] == WEAK_ETAG
    assert request.headers["if-modified-since"] == LAST_MODIFIED


@pytest.mark.asyncio
@pytest.mark.parametrize("status_304", [False, True])
async def test_absent_response_validators_clear_all_columns(
    env: CrawlEnvironment, status_304: bool
) -> None:
    env.regulator.response_headers = {"etag": WEAK_ETAG, "last-modified": LAST_MODIFIED}
    await env.service.run(env.source_id, max_documents=1)
    env.regulator.response_headers.clear()
    env.regulator.not_modified = status_304
    summary = await env.service.run(env.source_id, max_documents=1)
    assert summary.status == "succeeded"
    assert document_validators(env) == (None, None, None)
    assert env.totals() == (1, 1, 0)


@pytest.mark.asyncio
async def test_200_changed_content_is_processed_even_with_repeated_validator(
    env: CrawlEnvironment,
) -> None:
    env.regulator.response_headers = {"etag": WEAK_ETAG}
    await env.service.run(env.source_id, max_documents=1)
    env.regulator.bodies["10001"] = b"server changed bytes but reused its weak validator"
    summary = await env.service.run(env.source_id, max_documents=1)
    assert summary.status == "succeeded"
    assert_counts(summary, discovered=2, fetched=1, new_versions=1)
    assert env.totals() == (1, 2, 1)
    assert env.regulator.captured[-1].headers["if-none-match"] == WEAK_ETAG


@pytest.mark.asyncio
async def test_304_without_an_existing_version_fails(env: CrawlEnvironment) -> None:
    env.regulator.not_modified = True
    summary = await env.service.run(env.source_id, max_documents=1)
    assert summary.status == "failed" and summary.counts.failed == 1
    assert env.totals() == (1, 0, 0)
    assert document_validators(env) == (None, None, None)
    assert env.s3.list_objects_v2(Bucket=env.bucket)["KeyCount"] == 0


@pytest.mark.asyncio
async def test_concurrent_late_304_cannot_overwrite_newer_validator_state(
    env: CrawlEnvironment,
) -> None:
    env.regulator.response_headers = {"etag": WEAK_ETAG}
    await env.service.run(env.source_id, max_documents=1)
    arrived = asyncio.Event()
    release = asyncio.Event()
    first = True

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal first
        if request.url.path.endswith("NotificationUser.aspx"):
            if first:
                first = False
                arrived.set()
                await release.wait()
                return httpx.Response(304, headers={"etag": WEAK_ETAG})
            return httpx.Response(
                200, headers={"etag": '"newer"'}, stream=httpx.ByteStream(b"newer body")
            )
        return env.regulator.respond(request)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler), trust_env=False) as client:
        service = CrawlService(env.sessions, fetcher(client), env.store)
        late = asyncio.create_task(service.run(env.source_id, max_documents=1))
        try:
            await asyncio.wait_for(arrived.wait(), 5)
            newer = await CrawlService(env.sessions, fetcher(client), env.store).run(
                env.source_id, max_documents=1
            )
            assert newer.status == "succeeded"
        finally:
            release.set()
        old = await asyncio.wait_for(late, 5)
    assert old.status == "failed"
    assert any("Document changed during conditional request" in error for error in old.errors)
    assert document_validators(env)[0] == '"newer"'
    assert env.totals() == (1, 2, 1)


@pytest.mark.asyncio
async def test_snapshot_failure_keeps_previous_validators(
    env: CrawlEnvironment, monkeypatch: pytest.MonkeyPatch
) -> None:
    from app.core.s3 import SnapshotStorageError

    env.regulator.response_headers = {"etag": WEAK_ETAG, "last-modified": LAST_MODIFIED}
    await env.service.run(env.source_id, max_documents=1)
    before = document_validators(env)
    env.regulator.response_headers = {"etag": '"new-value"'}
    env.regulator.bodies["10001"] = b"new body cannot be stored"

    def fail(content: bytes, *, content_type: str = "application/octet-stream") -> Snapshot:
        raise SnapshotStorageError("Injected storage failure")

    monkeypatch.setattr(env.store, "put", fail)
    summary = await env.service.run(env.source_id, max_documents=1)
    assert summary.status == "failed"
    assert document_validators(env) == before
    assert env.totals() == (1, 1, 0)
