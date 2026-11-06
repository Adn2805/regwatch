"""Real Postgres ownership tests; each fixture owns a separate schema."""

import multiprocessing
import os
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Any
from uuid import UUID, uuid4

import pytest
from sqlalchemy import create_engine, select, text, event
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from app.core.config import Settings
from app.crawl.adapters.base import Publication
from app.crawl.conditional import DocumentRequestState, Validators
from app.crawl.fetcher import FetchResult
from app.crawl.repository import CrawlRepository, RunCounts
from app.crawl.snapshot import record_version
from app.models import Base
from app.models.schema import CrawlRun, Document, Source
from app.schedule.leases import LeaseClaim, LeaseLost, LeaseRepository
from test_snapshots import StorageFixture, storage as shared_storage

# Re-export the existing real-MinIO fixture under its pytest injection name.
storage = shared_storage

pytestmark = pytest.mark.skipif("POSTGRES_DB" not in os.environ, reason="requires real PostgreSQL")


def schema_engine(schema: str) -> Engine:
    assert schema.startswith("lease_test_") and schema.removeprefix("lease_test_").isalnum()
    return create_engine(
        Settings().database_url(),
        pool_timeout=5,
        connect_args={
            "connect_timeout": 5,
            "options": f"-c search_path={schema} -c statement_timeout=10000 -c lock_timeout=5000",
        },
    )


@dataclass
class LeaseEnvironment:
    schema: str
    sessions: sessionmaker[Session]
    source_id: UUID
    document_id: UUID

    def expire(self) -> None:
        with self.sessions.begin() as session:
            session.execute(
                text("UPDATE sources SET lease_expires_at=now()-interval '1 second' WHERE id=:id"),
                {"id": self.source_id},
            )

    def snapshot(self) -> dict[str, list[Any]]:
        with self.sessions() as session:
            return {
                table: list(
                    session.scalars(text(f"SELECT row_to_json(t) FROM {table} t ORDER BY id"))
                )
                for table in ("sources", "documents", "document_versions", "changes", "crawl_runs")
            }


@pytest.fixture
def leased() -> Iterator[LeaseEnvironment]:
    schema = "lease_test_" + uuid4().hex
    admin = create_engine(Settings().database_url(), connect_args={"connect_timeout": 5})
    with admin.begin() as connection:
        connection.execute(text(f'CREATE SCHEMA "{schema}"'))
    engine = schema_engine(schema)
    try:
        Base.metadata.create_all(engine)
        sessions = sessionmaker(engine)
        with sessions.begin() as session:
            source = Source(
                name="lease fixture",
                base_url="https://fixture.invalid/feed",
                adapter="fixture",
                status="active",
            )
            session.add(source)
            session.flush()
            document = Document(
                source_id=source.id,
                canonical_url="https://fixture.invalid/a",
                title="Original title",
            )
            session.add(document)
            session.flush()
            source_id, document_id = source.id, document.id
        yield LeaseEnvironment(schema, sessions, source_id, document_id)
    finally:
        engine.dispose()
        with admin.begin() as connection:
            connection.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
        admin.dispose()


def claim(leased: LeaseEnvironment) -> LeaseClaim:
    values = LeaseRepository(leased.sessions).claim_due(source_ids=[leased.source_id])
    assert len(values) == 1
    return values[0]


@pytest.mark.parametrize("write", ["version", "document", "checkpoint", "completion", "304"])
def test_expired_owner_cannot_write_after_new_owner_claims(
    leased: LeaseEnvironment, storage: StorageFixture, write: str
) -> None:
    first = claim(leased)
    old = CrawlRepository(leased.sessions, claim=first)
    context = old.start(leased.source_id)
    leased.expire()
    second = claim(leased)
    assert second.token != first.token
    before = leased.snapshot()
    with pytest.raises(LeaseLost):
        if write == "version":
            record_version(
                leased.sessions,
                storage.store,
                leased.document_id,
                FetchResult("https://fixture.invalid/a", 200, {"etag": '"stale"'}, b"stale bytes"),
                normalized_text="",
                sections=[],
                claim=first,
            )
        elif write == "document":
            old.upsert_document(
                leased.source_id, Publication("https://fixture.invalid/a", "Stale title")
            )
        elif write == "checkpoint":
            old.checkpoint(first.run_id, RunCounts(fetched=99))
        elif write == "completion":
            old.finish(context, RunCounts(), "succeeded", None, 0)
        else:
            old.record_not_modified(
                leased.document_id,
                FetchResult("https://fixture.invalid/a", 304, {}, b""),
                DocumentRequestState(Validators(None, None, None), None),
            )
    assert leased.snapshot() == before
    assert storage.client.list_objects_v2(Bucket=storage.bucket)["KeyCount"] == 0


def test_expired_or_replaced_token_cannot_heartbeat(leased: LeaseEnvironment) -> None:
    first = claim(leased)
    repository = LeaseRepository(leased.sessions)
    leased.expire()
    with pytest.raises(LeaseLost):
        repository.heartbeat(first)
    second = claim(leased)
    with pytest.raises(LeaseLost):
        repository.heartbeat(first)
    repository.heartbeat(second)
    with leased.sessions() as session:
        assert (
            session.scalar(
                text(
                    "SELECT lease_expires_at > now() AND lease_heartbeat_at <= now() AND lease_expires_at=lease_heartbeat_at+interval '60 seconds' FROM sources"
                )
            )
            is True
        )


def test_completion_clears_lease_and_schedules_atomically(leased: LeaseEnvironment) -> None:
    current = claim(leased)
    repository = CrawlRepository(leased.sessions, claim=current)
    repository.finish(repository.start(leased.source_id), RunCounts(), "succeeded", None, 0)
    with leased.sessions() as session:
        assert (
            session.scalar(
                text(
                    "SELECT s.lease_token IS NULL AND s.lease_expires_at IS NULL AND s.lease_heartbeat_at IS NULL AND s.next_crawl_at=r.finished_at+s.crawl_interval_s*interval '1 second' AND s.last_success_at=r.finished_at AND r.status='succeeded' FROM sources s JOIN crawl_runs r ON r.source_id=s.id"
                )
            )
            is True
        )
    before = leased.snapshot()
    with pytest.raises(LeaseLost):
        repository.finish(repository.start(leased.source_id), RunCounts(), "succeeded", None, 0)
    assert leased.snapshot() == before


def test_recovery_preserves_unleased_runs_and_checkpoint_counts(leased: LeaseEnvironment) -> None:
    with leased.sessions.begin() as session:
        historical = CrawlRun(source_id=leased.source_id)
        session.add(historical)
        session.flush()
        historical_id = historical.id
    first = claim(leased)
    CrawlRepository(leased.sessions, claim=first).checkpoint(
        first.run_id, RunCounts(fetched=1, discovered=2)
    )
    leased.expire()
    assert LeaseRepository(leased.sessions).recover_expired() == 1
    assert LeaseRepository(leased.sessions).recover_expired() == 0
    with leased.sessions() as session:
        old = session.get(CrawlRun, first.run_id)
        legacy = session.get(CrawlRun, historical_id)
        assert (
            old is not None
            and old.status == "failed"
            and old.error == "Lease expired; abandoned crawl recovered"
        )
        assert old.finished_at is not None and old.fetched == 1 and old.discovered == 2
        assert (
            legacy is not None
            and legacy.status == "running"
            and legacy.finished_at is None
            and legacy.lease_token is None
        )


def claim_process(schema: str, source_id: str, barrier: Any, results: Any) -> None:
    engine = schema_engine(schema)
    try:
        barrier.wait(timeout=15)
        claimed = LeaseRepository(sessionmaker(engine)).claim_due(source_ids=[UUID(source_id)])
        results.put([(str(c.source_id), str(c.token), str(c.run_id)) for c in claimed])
    finally:
        engine.dispose()


def test_two_processes_never_claim_same_source(leased: LeaseEnvironment) -> None:
    ctx = multiprocessing.get_context("spawn")
    barrier = ctx.Barrier(3)
    results = ctx.Queue()
    workers = [
        ctx.Process(
            target=claim_process, args=(leased.schema, str(leased.source_id), barrier, results)
        )
        for _ in range(2)
    ]
    try:
        for worker in workers:
            worker.start()
        barrier.wait(timeout=15)
        batches = [results.get(timeout=20), results.get(timeout=20)]
        for worker in workers:
            worker.join(timeout=10)
            assert worker.exitcode == 0
        flattened = [item for batch in batches for item in batch]
        assert len(flattened) == 1 and flattened[0][0] == str(leased.source_id)
    finally:
        for worker in workers:
            if worker.is_alive():
                worker.kill()
                worker.join(timeout=5)
            worker.close()
        results.close()
        results.join_thread()


def test_skip_locked_claim_uses_an_independent_connection(leased: LeaseEnvironment) -> None:
    with leased.sessions.begin() as owner:
        owner.scalar(select(Source).where(Source.id == leased.source_id).with_for_update())
        with ThreadPoolExecutor(max_workers=1) as executor:
            pending = executor.submit(
                LeaseRepository(leased.sessions).claim_due, source_ids=[leased.source_id]
            )
            assert pending.result(timeout=3) == []
    assert len(LeaseRepository(leased.sessions).claim_due(source_ids=[leased.source_id])) == 1


def test_not_due_or_paused_sources_are_not_claimed(leased: LeaseEnvironment) -> None:
    with leased.sessions.begin() as session:
        session.execute(text("UPDATE sources SET next_crawl_at=now()+interval '1 day'"))
    assert LeaseRepository(leased.sessions).claim_due(source_ids=[leased.source_id]) == []
    with leased.sessions.begin() as session:
        session.execute(
            text("UPDATE sources SET next_crawl_at=now()-interval '1 day', status='paused'")
        )
    assert LeaseRepository(leased.sessions).claim_due(source_ids=[leased.source_id]) == []


def test_completion_failure_rolls_back_run_and_source_together(leased: LeaseEnvironment) -> None:
    current = claim(leased)
    repository = CrawlRepository(leased.sessions, claim=current)
    context = repository.start(leased.source_id)
    before = leased.snapshot()
    with leased.sessions() as session:
        engine = session.get_bind()

    def reject(
        connection: Any, cursor: Any, statement: str, parameters: Any, context: Any, many: bool
    ) -> None:
        if statement.startswith("UPDATE sources"):
            raise RuntimeError("Injected completion transaction failure")

    event.listen(engine, "before_cursor_execute", reject)
    try:
        with pytest.raises(RuntimeError, match="Injected completion"):
            repository.finish(context, RunCounts(), "succeeded", None, 0)
    finally:
        event.remove(engine, "before_cursor_execute", reject)
    assert leased.snapshot() == before
