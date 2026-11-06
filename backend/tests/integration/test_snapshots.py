from __future__ import annotations

import os
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import TYPE_CHECKING
from uuid import UUID, uuid4

import pytest
from pydantic import AnyHttpUrl
from sqlalchemy import create_engine, delete, func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, sessionmaker

from app.core.config import Settings, StorageSettings
from app.core.s3 import SnapshotStorageError, SnapshotStore, make_s3_client
from app.crawl.fetcher import FetchResult
from app.crawl.snapshot import record_version
from app.diff.hashing import content_hash
from app.models.schema import Document, DocumentVersion, Source

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client

pytestmark = pytest.mark.skipif(
    "MINIO_ROOT_PASSWORD" not in os.environ or "POSTGRES_DB" not in os.environ,
    reason="run in Compose for MinIO/PostgreSQL integration tests",
)


@dataclass
class StorageFixture:
    client: S3Client
    bucket: str
    store: SnapshotStore


@pytest.fixture
def storage() -> Iterator[StorageFixture]:
    client = make_s3_client()
    bucket = f"regwatch-test-{uuid4().hex}"
    client.create_bucket(Bucket=bucket)
    try:
        yield StorageFixture(client, bucket, SnapshotStore(client, bucket))
    finally:
        # Cleanup is restricted to the unique bucket created by this fixture.
        for item in client.list_objects_v2(Bucket=bucket).get("Contents", []):
            client.delete_object(Bucket=bucket, Key=item["Key"])
        client.delete_bucket(Bucket=bucket)
        client.close()


@dataclass
class DatabaseFixture:
    sessions: sessionmaker[Session]
    document_id: UUID

    def counts(self) -> tuple[int, int, int]:
        with self.sessions() as session:
            return (
                session.scalar(select(func.count()).select_from(Source)) or 0,
                session.scalar(select(func.count()).select_from(Document)) or 0,
                session.scalar(select(func.count()).select_from(DocumentVersion)) or 0,
            )


@pytest.fixture
def database() -> Iterator[DatabaseFixture]:
    engine = create_engine(Settings().database_url(), connect_args={"connect_timeout": 5})
    sessions = sessionmaker(engine)
    with sessions.begin() as session:
        source = Source(
            name=f"snapshot-test-{uuid4().hex}",
            base_url="https://example.invalid",
            adapter="fixture",
        )
        session.add(source)
        session.flush()
        document = Document(
            source_id=source.id, canonical_url="https://example.invalid/notice", title="Fixture"
        )
        session.add(document)
        session.flush()
        source_id, document_id = source.id, document.id
    try:
        yield DatabaseFixture(sessions, document_id)
    finally:
        with sessions.begin() as session:
            session.execute(
                delete(DocumentVersion).where(DocumentVersion.document_id == document_id)
            )
            session.execute(delete(Document).where(Document.id == document_id))
            session.execute(delete(Source).where(Source.id == source_id))
        engine.dispose()


def response(body: bytes) -> FetchResult:
    return FetchResult(
        "https://example.invalid/notice", 200, {"content-type": "application/pdf"}, body
    )


def test_identical_bytes_create_one_object_and_preserve_metadata(storage: StorageFixture) -> None:
    body = b"%PDF-fixture\x00\xff"
    first = storage.store.put(body, content_type="application/pdf")
    before = storage.client.head_object(Bucket=storage.bucket, Key=first.key)
    second = storage.store.put(body, content_type="text/plain")
    after = storage.client.head_object(Bucket=storage.bucket, Key=first.key)
    assert first.key == second.key == f"sha256/{content_hash(body)}"
    assert first.created and not second.created
    assert before["LastModified"] == after["LastModified"]
    assert after["ContentType"] == "application/pdf"
    assert after["Metadata"] == {
        "content-type": "application/pdf",
        "byte-size": str(len(body)),
        "sha256": content_hash(body),
    }
    assert storage.client.list_objects_v2(Bucket=storage.bucket)["KeyCount"] == 1
    result = storage.client.get_object(Bucket=storage.bucket, Key=first.key)
    try:
        assert result["Body"].read() == body
    finally:
        result["Body"].close()


def test_different_bytes_create_distinct_keys(storage: StorageFixture) -> None:
    first = storage.store.put(b"old notice")
    second = storage.store.put(b"new notice")
    assert first.key != second.key
    assert first.created and second.created
    assert storage.client.list_objects_v2(Bucket=storage.bucket)["KeyCount"] == 2


def test_concurrent_identical_writes_have_exactly_one_creator(storage: StorageFixture) -> None:
    with ThreadPoolExecutor(max_workers=8) as workers:
        snapshots = list(workers.map(storage.store.put, [b"same concurrent bytes"] * 8))
    assert sum(snapshot.created for snapshot in snapshots) == 1
    assert len({snapshot.key for snapshot in snapshots}) == 1
    assert storage.client.list_objects_v2(Bucket=storage.bucket)["KeyCount"] == 1


def test_inconsistent_existing_object_is_never_overwritten(storage: StorageFixture) -> None:
    key = f"sha256/{content_hash(b'expected')}"
    storage.client.put_object(
        Bucket=storage.bucket, Key=key, Body=b"wrong", ContentType="text/plain"
    )
    with pytest.raises(SnapshotStorageError, match="metadata is inconsistent"):
        storage.store.put(b"expected")
    result = storage.client.get_object(Bucket=storage.bucket, Key=key)
    try:
        assert result["Body"].read() == b"wrong"
    finally:
        result["Body"].close()


def test_repeated_recording_creates_one_version_and_one_object(
    storage: StorageFixture,
    database: DatabaseFixture,
) -> None:
    first = record_version(
        database.sessions,
        storage.store,
        database.document_id,
        response(b"document"),
        normalized_text="Fixture",
        sections=[{"id": "1", "text": "Fixture"}],
    )
    second = record_version(
        database.sessions,
        storage.store,
        database.document_id,
        response(b"document"),
        normalized_text="Fixture",
        sections=[{"id": "1", "text": "Fixture"}],
    )
    assert first.version_id == second.version_id
    assert first.created and not second.created
    with database.sessions() as session:
        versions = session.scalars(
            select(DocumentVersion).where(
                DocumentVersion.document_id == database.document_id,
            )
        ).all()
    assert len(versions) == 1
    assert versions[0].content_hash == content_hash(b"document")
    assert versions[0].snapshot_key == f"sha256/{content_hash(b'document')}"
    assert versions[0].parser_yield == 1
    assert storage.client.list_objects_v2(Bucket=storage.bucket)["KeyCount"] == 1


def test_storage_network_failure_leaves_database_unchanged(
    storage: StorageFixture,
    database: DatabaseFixture,
) -> None:
    settings = StorageSettings().model_copy(
        update={
            "s3_endpoint_url": AnyHttpUrl("http://127.0.0.1:1"),
            "s3_max_attempts": 1,
            "s3_connect_timeout_s": 0.2,
            "s3_read_timeout_s": 0.2,
        }
    )
    client = make_s3_client(settings)
    before = database.counts()
    try:
        with pytest.raises(SnapshotStorageError, match="Snapshot upload failed"):
            record_version(
                database.sessions,
                SnapshotStore(client, storage.bucket),
                database.document_id,
                response(b"document"),
                normalized_text="Fixture",
                sections=[],
            )
    finally:
        client.close()
    assert database.counts() == before
    assert storage.client.list_objects_v2(Bucket=storage.bucket)["KeyCount"] == 0


def test_database_failure_rolls_back_and_object_is_safe_to_retry(
    storage: StorageFixture,
    database: DatabaseFixture,
) -> None:
    body = b"DB failure fixture"
    before = database.counts()
    with pytest.raises(IntegrityError):
        record_version(
            database.sessions,
            storage.store,
            uuid4(),
            response(body),
            normalized_text="Fixture",
            sections=[],
        )
    assert database.counts() == before
    assert storage.client.list_objects_v2(Bucket=storage.bucket)["KeyCount"] == 1
    version_id = record_version(
        database.sessions,
        storage.store,
        database.document_id,
        response(body),
        normalized_text="Fixture",
        sections=[],
    )
    with database.sessions() as session:
        assert session.get(DocumentVersion, version_id.version_id) is not None
    assert storage.client.list_objects_v2(Bucket=storage.bucket)["KeyCount"] == 1
