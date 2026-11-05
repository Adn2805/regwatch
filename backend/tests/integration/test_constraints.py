"""Run against migrated Postgres. Each test rolls back all inserted data."""

import os
from collections.abc import Iterator
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.core.config import Settings
from app.models.schema import Change, Document, DocumentVersion, Source

pytestmark = pytest.mark.skipif(
    "POSTGRES_DB" not in os.environ, reason="run in Compose for database integration tests"
)


@pytest.fixture
def session() -> Iterator[Session]:
    engine = create_engine(Settings().database_url(), connect_args={"connect_timeout": 5})
    with engine.connect() as connection:
        transaction = connection.begin()
        with Session(bind=connection, join_transaction_mode="create_savepoint") as db:
            yield db
        transaction.rollback()
    engine.dispose()


def add_document(session: Session) -> Document:
    source = Source(name=str(uuid4()), base_url="https://example.invalid", adapter="fixture")
    session.add(source)
    session.flush()
    document = Document(source_id=source.id, canonical_url="https://example.invalid/a", title="A")
    session.add(document)
    session.flush()
    return document


def add_version(session: Session, document: Document, digest: str) -> DocumentVersion:
    version = DocumentVersion(
        document_id=document.id,
        snapshot_key=f"sha256/{digest}",
        content_hash=digest,
        normalized_text="Regulatory obligation",
        sections=[],
        http_status=200,
        parser_yield=0,
    )
    session.add(version)
    session.flush()
    return version


def test_duplicate_content_is_rejected(session: Session) -> None:
    document = add_document(session)
    add_version(session, document, "a" * 64)
    with pytest.raises(IntegrityError, match="uq_version_content"):
        add_version(session, document, "a" * 64)


def test_cross_document_change_is_rejected(session: Session) -> None:
    first = add_document(session)
    second = add_document(session)
    old = add_version(session, first, "a" * 64)
    new = add_version(session, second, "b" * 64)
    session.add(Change(document_id=first.id, from_version_id=old.id, to_version_id=new.id, diff={}))
    with pytest.raises(IntegrityError, match="fk_change_to"):
        session.flush()


def test_invalid_hash_is_rejected(session: Session) -> None:
    document = add_document(session)
    with pytest.raises(IntegrityError, match="ck_version_hash"):
        add_version(session, document, "invalid")


def test_search_vector_is_generated(session: Session) -> None:
    version = add_version(session, add_document(session), "a" * 64)
    assert (
        session.scalar(
            text(
                "SELECT search_vector @@ plainto_tsquery('english', 'obligation') "
                "FROM document_versions WHERE id = :id"
            ),
            {"id": version.id},
        )
        is True
    )


def test_vector_extension_exists(session: Session) -> None:
    assert (
        session.scalar(text("SELECT extname FROM pg_extension WHERE extname = 'vector'"))
        == "vector"
    )
