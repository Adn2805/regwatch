"""Upload first, then atomically record a document version. No fetching or parsing here."""

from typing import Any
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session, sessionmaker

from app.core.s3 import SnapshotStore
from app.crawl.fetcher import FetchResult
from app.models.schema import DocumentVersion


def record_version(
    sessions: sessionmaker[Session],
    store: SnapshotStore,
    document_id: UUID,
    response: FetchResult,
    *,
    normalized_text: str,
    sections: list[dict[str, Any]],
) -> UUID:
    """Caller supplies extracted text/sections; this step does not implement a parser."""
    snapshot = store.put(
        response.content,
        content_type=response.headers.get("content-type", "application/octet-stream"),
    )
    # Storage failures cannot leave even an uncommitted version in this session.
    # If DB commit fails, a safe, unreferenced object may remain in MinIO.
    with sessions.begin() as session:
        statement = (
            insert(DocumentVersion)
            .values(
                document_id=document_id,
                snapshot_key=snapshot.key,
                content_hash=snapshot.content_hash,
                normalized_text=normalized_text,
                sections=sections,
                http_status=response.http_status,
                parser_yield=len(sections),
            )
            .on_conflict_do_nothing(constraint="uq_version_content")
            .returning(DocumentVersion.id)
        )
        version_id: UUID | None = session.scalar(statement)
        if version_id is None:
            version_id = session.scalar(
                select(DocumentVersion.id).where(
                    DocumentVersion.document_id == document_id,
                    DocumentVersion.content_hash == snapshot.content_hash,
                )
            )
        if version_id is None:
            raise RuntimeError("Document version could not be recorded")
        return version_id
