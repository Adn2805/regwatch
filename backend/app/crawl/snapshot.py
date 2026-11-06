"""Upload first, then atomically record a version and optional naive change. No fetching or parsing here."""

from dataclasses import dataclass
from typing import Any
from uuid import UUID

from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session, sessionmaker

from app.core.s3 import SnapshotStore
from app.crawl.fetcher import FetchResult
from app.models.schema import Change, Document, DocumentVersion
from app.crawl.repository import apply_validators
from app.crawl.conditional import Validators


@dataclass(frozen=True)
class VersionRecord:
    version_id: UUID
    created: bool
    change_id: UUID | None = None


def record_version(
    sessions: sessionmaker[Session],
    store: SnapshotStore,
    document_id: UUID,
    response: FetchResult,
    *,
    normalized_text: str,
    sections: list[dict[str, Any]],
) -> VersionRecord:
    """Caller supplies extracted text/sections; this step does not implement a parser."""
    snapshot = store.put(
        response.content,
        content_type=response.headers.get("content-type", "application/octet-stream"),
    )
    # Storage failures cannot leave even an uncommitted version in this session.
    # If DB commit fails, a safe, unreferenced object may remain in MinIO.
    with sessions.begin() as session:
        # Serialize the previous-version decision for this document, not all crawls.
        document = session.scalar(
            select(Document).where(Document.id == document_id).with_for_update()
        )
        previous = session.scalar(
            select(DocumentVersion)
            .where(DocumentVersion.document_id == document_id)
            .order_by(DocumentVersion.fetched_at.desc(), DocumentVersion.id.desc())
            .limit(1)
        )
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
                fetched_at=func.clock_timestamp(),
            )
            .on_conflict_do_nothing(constraint="uq_version_content")
            .returning(DocumentVersion.id)
        )
        version_id: UUID | None = session.scalar(statement)
        created = version_id is not None
        if version_id is None:
            version_id = session.scalar(
                select(DocumentVersion.id).where(
                    DocumentVersion.document_id == document_id,
                    DocumentVersion.content_hash == snapshot.content_hash,
                )
            )
        if version_id is None:
            raise RuntimeError("Document version could not be recorded")
        change_id = None
        if created and previous is not None:
            change = Change(
                document_id=document_id,
                from_version_id=previous.id,
                to_version_id=version_id,
                diff={
                    "kind": "raw_content_changed",
                    "from_hash": previous.content_hash,
                    "to_hash": snapshot.content_hash,
                },
                is_cosmetic=False,
            )
            session.add(change)
            session.flush()
            change_id = change.id
        if document is None:
            raise RuntimeError("Document disappeared during version persistence")
        apply_validators(document, Validators.from_response(response.url, response.headers))
        return VersionRecord(version_id, created, change_id)
