"""Read-only query/projection logic, with one short-lived session per method."""

from typing import Any, TypeVar
from uuid import UUID

from pydantic import BaseModel
from sqlalchemy import Select, func, select
from sqlalchemy.orm import Session, sessionmaker

from app.api.schemas import (
    ChangeQuery,
    ChangeRead,
    DocumentQuery,
    DocumentRead,
    Page,
    Pagination,
    SourceRead,
    VersionRead,
)
from app.models.schema import Change, Document, DocumentVersion, Source

Item = TypeVar("Item", bound=BaseModel)


class EntityNotFound(Exception):
    """A valid identifier does not exist; contains no SQL or configuration details."""


class ReadRepository:
    def __init__(self, sessions: sessionmaker[Session]) -> None:
        self._sessions = sessions

    def _page(
        self,
        session: Session,
        statement: Select[Any],
        model: type[Item],
        query: Pagination,
    ) -> Page[Item]:
        total = (
            session.scalar(select(func.count()).select_from(statement.order_by(None).subquery()))
            or 0
        )
        rows = session.execute(statement.limit(query.limit).offset(query.offset)).mappings()
        return Page[Item](
            items=[model.model_validate(row) for row in rows],
            total=total,
            limit=query.limit,
            offset=query.offset,
        )

    def sources(self, query: Pagination) -> Page[SourceRead]:
        statement = select(
            Source.id,
            Source.name,
            Source.base_url,
            Source.adapter,
            Source.crawl_interval_s,
            Source.next_crawl_at,
            Source.last_success_at,
            Source.consecutive_failures,
            Source.status,
        ).order_by(Source.name, Source.id)
        with self._sessions() as session:
            return self._page(session, statement, SourceRead, query)

    def documents(self, query: DocumentQuery) -> Page[DocumentRead]:
        statement = select(
            Document.id,
            Document.source_id,
            Document.canonical_url,
            Document.title,
            Document.created_at,
        ).order_by(Document.created_at.desc(), Document.id.desc())
        with self._sessions() as session:
            if query.source_id is not None:
                if session.get(Source, query.source_id) is None:
                    raise EntityNotFound("Source not found")
                statement = statement.where(Document.source_id == query.source_id)
            return self._page(session, statement, DocumentRead, query)

    def versions(self, document_id: UUID, query: Pagination) -> Page[VersionRead]:
        # Do not select the potentially large text, sections, or search-vector columns.
        statement = (
            select(
                DocumentVersion.id,
                DocumentVersion.document_id,
                DocumentVersion.snapshot_key,
                DocumentVersion.content_hash,
                DocumentVersion.fetched_at,
                DocumentVersion.http_status,
                DocumentVersion.parser_yield,
            )
            .where(DocumentVersion.document_id == document_id)
            .order_by(
                DocumentVersion.fetched_at.desc(),
                DocumentVersion.id.desc(),
            )
        )
        with self._sessions() as session:
            if session.get(Document, document_id) is None:
                raise EntityNotFound("Document not found")
            return self._page(session, statement, VersionRead, query)

    def changes(self, query: ChangeQuery) -> Page[ChangeRead]:
        statement = select(
            Change.id,
            Change.document_id,
            Change.from_version_id,
            Change.to_version_id,
            Change.diff,
            Change.is_cosmetic,
            Change.created_at,
        ).order_by(Change.created_at.desc(), Change.id.desc())
        with self._sessions() as session:
            if query.document_id is not None:
                if session.get(Document, query.document_id) is None:
                    raise EntityNotFound("Document not found")
                statement = statement.where(Change.document_id == query.document_id)
            if query.date_from is not None:
                statement = statement.where(Change.created_at >= query.date_from)
            if query.date_to is not None:
                statement = statement.where(Change.created_at <= query.date_to)
            return self._page(session, statement, ChangeRead, query)
