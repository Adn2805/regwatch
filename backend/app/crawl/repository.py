"""Short-lived database transactions for crawl audit and document identity."""

from dataclasses import dataclass
from datetime import datetime, timezone
from uuid import UUID

from sqlalchemy import update, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session, sessionmaker

from app.crawl.adapters.base import Publication
from app.models.schema import CrawlRun, Document, Source, DocumentVersion
from app.crawl.conditional import DocumentRequestState, Validators
from app.crawl.fetcher import FetchResult
from app.crawl.errors import FetchError


@dataclass
class RunCounts:
    discovered: int = 0
    fetched: int = 0
    new_versions: int = 0
    unchanged: int = 0
    failed: int = 0
    paused: int = 0

    def values(self) -> dict[str, int]:
        return {
            "discovered": self.discovered,
            "fetched": self.fetched,
            "new_versions": self.new_versions,
            "unchanged": self.unchanged,
            "failed": self.failed,
            "paused": self.paused,
        }


@dataclass(frozen=True)
class RunContext:
    run_id: UUID
    source_id: UUID
    listing_url: str
    adapter: str
    source_status: str


def apply_validators(document: Document, validators: Validators) -> None:
    document.etag = validators.etag
    document.last_modified = validators.last_modified
    document.validator_url = validators.url


def request_state(session: Session, document: Document) -> DocumentRequestState:
    version_id = session.scalar(
        select(DocumentVersion.id)
        .where(DocumentVersion.document_id == document.id)
        .order_by(DocumentVersion.fetched_at.desc(), DocumentVersion.id.desc())
        .limit(1)
    )
    return DocumentRequestState(
        Validators(document.validator_url, document.etag, document.last_modified), version_id
    )


class CrawlRepository:
    def __init__(self, sessions: sessionmaker[Session]) -> None:
        self._sessions = sessions

    def start(self, source_id: UUID) -> RunContext:
        with self._sessions.begin() as session:
            source = session.get(Source, source_id)
            if source is None:
                raise ValueError("Unknown source")
            run = CrawlRun(source_id=source_id)
            session.add(run)
            session.flush()
            return RunContext(run.id, source_id, source.base_url, source.adapter, source.status)

    def upsert_document(self, source_id: UUID, publication: Publication) -> UUID:
        with self._sessions.begin() as session:
            statement = (
                insert(Document)
                .values(
                    source_id=source_id,
                    canonical_url=publication.canonical_url,
                    title=publication.title,
                )
                .on_conflict_do_update(
                    constraint="uq_document_url", set_={"title": publication.title}
                )
                .returning(Document.id)
            )
            document_id: UUID | None = session.scalar(statement)
            if document_id is None:
                raise RuntimeError("Document upsert did not return an identity")
            return document_id

    def document_request_state(self, document_id: UUID) -> DocumentRequestState:
        with self._sessions.begin() as session:
            # Read validators and the version baseline under one short document lock.
            document = session.scalar(
                select(Document).where(Document.id == document_id).with_for_update()
            )
            if document is None:
                raise ValueError("Unknown document")
            return request_state(session, document)

    def record_not_modified(
        self, document_id: UUID, response: FetchResult, expected: DocumentRequestState
    ) -> None:
        with self._sessions.begin() as session:
            document = session.scalar(
                select(Document).where(Document.id == document_id).with_for_update()
            )
            if (
                document is None
                or expected.version_id is None
                or not expected.validators.headers_for(response.url)
            ):
                raise FetchError("Cannot accept HTTP 304 without a matching stored version")
            if request_state(session, document) != expected:
                raise FetchError(
                    "Document changed during conditional request; validators not updated"
                )
            apply_validators(document, Validators.from_response(response.url, response.headers))

    def checkpoint(self, run_id: UUID, counts: RunCounts) -> None:
        with self._sessions.begin() as session:
            session.execute(update(CrawlRun).where(CrawlRun.id == run_id).values(**counts.values()))

    def finish(
        self,
        context: RunContext,
        counts: RunCounts,
        status: str,
        reason: str | None,
        duration_ms: float,
    ) -> None:
        finished = datetime.now(timezone.utc)
        with self._sessions.begin() as session:
            session.execute(
                update(CrawlRun)
                .where(CrawlRun.id == context.run_id)
                .values(
                    **counts.values(),
                    status=status,
                    error=reason,
                    finished_at=finished,
                    duration_ms=max(0.0, duration_ms),
                )
            )
            source = update(Source).where(Source.id == context.source_id)
            if status == "paused":
                session.execute(source.values(status="paused"))
            elif status == "succeeded":
                session.execute(source.values(last_success_at=finished, consecutive_failures=0))
            else:
                session.execute(source.values(consecutive_failures=Source.consecutive_failures + 1))
