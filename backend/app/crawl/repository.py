"""Short-lived database transactions for crawl audit and document identity."""

from dataclasses import dataclass
from datetime import datetime, timezone
from uuid import UUID

from sqlalchemy import update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session, sessionmaker

from app.crawl.adapters.base import Publication
from app.models.schema import CrawlRun, Document, Source


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
