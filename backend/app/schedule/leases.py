"""Postgres is the authority for ownership, expiry and recovery timestamps."""

import math
from dataclasses import dataclass, field
from datetime import timedelta
from collections.abc import Sequence
from uuid import UUID, uuid4

from sqlalchemy import func, or_, select, update
from sqlalchemy.orm import Session, sessionmaker

from app.crawl.errors import FetchError
from app.models.schema import CrawlRun, Source

RECOVERED = "Lease expired; abandoned crawl recovered"


class LeaseLost(FetchError):
    pass


@dataclass(frozen=True)
class LeaseClaim:
    source_id: UUID
    token: UUID = field(repr=False)
    run_id: UUID


def fence(session: Session, claim: LeaseClaim) -> Source:
    source = session.scalar(
        select(Source)
        .where(
            Source.id == claim.source_id,
            Source.lease_token == claim.token,
            Source.lease_expires_at > func.now(),
        )
        .with_for_update()
    )
    if source is None:
        raise LeaseLost("Source lease expired or ownership changed; write rejected")
    return source


class LeaseRepository:
    def __init__(self, sessions: sessionmaker[Session], *, lease_s: float = 60) -> None:
        if not math.isfinite(lease_s) or not 0 < lease_s <= 3600:
            raise ValueError("Lease duration must be finite and between zero and 3600 seconds")
        self._sessions, self.lease_s = sessions, lease_s

    def _recover(self, session: Session, source: Source) -> None:
        if source.lease_token is not None:
            session.execute(
                update(CrawlRun)
                .where(
                    CrawlRun.source_id == source.id,
                    CrawlRun.lease_token == source.lease_token,
                    CrawlRun.status == "running",
                )
                .values(
                    status="failed",
                    error=RECOVERED,
                    finished_at=func.now(),
                    duration_ms=func.greatest(
                        0, func.extract("epoch", func.now() - CrawlRun.started_at) * 1000
                    ),
                )
            )
        source.lease_token = None
        source.lease_expires_at = None
        source.lease_heartbeat_at = None

    def claim_due(
        self, *, limit: int = 100, source_ids: Sequence[UUID] | None = None, force: bool = False
    ) -> list[LeaseClaim]:
        if not 1 <= limit <= 1000:
            raise ValueError("Claim limit must be between 1 and 1000")
        query = select(Source).where(
            Source.status == "active",
            or_(Source.lease_expires_at.is_(None), Source.lease_expires_at <= func.now()),
        )
        if not force:
            query = query.where(Source.next_crawl_at <= func.now())
        if source_ids is not None:
            query = query.where(Source.id.in_(source_ids))
        claims = []
        with self._sessions.begin() as session:
            sources = session.scalars(
                query.order_by(Source.next_crawl_at, Source.id)
                .limit(limit)
                .with_for_update(skip_locked=True)
            ).all()
            for source in sources:
                self._recover(session, source)
                token = uuid4()
                source.lease_token = token
                source.lease_heartbeat_at = func.now()
                source.lease_expires_at = func.now() + timedelta(seconds=self.lease_s)
                run = CrawlRun(source_id=source.id, lease_token=token, started_at=func.now())
                session.add(run)
                session.flush()
                claims.append(LeaseClaim(source.id, token, run.id))
        return claims

    def heartbeat(self, claim: LeaseClaim) -> None:
        with self._sessions.begin() as session:
            result = session.execute(
                update(Source)
                .where(
                    Source.id == claim.source_id,
                    Source.lease_token == claim.token,
                    Source.lease_expires_at > func.now(),
                )
                .values(
                    lease_heartbeat_at=func.now(),
                    lease_expires_at=func.now() + timedelta(seconds=self.lease_s),
                )
                .returning(Source.id)
            )
            if result.scalar_one_or_none() is None:
                raise LeaseLost("Heartbeat rejected: lease expired or ownership changed")

    def recover_expired(self, *, limit: int = 100) -> int:
        with self._sessions.begin() as session:
            sources = session.scalars(
                select(Source)
                .where(
                    Source.lease_token.is_not(None),
                    Source.lease_expires_at <= func.now(),
                )
                .order_by(Source.id)
                .limit(limit)
                .with_for_update(skip_locked=True)
            ).all()
            for source in sources:
                self._recover(session, source)
            return len(sources)

    def completed_by_owner(self, claim: LeaseClaim) -> bool:
        with self._sessions() as session:
            return (
                session.scalar(
                    select(CrawlRun.id).where(
                        CrawlRun.id == claim.run_id,
                        CrawlRun.lease_token == claim.token,
                        CrawlRun.status != "running",
                        or_(CrawlRun.error.is_(None), CrawlRun.error != RECOVERED),
                    )
                )
                is not None
            )
