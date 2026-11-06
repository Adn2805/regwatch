"""Phase 1 persistence contracts. Runtime services arrive in later steps."""

from datetime import datetime
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import (
    CheckConstraint,
    Computed,
    DateTime,
    Float,
    ForeignKey,
    ForeignKeyConstraint,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB, TSVECTOR
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class Source(Base):
    __tablename__ = "sources"
    __table_args__ = (
        CheckConstraint("crawl_interval_s > 0", name="ck_source_interval"),
        CheckConstraint("consecutive_failures >= 0", name="ck_source_failures"),
        CheckConstraint("status IN ('active', 'paused', 'degraded')", name="ck_source_status"),
        Index("ix_sources_next_crawl_at", "next_crawl_at"),
    )
    id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    name: Mapped[str] = mapped_column(String(200), unique=True)
    base_url: Mapped[str] = mapped_column(Text)
    adapter: Mapped[str] = mapped_column(String(100))
    crawl_interval_s: Mapped[int] = mapped_column(Integer, server_default="3600")
    next_crawl_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    etag: Mapped[str | None] = mapped_column(Text)
    last_modified: Mapped[str | None] = mapped_column(Text)
    last_success_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    consecutive_failures: Mapped[int] = mapped_column(Integer, server_default="0")
    status: Mapped[str] = mapped_column(String(20), server_default="paused")


class Document(Base):
    __tablename__ = "documents"
    __table_args__ = (UniqueConstraint("source_id", "canonical_url", name="uq_document_url"),)
    id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    source_id: Mapped[UUID] = mapped_column(ForeignKey("sources.id"), index=True)
    canonical_url: Mapped[str] = mapped_column(Text)
    title: Mapped[str] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class DocumentVersion(Base):
    __tablename__ = "document_versions"
    __table_args__ = (
        UniqueConstraint("document_id", "content_hash", name="uq_version_content"),
        UniqueConstraint("document_id", "id", name="uq_version_document_id"),
        CheckConstraint("content_hash ~ '^[0-9a-f]{64}$'", name="ck_version_hash"),
        CheckConstraint("http_status BETWEEN 100 AND 599", name="ck_version_http_status"),
        CheckConstraint("parser_yield >= 0", name="ck_version_parser_yield"),
        Index("ix_versions_search_vector", "search_vector", postgresql_using="gin"),
        Index("ix_versions_document_fetched", "document_id", "fetched_at"),
    )
    id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    document_id: Mapped[UUID] = mapped_column(ForeignKey("documents.id"))
    snapshot_key: Mapped[str] = mapped_column(Text)
    content_hash: Mapped[str] = mapped_column(String(64))
    normalized_text: Mapped[str] = mapped_column(Text)
    sections: Mapped[list[dict[str, Any]]] = mapped_column(JSONB, server_default="[]")
    fetched_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    http_status: Mapped[int] = mapped_column(Integer)
    parser_yield: Mapped[int] = mapped_column(Integer)
    search_vector: Mapped[Any] = mapped_column(
        TSVECTOR, Computed("to_tsvector('english'::regconfig, normalized_text)", persisted=True)
    )


class Change(Base):
    __tablename__ = "changes"
    __table_args__ = (
        ForeignKeyConstraint(
            ["document_id", "from_version_id"],
            ["document_versions.document_id", "document_versions.id"],
            name="fk_change_from",
        ),
        ForeignKeyConstraint(
            ["document_id", "to_version_id"],
            ["document_versions.document_id", "document_versions.id"],
            name="fk_change_to",
        ),
        UniqueConstraint("from_version_id", "to_version_id", name="uq_change_pair"),
        CheckConstraint("from_version_id <> to_version_id", name="ck_change_distinct_versions"),
    )
    id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    document_id: Mapped[UUID] = mapped_column(ForeignKey("documents.id"), index=True)
    from_version_id: Mapped[UUID]
    to_version_id: Mapped[UUID]
    diff: Mapped[dict[str, Any]] = mapped_column(JSONB)
    is_cosmetic: Mapped[bool] = mapped_column(server_default="false")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class CrawlRun(Base):
    __tablename__ = "crawl_runs"
    __table_args__ = (
        CheckConstraint(
            "status IN ('running', 'succeeded', 'failed', 'paused')", name="ck_run_status"
        ),
        CheckConstraint("finished_at IS NULL OR finished_at >= started_at", name="ck_run_times"),
        CheckConstraint("duration_ms IS NULL OR duration_ms >= 0", name="ck_run_duration"),
        CheckConstraint(
            "discovered >= 0 AND fetched >= 0 AND new_versions >= 0 AND unchanged >= 0 "
            "AND failed >= 0 AND paused >= 0",
            name="ck_run_counts",
        ),
    )
    id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    source_id: Mapped[UUID] = mapped_column(ForeignKey("sources.id"), index=True)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    status: Mapped[str] = mapped_column(String(20), server_default="running")
    error: Mapped[str | None] = mapped_column(Text)
    duration_ms: Mapped[float | None] = mapped_column(Float)

    discovered: Mapped[int] = mapped_column(Integer, server_default="0")
    fetched: Mapped[int] = mapped_column(Integer, server_default="0")
    new_versions: Mapped[int] = mapped_column(Integer, server_default="0")
    unchanged: Mapped[int] = mapped_column(Integer, server_default="0")
    failed: Mapped[int] = mapped_column(Integer, server_default="0")
    paused: Mapped[int] = mapped_column(Integer, server_default="0")
