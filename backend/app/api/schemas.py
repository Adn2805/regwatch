"""Explicit read contracts: no response schema contains raw snapshot bytes."""

from datetime import datetime
from typing import Any, Generic, Literal, Self, TypeVar
from uuid import UUID

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, model_validator


class Pagination(BaseModel):
    model_config = ConfigDict(extra="forbid")

    limit: int = Field(default=50, ge=1, le=100)
    offset: int = Field(default=0, ge=0, le=100_000)


class DocumentQuery(Pagination):
    source_id: UUID | None = None


class ChangeQuery(Pagination):
    document_id: UUID | None = None
    date_from: AwareDatetime | None = None
    date_to: AwareDatetime | None = None

    @model_validator(mode="after")
    def validate_range(self) -> Self:
        if (
            self.date_from is not None
            and self.date_to is not None
            and self.date_from > self.date_to
        ):
            raise ValueError("date_from must be before or equal to date_to")
        return self


class SourceRead(BaseModel):
    id: UUID
    name: str
    base_url: str
    adapter: str
    crawl_interval_s: int
    next_crawl_at: datetime
    last_success_at: datetime | None
    consecutive_failures: int
    status: Literal["active", "paused", "degraded"]


class DocumentRead(BaseModel):
    id: UUID
    source_id: UUID
    canonical_url: str
    title: str
    created_at: datetime


class VersionRead(BaseModel):
    id: UUID
    document_id: UUID
    snapshot_key: str
    content_hash: str
    fetched_at: datetime
    http_status: int
    parser_yield: int


class ChangeRead(BaseModel):
    id: UUID
    document_id: UUID
    from_version_id: UUID
    to_version_id: UUID
    diff: dict[str, Any]
    is_cosmetic: bool
    created_at: datetime


Item = TypeVar("Item", bound=BaseModel)


class Page(BaseModel, Generic[Item]):
    items: list[Item]
    total: int = Field(ge=0)
    limit: int
    offset: int
