"""HTTP input/output declarations only; SQL and pagination live in the repository."""

from typing import Annotated, cast
from uuid import UUID

from fastapi import APIRouter, Depends, Query, Request
from sqlalchemy.orm import Session, sessionmaker

from app.api.repository import ReadRepository
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

router = APIRouter()


def get_repository(request: Request) -> ReadRepository:
    sessions = cast(sessionmaker[Session], request.app.state.sessions)
    return ReadRepository(sessions)


Repository = Annotated[ReadRepository, Depends(get_repository)]


@router.get("/sources", response_model=Page[SourceRead], tags=["sources"])
def sources(query: Annotated[Pagination, Query()], repository: Repository) -> Page[SourceRead]:
    return repository.sources(query)


@router.get("/documents", response_model=Page[DocumentRead], tags=["documents"])
def documents(
    query: Annotated[DocumentQuery, Query()], repository: Repository
) -> Page[DocumentRead]:
    return repository.documents(query)


@router.get(
    "/documents/{document_id}/versions", response_model=Page[VersionRead], tags=["documents"]
)
def versions(
    document_id: UUID,
    query: Annotated[Pagination, Query()],
    repository: Repository,
) -> Page[VersionRead]:
    return repository.versions(document_id, query)


@router.get("/changes", response_model=Page[ChangeRead], tags=["changes"])
def changes(query: Annotated[ChangeQuery, Query()], repository: Repository) -> Page[ChangeRead]:
    return repository.changes(query)
