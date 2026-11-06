"""Real PostgreSQL read API tests, isolated from user data in a unique schema."""

import os
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import datetime, timezone
from uuid import UUID, uuid4

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.schema import CreateSchema, DropSchema

from app.api.repository import ReadRepository
from app.api.schemas import Page, Pagination, SourceRead
from app.core.config import Settings
from app.main import create_app
from app.models import Base
from app.models.schema import Change, Document, DocumentVersion, Source

pytestmark = pytest.mark.skipif(
    "POSTGRES_DB" not in os.environ, reason="run in Compose for PostgreSQL API tests"
)

SOURCE_A, SOURCE_B, SOURCE_EMPTY = UUID(int=1), UUID(int=2), UUID(int=3)
DOC_A, DOC_EMPTY, DOC_B = UUID(int=11), UUID(int=12), UUID(int=13)
FIRST = datetime(2026, 1, 2, tzinfo=timezone.utc)
SECOND = datetime(2026, 1, 3, tzinfo=timezone.utc)
BODY_MARKER = "PRIVATE_SNAPSHOT_BODY_SENTINEL"


@dataclass
class ApiEnvironment:
    client: TestClient
    sessions: sessionmaker[Session]

    def seed(self) -> None:
        with self.sessions.begin() as session:
            session.add_all(
                [
                    Source(
                        id=identity,
                        name=name,
                        base_url="https://example.invalid/feed",
                        adapter="rbi",
                        etag="PRIVATE_SOURCE_ETAG",
                        next_crawl_at=FIRST,
                    )
                    for identity, name in [
                        (SOURCE_A, "Alpha"),
                        (SOURCE_B, "Beta"),
                        (SOURCE_EMPTY, "Empty"),
                    ]
                ]
            )
            session.flush()
            session.add_all(
                [
                    Document(
                        id=DOC_A,
                        source_id=SOURCE_A,
                        canonical_url="https://example.invalid/a",
                        title="A",
                        created_at=FIRST,
                    ),
                    Document(
                        id=DOC_EMPTY,
                        source_id=SOURCE_A,
                        canonical_url="https://example.invalid/empty",
                        title="Empty",
                        created_at=FIRST,
                    ),
                    Document(
                        id=DOC_B,
                        source_id=SOURCE_B,
                        canonical_url="https://example.invalid/b",
                        title="B",
                        created_at=SECOND,
                    ),
                ]
            )
            session.flush()
            for identity, document_id, digest in [
                (101, DOC_A, "a"),
                (102, DOC_A, "b"),
                (103, DOC_B, "c"),
                (104, DOC_B, "d"),
                (105, DOC_A, "e"),
            ]:
                session.add(
                    DocumentVersion(
                        id=UUID(int=identity),
                        document_id=document_id,
                        snapshot_key=f"sha256/{digest * 64}",
                        content_hash=digest * 64,
                        normalized_text=BODY_MARKER,
                        sections=[{"text": BODY_MARKER}],
                        http_status=200,
                        parser_yield=1,
                        fetched_at=FIRST,
                    )
                )
            session.flush()
            session.add_all(
                [
                    Change(
                        id=UUID(int=201),
                        document_id=DOC_A,
                        from_version_id=UUID(int=101),
                        to_version_id=UUID(int=102),
                        diff={
                            "kind": "raw_content_changed",
                            "from_hash": "a" * 64,
                            "to_hash": "b" * 64,
                        },
                        created_at=FIRST,
                    ),
                    Change(
                        id=UUID(int=202),
                        document_id=DOC_B,
                        from_version_id=UUID(int=103),
                        to_version_id=UUID(int=104),
                        diff={
                            "kind": "raw_content_changed",
                            "from_hash": "c" * 64,
                            "to_hash": "d" * 64,
                        },
                        created_at=SECOND,
                    ),
                    Change(
                        id=UUID(int=203),
                        document_id=DOC_A,
                        from_version_id=UUID(int=102),
                        to_version_id=UUID(int=105),
                        diff={
                            "kind": "raw_content_changed",
                            "from_hash": "b" * 64,
                            "to_hash": "e" * 64,
                        },
                        created_at=SECOND,
                    ),
                ]
            )


@pytest.fixture
def env() -> Iterator[ApiEnvironment]:
    engine = create_engine(Settings().database_url(), connect_args={"connect_timeout": 5})
    schema = f"api_test_{uuid4().hex}"
    with engine.begin() as connection:
        connection.execute(CreateSchema(schema))
    translated = engine.execution_options(schema_translate_map={None: schema})
    try:
        Base.metadata.create_all(translated)
        sessions = sessionmaker(translated)
        with TestClient(create_app(sessions)) as client:
            yield ApiEnvironment(client, sessions)
    finally:
        # Drop only the unique test schema; public tables and user data are untouched.
        with engine.begin() as connection:
            connection.execute(DropSchema(schema, cascade=True))
        engine.dispose()


@pytest.fixture
def seeded(env: ApiEnvironment) -> ApiEnvironment:
    env.seed()
    return env


@pytest.mark.parametrize("path", ["/sources", "/documents", "/changes"])
def test_empty_collections(env: ApiEnvironment, path: str) -> None:
    response = env.client.get(path)
    assert response.status_code == 200
    assert response.json() == {"items": [], "total": 0, "limit": 50, "offset": 0}


def test_known_document_without_versions_is_empty(seeded: ApiEnvironment) -> None:
    response = seeded.client.get(f"/documents/{DOC_EMPTY}/versions")
    assert response.status_code == 200 and response.json()["total"] == 0
    assert response.json()["items"] == []


@pytest.mark.parametrize(
    ("path", "expected"),
    [
        ("/sources", [SOURCE_A, SOURCE_B, SOURCE_EMPTY]),
        ("/documents", [DOC_B, DOC_EMPTY, DOC_A]),
        (f"/documents/{DOC_A}/versions", [UUID(int=105), UUID(int=102), UUID(int=101)]),
        ("/changes", [UUID(int=203), UUID(int=202), UUID(int=201)]),
    ],
)
def test_pagination_boundaries_and_stable_ties(
    seeded: ApiEnvironment,
    path: str,
    expected: list[UUID],
) -> None:
    for offset, identity in enumerate(expected):
        response = seeded.client.get(path, params={"limit": 1, "offset": offset})
        assert response.status_code == 200
        page = response.json()
        assert page["total"] == len(expected) and page["limit"] == 1 and page["offset"] == offset
        assert [item["id"] for item in page["items"]] == [str(identity)]
    for offset in [len(expected), 100_000]:
        response = seeded.client.get(path, params={"limit": 100, "offset": offset})
        assert response.status_code == 200
        assert response.json()["items"] == [] and response.json()["total"] == len(expected)
    last = seeded.client.get(path, params={"limit": 2, "offset": 2}).json()
    assert [item["id"] for item in last["items"]] == [str(expected[-1])]
    assert seeded.client.get(path, params={"limit": 100}).json()["limit"] == 100


def test_documents_filter_source_and_filtered_total(seeded: ApiEnvironment) -> None:
    page = seeded.client.get("/documents", params={"source_id": str(SOURCE_A), "limit": 1}).json()
    assert page["total"] == 2
    assert [item["id"] for item in page["items"]] == [str(DOC_EMPTY)]
    empty = seeded.client.get("/documents", params={"source_id": str(SOURCE_EMPTY)})
    assert empty.status_code == 200 and empty.json()["items"] == []


@pytest.mark.parametrize(
    ("params", "expected"),
    [
        ({"document_id": str(DOC_A)}, [203, 201]),
        ({"date_from": FIRST.isoformat(), "date_to": FIRST.isoformat()}, [201]),
        ({"date_from": SECOND.isoformat(), "date_to": SECOND.isoformat()}, [203, 202]),
        ({"document_id": str(DOC_A), "date_from": SECOND.isoformat()}, [203]),
        ({"date_from": "2026-01-02T05:30:00+05:30", "date_to": FIRST.isoformat()}, [201]),
        ({"date_to": "2026-01-01T00:00:00Z"}, []),
        ({"document_id": str(DOC_EMPTY)}, []),
    ],
)
def test_change_filters_and_inclusive_date_bounds(
    seeded: ApiEnvironment,
    params: dict[str, str],
    expected: list[int],
) -> None:
    response = seeded.client.get("/changes", params=params)
    assert response.status_code == 200
    page = response.json()
    assert page["total"] == len(expected)
    assert [item["id"] for item in page["items"]] == [str(UUID(int=value)) for value in expected]


@pytest.mark.parametrize(
    "path",
    [
        f"/documents/{UUID(int=999)}/versions",
        f"/documents?source_id={UUID(int=999)}",
        f"/changes?document_id={UUID(int=999)}",
    ],
)
def test_unknown_valid_ids_return_404(env: ApiEnvironment, path: str) -> None:
    response = env.client.get(path)
    assert response.status_code == 404 and "not found" in response.json()["detail"]


@pytest.mark.parametrize(
    "params",
    [
        {"limit": "0"},
        {"limit": "101"},
        {"limit": "many"},
        {"offset": "-1"},
        {"offset": "100001"},
    ],
)
def test_bad_pagination_is_422_for_all_endpoints(
    seeded: ApiEnvironment, params: dict[str, str]
) -> None:
    for path in ["/sources", "/documents", f"/documents/{DOC_A}/versions", "/changes"]:
        assert seeded.client.get(path, params=params).status_code == 422


@pytest.mark.parametrize(
    "path",
    [
        "/documents/not-a-uuid/versions",
        "/documents?source_id=bad",
        "/changes?document_id=bad",
        "/changes?date_from=not-a-date",
        "/changes?date_to=2026-01-03T00:00:00",
        "/changes?date_from=2026-01-04T00:00:00Z&date_to=2026-01-02T00:00:00Z",
        "/sources?unknown_filter=value",
    ],
)
def test_other_bad_parameters_are_422(env: ApiEnvironment, path: str) -> None:
    assert env.client.get(path).status_code == 422


def test_version_responses_exclude_snapshot_content(seeded: ApiEnvironment) -> None:
    response = seeded.client.get(f"/documents/{DOC_A}/versions")
    assert response.status_code == 200 and BODY_MARKER not in response.text
    item = response.json()["items"][0]
    assert set(item) == {
        "id",
        "document_id",
        "snapshot_key",
        "content_hash",
        "fetched_at",
        "http_status",
        "parser_yield",
    }
    assert item["snapshot_key"] == f"sha256/{item['content_hash']}"
    sources = seeded.client.get("/sources")
    assert "PRIVATE_SOURCE_ETAG" not in sources.text


def test_docs_and_openapi_load_with_four_read_paths(env: ApiEnvironment) -> None:
    docs = env.client.get("/docs")
    assert docs.status_code == 200 and "text/html" in docs.headers["content-type"]
    assert "SwaggerUIBundle" in docs.text
    response = env.client.get("/openapi.json")
    assert response.status_code == 200
    specification = response.json()
    assert set(specification["paths"]) == {
        "/sources",
        "/documents",
        "/documents/{document_id}/versions",
        "/changes",
    }
    assert all(set(path) == {"get"} for path in specification["paths"].values())
    assert set(specification["components"]["schemas"]["VersionRead"]["properties"]) == {
        "id",
        "document_id",
        "snapshot_key",
        "content_hash",
        "fetched_at",
        "http_status",
        "parser_yield",
    }


def test_database_failure_is_503_without_internal_details(
    env: ApiEnvironment, monkeypatch: pytest.MonkeyPatch
) -> None:
    def unavailable(self: ReadRepository, query: Pagination) -> Page[SourceRead]:
        raise OperationalError("secret SQL", {}, Exception("private credential"))

    monkeypatch.setattr(ReadRepository, "sources", unavailable)
    response = env.client.get("/sources")
    assert response.status_code == 503
    assert response.json() == {"detail": "Database unavailable"}
    assert "private credential" not in response.text and "secret SQL" not in response.text


def test_api_does_not_accept_writes(env: ApiEnvironment) -> None:
    assert env.client.post("/documents", json={"title": "injected"}).status_code == 405
    assert env.client.get("/documents").json()["total"] == 0
