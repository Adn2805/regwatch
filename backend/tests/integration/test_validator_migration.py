import os
from uuid import uuid4

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from app.core.config import Settings

pytestmark = pytest.mark.skipif("POSTGRES_DB" not in os.environ, reason="requires real PostgreSQL")
TABLES = ("sources", "documents", "document_versions", "changes", "crawl_runs")


def original_rows(engine: Engine) -> dict[str, list[dict[str, object]]]:
    with engine.connect() as connection:
        result = {
            table: list(
                connection.scalars(text(f"SELECT row_to_json(t) FROM {table} t ORDER BY id"))
            )
            for table in TABLES
        }
    for document in result["documents"]:
        for field in ("etag", "last_modified", "validator_url"):
            document.pop(field, None)
    return result


def test_upgrade_downgrade_upgrade_preserves_existing_rows(monkeypatch: pytest.MonkeyPatch) -> None:
    url = Settings().database_url()
    name = "migration_test_" + uuid4().hex
    admin = create_engine(
        url.set(database="postgres"),
        isolation_level="AUTOCOMMIT",
        connect_args={
            "connect_timeout": 5,
            "options": "-c statement_timeout=30000 -c lock_timeout=5000",
        },
    )
    engine = create_engine(
        url.set(database=name),
        connect_args={
            "connect_timeout": 5,
            "options": "-c statement_timeout=30000 -c lock_timeout=5000",
        },
    )
    try:
        with admin.connect() as connection:
            connection.execute(text(f'CREATE DATABASE "{name}"'))
        monkeypatch.setenv("POSTGRES_DB", name)
        config = Config("alembic.ini")
        command.upgrade(config, "0002_crawl_run_counts")
        source, document, version = uuid4(), uuid4(), uuid4()
        with engine.begin() as connection:
            connection.execute(
                text(
                    "INSERT INTO sources(id,name,base_url,adapter) VALUES (:id,'migration fixture','https://fixture.invalid','fixture')"
                ),
                {"id": source},
            )
            connection.execute(
                text(
                    "INSERT INTO documents(id,source_id,canonical_url,title) VALUES (:id,:source,'https://fixture.invalid/a','Keep this title')"
                ),
                {"id": document, "source": source},
            )
            connection.execute(
                text(
                    "INSERT INTO document_versions(id,document_id,snapshot_key,content_hash,normalized_text,http_status,parser_yield) VALUES (:id,:document,:key,:hash,'Keep original text',200,0)"
                ),
                {
                    "id": version,
                    "document": document,
                    "key": "sha256/" + "a" * 64,
                    "hash": "a" * 64,
                },
            )
            connection.execute(
                text(
                    "INSERT INTO crawl_runs(id,source_id,status) VALUES (:id,:source,'succeeded')"
                ),
                {"id": uuid4(), "source": source},
            )
        before = original_rows(engine)
        command.upgrade(config, "0003_document_validators")
        assert original_rows(engine) == before
        with engine.connect() as connection:
            assert tuple(
                connection.execute(
                    text("SELECT etag,last_modified,validator_url FROM documents")
                ).one()
            ) == (None, None, None)
        command.downgrade(config, "0002_crawl_run_counts")
        assert original_rows(engine) == before
        command.upgrade(config, "0003_document_validators")
        assert original_rows(engine) == before
    finally:
        engine.dispose()
        with admin.connect() as connection:
            connection.execute(text(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)'))
        admin.dispose()
