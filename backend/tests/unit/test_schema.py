from sqlalchemy.schema import CreateTable
from sqlalchemy.dialects import postgresql

from app.models import Base


def test_all_tables_compile_for_postgres() -> None:
    assert set(Base.metadata.tables) == {
        "sources",
        "documents",
        "document_versions",
        "changes",
        "crawl_runs",
    }
    for table in Base.metadata.sorted_tables:
        assert str(CreateTable(table).compile(dialect=postgresql.dialect()))  # type: ignore[no-untyped-call]


def test_change_versions_are_scoped_to_the_document() -> None:
    constraints = Base.metadata.tables["changes"].foreign_key_constraints
    pairs = {tuple(constraint.column_keys) for constraint in constraints}
    assert ("document_id", "from_version_id") in pairs
    assert ("document_id", "to_version_id") in pairs
