from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "0001_core_schema"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("CREATE EXTENSION IF NOT EXISTS vector")
    op.create_table(
        "sources",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("name", sa.String(length=200), nullable=False),
        sa.Column("base_url", sa.Text(), nullable=False),
        sa.Column("adapter", sa.String(length=100), nullable=False),
        sa.Column("crawl_interval_s", sa.Integer(), server_default="3600", nullable=False),
        sa.Column(
            "next_crawl_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("etag", sa.Text(), nullable=True),
        sa.Column("last_modified", sa.Text(), nullable=True),
        sa.Column("last_success_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("consecutive_failures", sa.Integer(), server_default="0", nullable=False),
        sa.Column("status", sa.String(length=20), server_default="paused", nullable=False),
        sa.CheckConstraint("status IN ('active', 'paused', 'degraded')", name="ck_source_status"),
        sa.CheckConstraint("consecutive_failures >= 0", name="ck_source_failures"),
        sa.CheckConstraint("crawl_interval_s > 0", name="ck_source_interval"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("name"),
    )
    op.create_table(
        "crawl_runs",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("source_id", sa.Uuid(), nullable=False),
        sa.Column(
            "started_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("status", sa.String(length=20), server_default="running", nullable=False),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column("duration_ms", sa.Float(), nullable=True),
        sa.CheckConstraint("status IN ('running', 'succeeded', 'failed')", name="ck_run_status"),
        sa.CheckConstraint("duration_ms IS NULL OR duration_ms >= 0", name="ck_run_duration"),
        sa.CheckConstraint("finished_at IS NULL OR finished_at >= started_at", name="ck_run_times"),
        sa.ForeignKeyConstraint(
            ["source_id"],
            ["sources.id"],
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_table(
        "documents",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("source_id", sa.Uuid(), nullable=False),
        sa.Column("canonical_url", sa.Text(), nullable=False),
        sa.Column("title", sa.Text(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["source_id"],
            ["sources.id"],
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("source_id", "canonical_url", name="uq_document_url"),
    )
    op.create_table(
        "document_versions",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("document_id", sa.Uuid(), nullable=False),
        sa.Column("snapshot_key", sa.Text(), nullable=False),
        sa.Column("content_hash", sa.String(length=64), nullable=False),
        sa.Column("normalized_text", sa.Text(), nullable=False),
        sa.Column(
            "sections",
            postgresql.JSONB,
            server_default="[]",
            nullable=False,
        ),
        sa.Column(
            "fetched_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("http_status", sa.Integer(), nullable=False),
        sa.Column("parser_yield", sa.Integer(), nullable=False),
        sa.Column(
            "search_vector",
            postgresql.TSVECTOR(),
            sa.Computed("to_tsvector('english'::regconfig, normalized_text)", persisted=True),
            nullable=False,
        ),
        sa.CheckConstraint("content_hash ~ '^[0-9a-f]{64}$'", name="ck_version_hash"),
        sa.CheckConstraint("http_status BETWEEN 100 AND 599", name="ck_version_http_status"),
        sa.CheckConstraint("parser_yield >= 0", name="ck_version_parser_yield"),
        sa.ForeignKeyConstraint(
            ["document_id"],
            ["documents.id"],
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("document_id", "content_hash", name="uq_version_content"),
        sa.UniqueConstraint("document_id", "id", name="uq_version_document_id"),
    )
    op.create_table(
        "changes",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("document_id", sa.Uuid(), nullable=False),
        sa.Column("from_version_id", sa.Uuid(), nullable=False),
        sa.Column("to_version_id", sa.Uuid(), nullable=False),
        sa.Column("diff", postgresql.JSONB, nullable=False),
        sa.Column("is_cosmetic", sa.Boolean(), server_default="false", nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint("from_version_id <> to_version_id", name="ck_change_distinct_versions"),
        sa.ForeignKeyConstraint(
            ["document_id", "from_version_id"],
            ["document_versions.document_id", "document_versions.id"],
            name="fk_change_from",
        ),
        sa.ForeignKeyConstraint(
            ["document_id", "to_version_id"],
            ["document_versions.document_id", "document_versions.id"],
            name="fk_change_to",
        ),
        sa.ForeignKeyConstraint(
            ["document_id"],
            ["documents.id"],
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("from_version_id", "to_version_id", name="uq_change_pair"),
    )
    op.create_index("ix_sources_next_crawl_at", "sources", ["next_crawl_at"], unique=False)
    op.create_index(op.f("ix_crawl_runs_source_id"), "crawl_runs", ["source_id"], unique=False)
    op.create_index(op.f("ix_documents_source_id"), "documents", ["source_id"], unique=False)
    op.create_index(
        "ix_versions_document_fetched",
        "document_versions",
        ["document_id", "fetched_at"],
        unique=False,
    )
    op.create_index(
        "ix_versions_search_vector",
        "document_versions",
        ["search_vector"],
        unique=False,
        postgresql_using="gin",
    )
    op.create_index(op.f("ix_changes_document_id"), "changes", ["document_id"], unique=False)


def downgrade() -> None:
    op.drop_table("changes")
    op.drop_table("document_versions")
    op.drop_table("documents")
    op.drop_table("crawl_runs")
    op.drop_table("sources")
