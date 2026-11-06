"""Nullable source/run ownership metadata; no backfill or status-constraint changes."""

from alembic import op
import sqlalchemy as sa

revision = "0004_source_leases"
down_revision = "0003_document_validators"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("sources", sa.Column("lease_token", sa.Uuid(), nullable=True))
    op.add_column(
        "sources", sa.Column("lease_expires_at", sa.DateTime(timezone=True), nullable=True)
    )
    op.add_column(
        "sources", sa.Column("lease_heartbeat_at", sa.DateTime(timezone=True), nullable=True)
    )
    op.add_column("crawl_runs", sa.Column("lease_token", sa.Uuid(), nullable=True))
    op.create_index("ix_sources_lease_expires_at", "sources", ["lease_expires_at"])


def downgrade() -> None:
    op.drop_index("ix_sources_lease_expires_at", table_name="sources")
    op.drop_column("crawl_runs", "lease_token")
    for column in ("lease_heartbeat_at", "lease_expires_at", "lease_token"):
        op.drop_column("sources", column)
