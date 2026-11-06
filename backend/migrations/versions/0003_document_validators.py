"""Add nullable per-document HTTP validators; no defaults or data backfill."""

from alembic import op
import sqlalchemy as sa

revision = "0003_document_validators"
down_revision = "0002_crawl_run_counts"
branch_labels = None
depends_on = None


def upgrade() -> None:
    for name in ("etag", "last_modified", "validator_url"):
        op.add_column("documents", sa.Column(name, sa.Text(), nullable=True))


def downgrade() -> None:
    # Only the three newly added metadata columns are removed; document rows stay.
    for name in ("validator_url", "last_modified", "etag"):
        op.drop_column("documents", name)
