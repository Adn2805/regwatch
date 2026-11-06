"""Add durable crawl counters and an explicit paused run outcome."""

from alembic import op
import sqlalchemy as sa

revision = "0002_crawl_run_counts"
down_revision = "0001_core_schema"
branch_labels = None
depends_on = None

COUNTERS = ("discovered", "fetched", "new_versions", "unchanged", "failed", "paused")


def upgrade() -> None:
    for name in COUNTERS:
        op.add_column(
            "crawl_runs", sa.Column(name, sa.Integer(), server_default="0", nullable=False)
        )
    op.create_check_constraint(
        "ck_run_counts", "crawl_runs", " AND ".join(f"{name} >= 0" for name in COUNTERS)
    )
    op.drop_constraint("ck_run_status", "crawl_runs", type_="check")
    op.create_check_constraint(
        "ck_run_status", "crawl_runs", "status IN ('running', 'succeeded', 'failed', 'paused')"
    )


def downgrade() -> None:
    # Preserve the old schema's valid status domain before restoring its check.
    op.execute("UPDATE crawl_runs SET status = 'failed' WHERE status = 'paused'")
    op.drop_constraint("ck_run_status", "crawl_runs", type_="check")
    op.create_check_constraint(
        "ck_run_status", "crawl_runs", "status IN ('running', 'succeeded', 'failed')"
    )
    op.drop_constraint("ck_run_counts", "crawl_runs", type_="check")
    for name in reversed(COUNTERS):
        op.drop_column("crawl_runs", name)
