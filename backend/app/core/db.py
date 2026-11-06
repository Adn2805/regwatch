"""API database engine lifecycle and bounded connection/query waits."""

from sqlalchemy import Engine, create_engine

from app.core.config import Settings


def make_engine() -> Engine:
    return create_engine(
        Settings().database_url(),
        pool_pre_ping=True,
        pool_timeout=5,
        connect_args={
            "connect_timeout": 5,
            "options": "-c statement_timeout=10000 -c lock_timeout=5000",
        },
    )
