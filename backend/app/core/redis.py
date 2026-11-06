"""Owned async Redis client; coordination errors never include connection details."""

from redis.asyncio import Redis
from redis.backoff import NoBackoff
from redis.asyncio.retry import Retry

from app.core.config import RedisSettings
from app.crawl.errors import FetchError


class CoordinationUnavailable(FetchError):
    """Shared coordination failed; callers must not issue an HTTP request."""


def make_redis_client(settings: RedisSettings) -> Redis:
    return Redis(
        host=settings.redis_host,
        port=settings.redis_port,
        db=settings.redis_db,
        password=settings.redis_password.get_secret_value() if settings.redis_password else None,
        socket_connect_timeout=settings.redis_timeout_s,
        socket_timeout=settings.redis_timeout_s,
        max_connections=settings.redis_max_connections,
        retry=Retry(NoBackoff(), 0),
        retry_on_timeout=False,
        decode_responses=True,
    )
