"""Postgres claims own work; Redis queue delivery is recoverable via expiry."""

import asyncio
import logging
import math

from arq.connections import ArqRedis, RedisSettings as ArqSettings
from redis.asyncio.retry import Retry
from redis.backoff import NoBackoff
from redis.exceptions import RedisError
from sqlalchemy.orm import Session, sessionmaker

from app.core.config import LeaseSettings, RedisSettings, WorkerSettings
from app.core.db import make_engine
from app.schedule.leases import LeaseClaim, LeaseRepository


def arq_settings(settings: RedisSettings) -> ArqSettings:
    return ArqSettings(
        host=settings.redis_host,
        port=settings.redis_port,
        database=settings.redis_db,
        password=settings.redis_password.get_secret_value() if settings.redis_password else None,
        conn_timeout=math.ceil(settings.redis_timeout_s),
        conn_retries=0,
        max_connections=settings.redis_max_connections,
        retry=Retry(NoBackoff(), 0),
    )


async def open_queue(settings: RedisSettings) -> ArqRedis:
    redis = ArqRedis(
        host=settings.redis_host,
        port=settings.redis_port,
        db=settings.redis_db,
        password=settings.redis_password.get_secret_value() if settings.redis_password else None,
        socket_connect_timeout=settings.redis_timeout_s,
        socket_timeout=settings.redis_timeout_s,
        retry=Retry(NoBackoff(), 0),
        max_connections=settings.redis_max_connections,
    )
    try:
        await redis.ping()
        return redis
    except (RedisError, OSError):
        await redis.aclose()
        raise RuntimeError("Queue connection unavailable") from None


class Scheduler:
    def __init__(
        self,
        sessions: sessionmaker[Session],
        redis: ArqRedis,
        *,
        queue_name: str = "regwatch:queue",
        lease_s: float = 60,
        timeout_s: float = 2,
    ) -> None:
        self._leases = LeaseRepository(sessions, lease_s=lease_s)
        self._redis, self._queue_name, self._timeout_s = redis, queue_name, timeout_s

    async def tick(self) -> list[LeaseClaim]:
        await asyncio.to_thread(self._leases.recover_expired)
        claims = await asyncio.to_thread(self._leases.claim_due)
        try:
            for claim in claims:
                async with asyncio.timeout(self._timeout_s):
                    await self._redis.enqueue_job(
                        "crawl_source",
                        str(claim.source_id),
                        str(claim.token),
                        str(claim.run_id),
                        _job_id=f"{self._queue_name}:{claim.token}",
                        _queue_name=self._queue_name,
                    )
        except (RedisError, OSError, TimeoutError):
            # Do not undo a claim: enqueue may have succeeded before connection loss.
            raise RuntimeError(
                "Scheduler enqueue failed; claims retained for expiry recovery"
            ) from None
        return claims


async def main() -> None:
    settings = WorkerSettings()
    if not settings.scheduler_enabled:
        logging.getLogger(__name__).info("Scheduler disabled; no sources will be crawled")
        await asyncio.Event().wait()
        return
    redis_settings = RedisSettings()
    redis = await open_queue(redis_settings)
    engine = make_engine()
    try:
        scheduler = Scheduler(
            sessionmaker(engine),
            redis,
            queue_name=settings.queue_name,
            lease_s=LeaseSettings().lease_duration_s,
            timeout_s=redis_settings.redis_timeout_s,
        )
        while True:
            try:
                await scheduler.tick()
            except Exception:
                # Poll again after the configured interval; never log arbitrary SQL/Redis payloads.
                logging.getLogger(__name__).error(
                    "Scheduler tick failed; leases remain recoverable"
                )
            await asyncio.sleep(settings.scheduler_poll_s)
    finally:
        await redis.aclose()
        engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())
