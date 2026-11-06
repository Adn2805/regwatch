"""One explicitly requested, bounded regulatory crawl through the normal pipeline."""

import argparse
import asyncio
import json
from pathlib import Path
from uuid import UUID
from xml.etree.ElementTree import Element, tostring

import httpx
from defusedxml import ElementTree  # type: ignore[import-untyped]
from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker

from app.core.config import FetchSettings, StorageSettings, RedisSettings, LeaseSettings
from app.core.redis import make_redis_client
from app.crawl.concurrency import RedisRequestGate
from app.crawl.robots import PolicyCache
from app.crawl.robots_shared import RedisRobotsCache
from app.core.db import make_engine
from app.core.s3 import SnapshotStore, make_s3_client
from app.crawl.fetcher import Fetcher, FetchResult
from app.crawl.conditional import Validators
from app.crawl.errors import FetchError
from app.crawl.service import CrawlService, CrawlSummary, safe_error
from app.schedule.leases import LeaseRepository
from app.schedule.worker import execute_with_heartbeat
from app.models.schema import Source

FEED_URL = "https://www.rbi.org.in/notifications_rss.xml"
SEBI_URL = "https://www.sebi.gov.in/sebiweb/home/HomeAction.do?doListing=yes&sid=1&ssid=7&smid=0"


def document_limit(value: str) -> int:
    try:
        limit = int(value)
    except ValueError:
        raise argparse.ArgumentTypeError("max-documents must be an integer from 1 to 100") from None
    if not 1 <= limit <= 100:
        raise argparse.ArgumentTypeError("max-documents must be an integer from 1 to 100")
    return limit


def require_real_contact(settings: FetchSettings) -> None:
    settings.require_real_contact()


def rbi_source(sessions: sessionmaker[Session], source_name: str = "rbi") -> UUID:
    adapter = source_name
    listing_url = SEBI_URL if adapter == "sebi" else FEED_URL
    with sessions.begin() as session:
        source = session.scalar(
            select(Source).where(Source.adapter == adapter, Source.base_url == listing_url)
        )
        if source is None:
            # Only this explicit CLI command activates a newly registered live source.
            source = Source(
                name="SEBI circulars" if adapter == "sebi" else "RBI notifications",
                base_url=listing_url,
                adapter=adapter,
                status="active",
            )
            session.add(source)
            session.flush()
        # Never reactivate a source paused by robots or an operator.
        return source.id


class RecordingFetcher(Fetcher):
    def __init__(
        self,
        client: httpx.AsyncClient,
        settings: FetchSettings,
        *,
        coordination: RedisRequestGate | None = None,
        robots_cache: PolicyCache | None = None,
    ) -> None:
        super().__init__(client, settings, coordination=coordination, robots_cache=robots_cache)
        self.feed: bytes | None = None

    async def fetch(self, url: str, *, validators: Validators | None = None) -> FetchResult:
        response = await super().fetch(url, validators=validators)
        if url == FEED_URL:
            self.feed = response.content
        return response


def save_feed_sample(content: bytes, destination: Path, limit: int) -> None:
    root: Element = ElementTree.fromstring(content, forbid_dtd=True)
    channel = next(child for child in root if child.tag.rsplit("}", 1)[-1] == "channel")
    items = [child for child in channel if child.tag.rsplit("}", 1)[-1] == "item"]
    for item in items[limit:]:
        channel.remove(item)
    destination.write_bytes(tostring(root, encoding="utf-8", xml_declaration=True))


def check_sebi_request(url: httpx.URL) -> None:
    if url.host != "www.sebi.gov.in" or url.scheme != "https":
        raise FetchError("Live SEBI run permits only the approved HTTPS host")
    if str(url) in {SEBI_URL, "https://www.sebi.gov.in/robots.txt"}:
        return
    if not (url.path.startswith("/legal/circulars/") and url.path.endswith(".html")):
        raise FetchError("Live SEBI run permits circular HTML pages only; PDFs are prohibited")


async def run_once(max_documents: int, feed_sample: Path | None, source_name: str = "rbi") -> int:
    fetch_settings = FetchSettings()
    require_real_contact(fetch_settings)
    fetch_settings = fetch_settings.model_copy(
        update={
            "crawl_delay_s": max(5.0, fetch_settings.crawl_delay_s),
            "crawl_max_retries": 0,
            "crawl_max_redirects": 0,
        }
    )
    redis_settings = RedisSettings()
    redis = make_redis_client(redis_settings)
    storage_settings = StorageSettings()
    engine = make_engine()
    s3 = make_s3_client(storage_settings)
    sessions = sessionmaker(engine)
    requested: list[str] = []
    responses: list[dict[str, str | int]] = []

    async def request_hook(request: httpx.Request) -> None:
        if source_name == "sebi":
            check_sebi_request(request.url)
        # Trace destinations/status only. Never record headers, query values, or bodies.
        requested.append(str(request.url.copy_with(query=None, fragment=None)))

    async def response_hook(response: httpx.Response) -> None:
        if source_name == "sebi" and response.url.path != "/robots.txt" and response.is_success:
            if response.headers.get("content-type", "").split(";")[0].strip().lower() not in {
                "text/html",
                "application/xhtml+xml",
            }:
                raise FetchError("Live SEBI run received a non-HTML response; body not downloaded")
        responses.append(
            {
                "url": str(response.url.copy_with(query=None, fragment=None)),
                "status": response.status_code,
            }
        )

    try:
        source_id = await asyncio.to_thread(rbi_source, sessions, source_name)
        lease_settings = LeaseSettings()
        claims = await asyncio.to_thread(
            LeaseRepository(sessions, lease_s=lease_settings.lease_duration_s).claim_due,
            source_ids=[source_id],
            force=True,
        )
        if not claims:
            raise FetchError("Source is paused or already leased; no HTTP requests issued")
        claim = claims[0]
        async with httpx.AsyncClient(
            event_hooks={"request": [request_hook], "response": [response_hook]}
        ) as client:
            fetcher = RecordingFetcher(
                client,
                fetch_settings,
                coordination=RedisRequestGate(
                    redis, redis_settings, request_timeout_s=fetch_settings.crawl_total_timeout_s
                ),
                robots_cache=RedisRobotsCache(
                    redis, redis_settings, ttl_s=fetch_settings.robots_cache_ttl_s
                ),
            )
            service = CrawlService(
                sessions, fetcher, SnapshotStore(s3, storage_settings.snapshot_bucket), claim=claim
            )

            async def operation() -> CrawlSummary:
                return await service.run(source_id, max_documents=max_documents, stop_on_error=True)

            summary = await execute_with_heartbeat(sessions, claim, lease_settings, operation)
            sample_saved = False
            if summary.status == "succeeded" and feed_sample is not None:
                if fetcher.feed is None:
                    raise RuntimeError("Successful crawl did not capture its fetched listing")
                save_feed_sample(fetcher.feed, feed_sample, max_documents)
                sample_saved = True
            print(
                json.dumps(
                    {
                        "source_id": str(source_id),
                        "crawl_run_id": str(summary.run_id),
                        "status": summary.status,
                        "reason": summary.reason,
                        "errors": summary.errors,
                        "max_documents": max_documents,
                        "source": source_name,
                        "delay_s": fetch_settings.crawl_delay_s,
                        "retries": fetch_settings.crawl_max_retries,
                        "counts": summary.counts.values(),
                        "requested": requested,
                        "responses": responses,
                        "feed_sample_saved": sample_saved,
                    },
                    indent=2,
                )
            )
            return 0 if summary.status == "succeeded" else 1
    finally:
        await redis.aclose()
        s3.close()
        engine.dispose()


def main() -> int:
    parser = argparse.ArgumentParser(description="Run one polite, bounded regulatory crawl")
    parser.add_argument("--source", choices=["rbi", "sebi"], default="rbi")
    parser.add_argument("--max-documents", type=document_limit, default=3)
    parser.add_argument(
        "--feed-sample", type=Path, help="Save a trimmed feed only after a successful crawl"
    )
    arguments = parser.parse_args()
    if arguments.source == "sebi" and arguments.feed_sample is not None:
        parser.error("SEBI feed samples are disabled; fetched content stays private")
    try:
        return asyncio.run(
            run_once(arguments.max_documents, arguments.feed_sample, arguments.source)
        )
    except Exception as error:
        # Known pipeline errors are sanitized; configuration errors never echo input values.
        message = safe_error(error)
        if isinstance(error, ValueError):
            message = "Configuration or feed-sample validation failed; check environment settings"
        print(
            json.dumps({"status": "failed", "error_type": type(error).__name__, "error": message})
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
