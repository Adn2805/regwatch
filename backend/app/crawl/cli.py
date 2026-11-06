"""One explicitly requested, bounded RBI crawl through the normal pipeline."""

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

from app.core.config import FetchSettings, StorageSettings
from app.core.db import make_engine
from app.core.s3 import SnapshotStore, make_s3_client
from app.crawl.fetcher import Fetcher, FetchResult
from app.crawl.service import CrawlService, safe_error
from app.models.schema import Source

FEED_URL = "https://www.rbi.org.in/notifications_rss.xml"


def document_limit(value: str) -> int:
    try:
        limit = int(value)
    except ValueError:
        raise argparse.ArgumentTypeError("max-documents must be an integer from 1 to 100") from None
    if not 1 <= limit <= 100:
        raise argparse.ArgumentTypeError("max-documents must be an integer from 1 to 100")
    return limit


def require_real_contact(settings: FetchSettings) -> None:
    domain = settings.crawl_contact_email.get_secret_value().split("@")[-1].lower()
    if domain in {"example.com", "example.org", "example.net"} or domain.endswith(
        (".invalid", ".test", ".example", ".localhost")
    ):
        raise ValueError("Configure a non-placeholder CRAWL_CONTACT_EMAIL before a live crawl")


def rbi_source(sessions: sessionmaker[Session]) -> UUID:
    with sessions.begin() as session:
        source = session.scalar(
            select(Source).where(Source.adapter == "rbi", Source.base_url == FEED_URL)
        )
        if source is None:
            # Only this explicit CLI command activates a newly registered live source.
            source = Source(
                name="RBI notifications", base_url=FEED_URL, adapter="rbi", status="active"
            )
            session.add(source)
            session.flush()
        # Never reactivate a source paused by robots or an operator.
        return source.id


class RecordingFetcher(Fetcher):
    def __init__(self, client: httpx.AsyncClient, settings: FetchSettings) -> None:
        super().__init__(client, settings)
        self.feed: bytes | None = None

    async def fetch(self, url: str) -> FetchResult:
        response = await super().fetch(url)
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


async def run_once(max_documents: int, feed_sample: Path | None) -> int:
    fetch_settings = FetchSettings()
    require_real_contact(fetch_settings)
    fetch_settings = fetch_settings.model_copy(
        update={"crawl_delay_s": max(5.0, fetch_settings.crawl_delay_s)}
    )
    storage_settings = StorageSettings()
    engine = make_engine()
    s3 = make_s3_client(storage_settings)
    sessions = sessionmaker(engine)
    requested: list[str] = []
    responses: list[dict[str, str | int]] = []

    async def request_hook(request: httpx.Request) -> None:
        # Trace destinations/status only. Never record headers, query values, or bodies.
        requested.append(str(request.url.copy_with(query=None, fragment=None)))

    async def response_hook(response: httpx.Response) -> None:
        responses.append(
            {
                "url": str(response.url.copy_with(query=None, fragment=None)),
                "status": response.status_code,
            }
        )

    try:
        source_id = await asyncio.to_thread(rbi_source, sessions)
        async with httpx.AsyncClient(
            event_hooks={"request": [request_hook], "response": [response_hook]}
        ) as client:
            fetcher = RecordingFetcher(client, fetch_settings)
            service = CrawlService(
                sessions, fetcher, SnapshotStore(s3, storage_settings.snapshot_bucket)
            )
            summary = await service.run(source_id, max_documents=max_documents)
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
        s3.close()
        engine.dispose()


def main() -> int:
    parser = argparse.ArgumentParser(description="Run one polite, bounded RBI notification crawl")
    parser.add_argument("--max-documents", type=document_limit, default=3)
    parser.add_argument(
        "--feed-sample", type=Path, help="Save a trimmed feed only after a successful crawl"
    )
    arguments = parser.parse_args()
    try:
        return asyncio.run(run_once(arguments.max_documents, arguments.feed_sample))
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
