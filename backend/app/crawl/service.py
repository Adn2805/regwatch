"""Phase 1 crawl orchestration; no scheduler, parser, API, or live source activation."""

import asyncio
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from uuid import UUID

from sqlalchemy.orm import Session, sessionmaker

from app.core.s3 import SnapshotStorageError, SnapshotStore
from app.crawl.adapters.base import AdapterParseError, Publication, SourceAdapter
from app.crawl.adapters.rbi import RbiAdapter
from app.crawl.errors import FetchError, RobotsDisallowed, SourcePaused
from app.crawl.fetcher import Fetcher
from app.crawl.repository import CrawlRepository, RunCounts
from app.crawl.snapshot import record_version


@dataclass(frozen=True)
class CrawlSummary:
    run_id: UUID
    status: str
    reason: str | None
    counts: RunCounts
    errors: tuple[str, ...] = ()


class CrawlService:
    def __init__(
        self,
        sessions: sessionmaker[Session],
        fetcher: Fetcher,
        store: SnapshotStore,
        *,
        adapters: Mapping[str, SourceAdapter] | None = None,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._sessions, self._fetcher, self._store = sessions, fetcher, store
        self._repository = CrawlRepository(sessions)
        self._adapters = adapters if adapters is not None else {"rbi": RbiAdapter()}
        self._clock = clock

    async def run(self, source_id: UUID, *, max_documents: int | None = None) -> CrawlSummary:
        if max_documents is not None and not 1 <= max_documents <= 100:
            raise ValueError("max_documents must be between 1 and 100")
        errors: list[str] = []
        started = self._clock()
        context = await asyncio.to_thread(self._repository.start, source_id)
        counts = RunCounts()
        status = "failed"
        reason: str | None = None
        try:
            if context.source_status == "paused":
                counts.paused = 1
                status, reason = "paused", "Source is paused; explicit reactivation required"
            else:
                adapter = self._adapters.get(context.adapter)
                if adapter is None:
                    raise ValueError("Unsupported source adapter")
                listing = await self._fetcher.fetch(context.listing_url)
                publications = adapter.discover(listing.content, base_url=listing.url)
                unique: dict[str, Publication] = {}
                for publication in publications:
                    unique.setdefault(publication.canonical_url, publication)
                counts.discovered = len(unique)
                await asyncio.to_thread(self._repository.checkpoint, context.run_id, counts)
                selected = list(unique.values())
                if max_documents is not None:
                    selected = selected[:max_documents]
                for publication in selected:
                    try:
                        document_id = await asyncio.to_thread(
                            self._repository.upsert_document, source_id, publication
                        )
                        response = await self._fetcher.fetch(publication.canonical_url)
                        counts.fetched += 1
                        # Persist progress before upload, including the hard-crash window.
                        await asyncio.to_thread(self._repository.checkpoint, context.run_id, counts)
                        record = await asyncio.to_thread(
                            record_version,
                            self._sessions,
                            self._store,
                            document_id,
                            response,
                            normalized_text="",
                            sections=[],
                        )
                        if record.created:
                            counts.new_versions += 1
                        else:
                            counts.unchanged += 1
                    except SourcePaused as error:
                        errors.append(safe_error(error))
                        counts.paused += 1
                        status = "paused"
                        reason = str(error)
                        break
                    except Exception as error:
                        errors.append(safe_error(error))
                        # Isolate document failures, but do not swallow cancellation or process exits.
                        counts.failed += 1
                        if reason is None:
                            reason = f"Document processing failed: {type(error).__name__}"
                    await asyncio.to_thread(self._repository.checkpoint, context.run_id, counts)
                else:
                    status = "failed" if counts.failed else "succeeded"
        except SourcePaused as error:
            errors.append(safe_error(error))
            counts.paused += 1
            status, reason = "paused", str(error)
        except RobotsDisallowed as error:
            errors.append(safe_error(error))
            counts.paused += 1
            status, reason = "paused", "Robots policy disallows source listing"
        except Exception as error:
            errors.append(safe_error(error))
            counts.failed += 1
            status, reason = "failed", f"Discovery failed: {type(error).__name__}"
        except BaseException:
            counts.failed += 1
            await asyncio.to_thread(
                self._repository.finish,
                context,
                counts,
                "failed",
                "Crawl interrupted",
                (self._clock() - started) * 1000,
            )
            raise
        await asyncio.to_thread(
            self._repository.finish,
            context,
            counts,
            status,
            reason,
            (self._clock() - started) * 1000,
        )
        return CrawlSummary(context.run_id, status, reason, counts, tuple(errors))


def safe_error(error: Exception) -> str:
    """Known layers already sanitize messages; never expose arbitrary SQL/SDK exceptions."""
    if isinstance(error, (AdapterParseError, FetchError, SnapshotStorageError)):
        return f"{type(error).__name__}: {error}"
    return type(error).__name__
