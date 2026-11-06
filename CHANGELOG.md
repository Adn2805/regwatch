# Changelog

## Unreleased

- Add a bounded crawl CLI with document-limit validation, success-only feed
  sampling, sanitized diagnostics, and tests; RBI live verification stopped at
  robots HTTP 418 without requesting the feed or any document.

- Add Step 6 read endpoints for sources, documents, document versions, and changes,
  with Pydantic responses, bounded pagination, source/document/date filters,
  and explicit 404/422 responses.
- Add a localhost-only FastAPI/Uvicorn Compose service and real-PostgreSQL API
  tests for query boundaries, filters, content exclusion, errors, and OpenAPI docs.
- Record Phase 2 lease-based recovery for stuck running crawl audit records.
- Verify 191 tests and a running API docs/OpenAPI/read smoke test; live RBI verification and the Phase 1 gate remain pending.

- Add Phase 1 Step 5 crawl orchestration, source-scoped document upserts,
  version deduplication, and atomic first/subsequent-version naive changes.
- Add migration 0002 for durable crawl counts and paused outcomes; isolate
  document failures and stop paused sources with recorded reasons.
- Verify mocked discovery/fetch plus real PostgreSQL/MinIO, including a hard
  exit after upload and rollback when inserting a change fails.
- Clarify the unexpected-compression error and document safe orphan snapshots.
- Verify 156 tests in Docker; the complete Phase 1 gate remains pending.

- Verify identity encoding with a real-gzip test that rejects compression before reading.
- Add SHA-256 hashing, conditional MinIO snapshot writes, immutable object metadata,
  and upload-before-transaction version persistence.
- Test duplicate/concurrent writes, raw-byte retrieval, storage failure isolation,
  and database rollback/retry against local MinIO and PostgreSQL.

- Add Phase 1 Step 3: httpx fetcher, robots policy checks and TTL cache,
  request pacing, bounded retries/redirects, and streaming response size limits.
- Add mocked timeout/status/redirect/policy/size/privacy tests with fake clocks;
  no live regulator requests are performed.
- Add environment-driven fetch settings and sanitized failure outcomes.

- Add a generic discovery protocol, immutable publication records, and a pure
  RBI RSS adapter with synthetic fixtures and failure-path tests.
- Validate URLs, normalize publication timestamps to UTC, deduplicate entries,
  and reject malformed/unsafe feeds without making network requests.
- Add Step 1 Compose infrastructure, core schema, and initial Alembic migration.
- Keep live crawling disabled pending a concrete robots.txt access statement.
