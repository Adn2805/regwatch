# RegWatch

A deterministic regulatory change monitor, built in gated steps. Currently:
Phase 1: infrastructure, schema, fixture-based RBI discovery, and a tested
fetcher with robots handling, plus snapshot storage and version persistence. RBI is the
first intended source; live crawling is disabled pending robots policy review.

## Start

Requires Docker Desktop with Linux containers and Docker Compose.
From this directory, copy `.env.example` to `.env` and replace both password
placeholders with different random local values. Do not commit `.env`.
After this one-time configuration:

```powershell
docker compose up --build
```

Postgres and Redis become healthy; `migrate` and `bucket-init` must exit 0.
MinIO console: http://localhost:9001 (credentials from your local `.env`).
Persistent named volumes survive container restarts. Never use `down -v` unless
you intend to delete all local database and object data.
MinIO and its client compile from upstream source because the official registry
pulls failed. Expect a longer first build and network access for dependencies.

## Verify Step 1

```powershell
docker compose ps -a
docker compose logs migrate bucket-init
docker compose run --rm migrate alembic current
docker compose run --rm migrate alembic check
docker compose run --rm migrate pytest -q
docker compose run --rm migrate ruff check .
docker compose run --rm migrate mypy app migrations tests
docker compose exec redis redis-cli ping
docker compose exec postgres sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "\dt"'
```

To check migration reversibility on an empty development database only:
`docker compose run --rm migrate alembic downgrade base`, followed by
`docker compose run --rm migrate alembic upgrade head`. Downgrading deletes tables.

## Results

No crawl, performance, or AI evaluation results have been measured.
Phase 1's complete gate remains pending. Discovery uses synthetic fixtures;
crawl orchestration, parsing, change detection, read endpoints, and CI remain unfinished.

Step 1 verification on 2026-11-05 (Docker Desktop, Linux containers):

| Check | Observed result |
| --- | --- |
| `docker compose up --build -d` | Successful; all five services created |
| PostgreSQL / Redis | Healthy; Redis returned `PONG` |
| Migration / bucket initializer | Both exited 0 |
| Alembic current | `0001_core_schema (head)` |
| Alembic schema comparison | `No new upgrade operations detected.` |
| Unit + PostgreSQL integration tests | 7 passed |
| Ruff / strict mypy | Passed |
| Migration downgrade, upgrade, repeat upgrade | Successful on empty development data |
| Repeated bucket initialization | Successful; bucket exists, anonymous access disabled |

These are functional checks, not load benchmarks or a completed Phase 1 gate.
GitHub CI has not been configured or run in Step 1.

## Verify fixture-based discovery

Rebuild the backend image to include the XML dependency and adapter:

```powershell
docker compose build migrate
docker compose run --rm migrate pytest tests/unit/test_adapters.py -q
docker compose run --rm migrate ruff check .
docker compose run --rm migrate mypy app migrations tests
```

For a readable preview, run from `backend/` in the Python 3.12 environment:

```powershell
python -c "from pathlib import Path; from app.crawl.adapters.rbi import RbiAdapter; print(*RbiAdapter().discover(Path('tests/fixtures/rbi_notifications.xml').read_bytes(), base_url='https://www.rbi.org.in/notifications_rss.xml'), sep='\n')"
```

The synthetic fixture contains three entries representing two distinct URLs.
The parser returns two records, preserves query parameters, removes fragments,
and converts the supplied date to UTC. A missing date stays `None`. Invalid
entries raise `AdapterParseError`; they are not silently reported as no updates.
These fixture entries are illustrative and are not actual RBI circulars.

Verification on 2026-11-06: the rebuilt Compose image passed all 42 tests
(35 adapter cases, 2 schema unit tests, and 5 PostgreSQL integration tests).
Ruff and strict mypy passed locally; these functional checks do not establish
live RBI compatibility or complete the Phase 1 gate.

## Explain the schema

- `sources` holds discovery configuration and crawl health. It defaults to
  paused so adding a row cannot silently authorize network access.
- `documents` identifies publications by source and canonical URL.
- `document_versions` references a raw snapshot and stores extracted text,
  JSONB sections, and its SHA-256 digest. A database unique constraint prevents
  duplicate content for one document, even if two future workers race.
- `changes` joins two distinct versions of the same document. Composite foreign
  keys reject accidental comparisons between unrelated documents.
- `crawl_runs` records attempt status and timing. Queue leases and crash recovery
  remain Phase 2 work.

All timestamps use PostgreSQL `timestamptz`. The generated search vector follows
the text automatically; its GIN index enables full-text lookup later. The vector
extension is enabled, but no embeddings or AI behavior exist in this step.


## Step 3: fetcher and robots handling

Project root: `D:\Project\regwatch`. Rebuild before verifying the new code:

```powershell
docker compose build migrate
docker run --rm --network none --entrypoint pytest regwatch-migrate tests/unit/test_fetcher.py tests/unit/test_robots.py -q
docker compose run --rm migrate pytest -q
docker compose run --rm migrate ruff check .
docker compose run --rm migrate mypy app migrations tests
```

The first test command disables container networking. All HTTP responses and
failures use `httpx.MockTransport`; a fake clock replaces real retry sleeps.
The full suite additionally uses the local Compose database for existing schema
integration tests. No test contacts RBI or another public host.

`Fetcher.fetch(url)` checks robots before content requests and redirect hops.
It returns a `FetchResult` containing final URL, status, response headers, and
bounded bytes. It raises `RobotsDisallowed` for a forbidden path, `SourcePaused`
for an unavailable/unparseable robots policy, `ResponseTooLarge` for oversized
content, and `FetchError` for other fetch failures. These exceptions are outcomes
for future orchestration; no source row is updated or scheduled by this module.
The caller owns and closes the injected `httpx.AsyncClient`.

Robots 404/410 allows access. Robots 401/403, exhausted 5xx/timeout/network
failures, malformed UTF-8/directives, oversize files, and failed redirect chains
pause the source. A valid empty file allows access. Policy cache keys include
scheme/host/port, while pacing is shared per hostname. Both allowed and paused
policies expire after the configured TTL. Cache and pacing are process-local to
one Fetcher; they are not distributed worker limits.

| Environment variable | Default | Meaning |
| --- | --- | --- |
| `CRAWL_CONTACT_EMAIL` | `contact@example.invalid` | Placeholder in User-Agent; replace before live use |
| `CRAWL_TIMEOUT_S` | 15 | Connect/read/write/pool timeout, not an overall crawl deadline |
| `CRAWL_DELAY_S` | 2 | Minimum delay after the previous request finishes |
| `CRAWL_MAX_RETRIES` | 2 | Retries after the first attempt, per redirect hop |
| `CRAWL_BACKOFF_S` | 1 | Initial exponential retry delay |
| `CRAWL_RETRY_AFTER_CAP_S` | 60 | Cap for Retry-After and backoff; pacing can add further delay |
| `CRAWL_MAX_REDIRECTS` | 5 | Maximum followed hops in each redirect chain |
| `CRAWL_MAX_RESPONSE_BYTES` | 10485760 | Streamed raw body limit, including robots |
| `ROBOTS_CACHE_TTL_S` | 3600 | Lifetime for cached permission or paused outcomes |

The effective delay uses the larger of configured delay and applicable
Crawl-delay; the largest observed value stays in effect for that Fetcher's
lifetime. Retry-After supports integer seconds and HTTP dates on 429/503.
Content requests, policy retrieval, and retries all use the same pacing layer.
Robots retrieval and its redirects are necessarily exempt from recursive robots
checks; the retrieved policy applies to the original origin.

Only identity content encoding is supported in this step. Unexpected compression
is rejected, and oversize streams close without buffering the remaining body.
The layer emits no payload/request logs; contact settings are masked in repr,
and request URLs/headers are excluded from error messages.

Live crawling remains disabled. Full orchestration, parsing, change detection,
APIs, and CI are still required before the complete Phase 1 gate can pass.


Step 3 verification on 2026-11-06, using the final Docker image:

```text
Offline fetcher + robots tests: 83 passed in 0.31s
Full suite with local PostgreSQL: 125 passed in 0.95s
Ruff: All checks passed!
Strict mypy: Success: no issues found in 20 source files
```

The network tests cover each requested robots status, timeout/network failures,
malformed policy, TTL refresh and cached failures, fractional Crawl-delay,
concurrent callers, expired policy rechecks during retry, HTTP Retry-After,
redirect loops/hop limits/disallowed destinations, streaming size limits and
stream closure, User-Agent/timeouts, and secret-safe failure/log output.
These are functional test results, not production throughput or load benchmarks.


## Step 4: immutable snapshots and hashing

Compression choice: keep `Accept-Encoding: identity`. Unexpected compression is
rejected before any body read, and the stream closes. A regression test uses a
real gzip payload expanding to 100,000 bytes; it verifies zero reads. There is
no decompression implementation or compressed-payload memory expansion.

`app.diff.hashing.content_hash(bytes)` returns SHA-256 of the exact body bytes.
`SnapshotStore.put(bytes, content_type=...)` writes `sha256/<digest>` with a
conditional S3 PUT (`IfNoneMatch="*"`). Repeated/concurrent identical uploads
create one object. Different bytes have different keys. The original HTTP
Content-Type, `content-type`, `byte-size`, and `sha256` metadata are stored.
If the key exists, metadata and byte size are checked and the original metadata
is retained. No existing object is overwritten, including one with inconsistent
metadata; the latter produces a clear failure rather than a repair write.

`record_version(...)` takes an existing document ID, a FetchResult, and
caller-supplied extracted text/sections. It uploads first, then opens a database
transaction and inserts one version, deduplicated by document ID and content
hash. Storage failure leaves source/document/version counts unchanged. Database
failure rolls back the version; an unreferenced object can remain and be reused
on retry. This is deliberately not described as atomic across two services.
No parser, live crawl, change record, endpoint, or schema migration was added.

S3 configuration uses the existing local-development `MINIO_ROOT_USER`,
`MINIO_ROOT_PASSWORD`, and `SNAPSHOT_BUCKET` values. The client is synchronous
and must be closed by its caller; async workers will need to offload it.

| Environment variable | Default |
| --- | --- |
| `S3_ENDPOINT_URL` | `http://minio:9000` |
| `S3_REGION` | `us-east-1` |
| `S3_CONNECT_TIMEOUT_S` | 3 seconds |
| `S3_READ_TIMEOUT_S` | 10 seconds |
| `S3_MAX_ATTEMPTS` | 3 total attempts per SDK call |

Run from `D:\Project\regwatch` with the existing services running:

```powershell
docker compose build migrate
docker run --rm --network none --entrypoint pytest regwatch-migrate tests/unit -q
docker compose run --rm --no-deps migrate pytest tests/integration/test_snapshots.py -q
docker compose run --rm --no-deps migrate pytest -q
docker run --rm --network none --entrypoint ruff regwatch-migrate check .
docker run --rm --network none --entrypoint mypy regwatch-migrate app migrations tests
```

Unit tests disable container networking and stub S3 responses. Integration tests
use only the local MinIO/PostgreSQL services, create unique `regwatch-test-*`
buckets and database rows, and clean up their own data. They verify byte-for-byte
retrieval, metadata preservation, eight simultaneous identical writes, database
isolation on storage failure, and retry after database rollback. No regulator is
contacted. The pinned SDK currently emits a UTC deprecation warning in signing;
this does not change the test outcome and is not hidden by a warning filter.


Step 4 verification on 2026-11-06, final Docker image:

```text
Offline unit suite: 135 passed in 0.84s
Full suite: 147 passed, 69 warnings in 1.97s
Ruff: All checks passed!
Formatting: 43 files already formatted
Strict mypy: Success: no issues found in 27 source files
```

The 69 warnings are the pinned SDK's UTC deprecation warning during local S3
request signing. Tests passed with the warnings visible. These are functional
checks, not load benchmarks; the complete Phase 1 gate remains pending.
