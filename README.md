# RegWatch

A deterministic regulatory change monitor, built in gated steps. Phase 1 is
complete: PostgreSQL/Redis/MinIO infrastructure, schema and migrations, robots-aware
bounded fetching, immutable snapshots, raw-byte version deduplication, crawl-run
auditing, four read endpoints and Docker-based GitHub Actions CI.

RBI is fixtures-only after robots HTTP 418. SEBI discovery covers the first
listing page only; a two-document live repeat crawl created zero new versions.
The CLI is explicitly invoked; there is no automatic scheduler yet.

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

Phase 1 gate passed for commit `406a648`: a fresh external clone started with
one Compose command; `/docs` and `/sources` returned HTTP 200; hashing and adapter
tests passed (54 tests); GitHub Actions passed. The two live SEBI runs fetched two
HTML documents each, with five-second delays and zero retries: two versions on
the first run and zero new versions on the repeat. These are functional checks,
not load benchmarks or AI evaluation results.

[Phase 1 CI result](https://github.com/Adn2805/regwatch/actions/runs/42211577032).

The following historical checks describe earlier implementation steps.

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

These historical Step 1 checks are functional checks, not load benchmarks.
The completed Phase 1 gate and CI result are summarized above.

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
  were deferred to Phase 2; see the current Batch A notes below.

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

Live crawling requires an explicit CLI invocation and a real contact setting.
Phase 1 live verification and CI passed; parsing and scheduled workers are Phase 2 work.


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
checks, not load benchmarks. The later completed Phase 1 gate is summarized above.


## Step 5: crawl service and deduplication

`await CrawlService(sessions, fetcher, store).run(source_id)` runs one existing
source. The caller owns the SQL engine, HTTP client, and S3 client. The default
adapter registry contains RBI; dependencies can be injected for deterministic
fixtures. No source is seeded/activated and no worker, scheduler, or HTTP API is
started by this step.

The listing is fetched with robots checks, then the adapter discovers unique
canonical URLs. Each document is upserted by `(source_id, canonical_url)` and
processed independently. Fetch failures do not prevent later documents from
being attempted. Immutable storage precedes a short transaction that locks the
document, inserts a previously unseen content hash, and links a naive change to
the latest stored version. Version and change either commit together or roll
back together. The first version creates no change. Repeat hashes create neither
versions nor changes. This includes a return to previously stored bytes: the
unique-content model does not record separate reversion events.

Versions are raw-only for now: `normalized_text=""`, `sections=[]`, and
`parser_yield=0`. The naive diff records from/to hashes and sets
`is_cosmetic=false`; it does not claim a meaningful regulatory difference.
`record_version` now returns a VersionRecord with `version_id`, `created`, and
optional `change_id`, allowing precise per-run counts without a separate
existence check.

| Crawl-run count | Meaning |
| --- | --- |
| `discovered` | Unique URLs returned by the adapter |
| `fetched` | Successful document HTTP fetches, excluding the listing |
| `new_versions` | Newly committed document versions |
| `unchanged` | Documents whose hash already exists for that document |
| `failed` | Failed document attempts, or one failed discovery attempt |
| `paused` | One stop event for an already-paused source or unavailable robots policy |

Counts overlap: a fetched document can later fail during storage/database work.
Untouched documents after a pause are not counted as failed. A partial run with
any document failures has status `failed`, while successfully processed documents
remain committed. A robots-policy pause stops cleanly, records a reason, and
sets source status to `paused`; explicit reactivation is required. Audit errors
contain fixed messages or exception class names, not request payloads or secrets.

Apply the new count/status migration and run from `D:\Project\regwatch`:

```powershell
docker compose build migrate
docker compose run --rm --no-deps migrate alembic upgrade head
docker compose run --rm --no-deps migrate alembic check
docker compose run --rm --no-deps migrate pytest tests/integration/test_crawl_service.py -q
docker compose run --rm --no-deps migrate pytest -q
docker run --rm --network none --entrypoint ruff regwatch-migrate check .
docker run --rm --network none --entrypoint mypy regwatch-migrate app migrations tests
```

The crawl tests mock every regulator HTTP request and use a synthetic RSS
fixture. They use local PostgreSQL and MinIO with isolated test rows/buckets.
Scenarios cover the first crawl, an unchanged repeat, new bytes, a failed document
followed by a successful one, robots pauses before and during processing,
malformed discovery, and atomic rollback if Change insertion fails.

A child-process test calls `os._exit(17)` immediately after snapshot upload.
It confirms one object and zero versions at the crash point; retry produces
exactly two document versions and zero changes for the two-document fixture.
A further unchanged run adds nothing. The old run remains `running` with its
last checkpoint because a hard-killed process cannot finalize audit state.
Counters can also lag a committed version if a process dies before its next
checkpoint. Phase 2 leases/recovery must reconcile abandoned work; this step
proves duplicate-free data retries, not automatic crash recovery.

The compression error is explicitly distinct: `Unexpected response content
encoding; expected identity`. The exact message is asserted in the tests.
Phase 1 is complete. Phase 2 Batch A adds distributed fetching controls;
source leases and abandoned-run recovery remain Batch B work.


Step 5 verification on 2026-11-06, final Docker image:

```text
Alembic: 0002_crawl_run_counts (head)
Schema check: No new upgrade operations detected.
Offline unit suite: 135 passed in 0.77s
Full suite: 156 passed, 153 warnings in 4.33s
Ruff: All checks passed!
Formatting: 49 files already formatted
Strict mypy: Success: no issues found in 31 source files
```

All nine crawl scenarios passed, including the hard-exit retry and atomic
version/change rollback. The 153 warnings come from the SDK UTC deprecation
already observed in Step 4 and remain visible. No live regulator HTTP requests
were made. These are functional checks, not throughput/load benchmarks.


## Step 6: read API

Start with `docker compose up --build` from `D:\Project\regwatch`.
The API waits for migrations and binds only to localhost. Open
[Swagger UI](http://127.0.0.1:8000/docs) or
[the OpenAPI specification](http://127.0.0.1:8000/openapi.json).
No source is activated and no crawl is triggered by API startup or requests.

All four endpoints return `{items, total, limit, offset}`. Defaults are
`limit=50` and `offset=0`; accepted bounds are limit 1..100 and offset 0..100000.
An offset at or beyond the result count returns an empty items list. Total is
the number of matching rows before pagination.

| Endpoint | Filters | Order |
| --- | --- | --- |
| `GET /sources` | None | name, UUID ascending |
| `GET /documents` | `source_id` | created_at, UUID descending |
| `GET /documents/{document_id}/versions` | Required document path ID | fetched_at, UUID descending |
| `GET /changes` | `document_id`, `date_from`, `date_to` | created_at, UUID descending |

Date filters use the change's created_at and accept timezone-aware timestamps,
e.g. `2026-11-06T00:00:00Z` or `2026-11-06T05:30:00+05:30`. Both bounds are
inclusive; date_from may equal date_to. Naive timestamps or reversed bounds
return 422. Encode `+` as `%2B` when writing an offset timestamp directly in a URL.
UUID tie-breakers provide stable ordering for equal timestamps, but concurrent
inserts can still shift offset pages; this API does not promise a snapshot.

Unknown document path IDs, unknown source_id filters, and unknown document_id
filters return 404. A known entity with no matching children returns 200 and an
empty page. Invalid UUIDs, pagination, dates, date ranges, and unknown query
parameters return 422. Database failures return a sanitized 503; there are no
automatic query retries in this API layer.

Version responses include ID, document ID, snapshot_key, content_hash, fetched_at,
HTTP status, and parser yield. They never include raw snapshot bytes, extracted
text, sections, or search vectors; the query excludes those content columns.
The read API has no object-download endpoint and does not connect to MinIO.
Source responses exclude validators and configuration credentials.

Examples:

```powershell
Invoke-RestMethod -NoProxy 'http://127.0.0.1:8000/sources?limit=10&offset=0'
Invoke-RestMethod -NoProxy 'http://127.0.0.1:8000/changes?date_from=2026-11-06T00:00:00Z'
```

Verification with the existing services running:

```powershell
docker compose build migrate api
docker compose run --rm --no-deps migrate pytest tests/integration/test_api.py -q
docker compose run --rm --no-deps migrate pytest -q
docker run --rm --network none --entrypoint ruff regwatch-migrate check .
docker run --rm --network none --entrypoint mypy regwatch-migrate app migrations tests
```

API tests use real PostgreSQL in unique `api_test_*` schemas, seed committed
fixture data, and drop only their own schema afterward. They cover empty pages,
pagination/max bounds and timestamp ties, filters and inclusive timezone-aware
dates, 404/422, fixed 503 errors, response metadata allowlists, read-only HTTP
methods, docs HTML, and the OpenAPI specification. Other integration tests
continue to use only local PostgreSQL/MinIO and mocked regulator HTTP.

The stuck-running audit decision is recorded in ARCHITECTURE.md: Phase 2 will
add lease expiry and recovery/reconciliation. No recovery implementation or
live RBI request was added in Step 6. Later verification kept RBI fixtures-only
after robots HTTP 418, verified SEBI repeat-crawl deduplication, and passed CI
and the fresh-clone startup gate.


Step 6 verification on 2026-11-06:

```text
API integration tests: 35 passed, 1 warning in 4.12s
Schema check: No new upgrade operations detected.
Offline unit suite: 135 passed in 1.04s
Full final-image suite: 191 passed, 154 warnings in 9.25s
Ruff: All checks passed!
Formatting: 62 files already formatted
Strict mypy: Success: no issues found in 38 source files
Running API smoke test: /docs 200, /openapi.json 200, /sources 200
```

The running-service probe used direct IPv4 loopback with PowerShell `-NoProxy`.
An initial `localhost` probe did not reach the container; the direct probe
succeeded without changing the application. The 154 warnings consist of the
existing SDK UTC deprecation plus a Starlette/AnyIO compatibility deprecation;
they remain visible. API tests and all existing tests passed. These are
historical functional checks, not load benchmarks; the final Phase 1 gate passed later.


## Bounded crawl CLI

The explicit command `docker compose run --rm --no-deps migrate python -m
app.crawl.cli --max-documents 3` runs at most three unique documents through the
normal fetcher and service. It requires a non-placeholder contact in ignored
`.env`, never reactivates a paused source, and enforces at least five-second
pacing plus any larger Crawl-delay. Optional `--feed-sample PATH` writes a
trimmed listing only after success; it reuses fetched bytes without another
listing request. Output contains counts and sanitized errors/status traces,
not contact headers or credentials.

The initial RBI live attempt stopped at robots HTTP 418 with paused=1 and all
other counts zero. No feed/document request or real fixture was produced.
RBI remains paused and must be exercised through fixtures, not retried live.


Robots failure regression: every HTTP 4xx other than 404/410 pauses access,
including 418 and retry-exhausted 429. A cached failure retains its exact safe
status reason, e.g. `Source paused: robots.txt returned HTTP 418`. Crawl audit
rows preserve that reason. The original first RBI run is not rewritten.
404/410 retain the approved unrestricted-policy behavior. The public
`Fetcher.robots_policy(url)` method loads policy through the same HTTP transport,
User-Agent, timeouts, limits, pacing, and parser without fetching a content page.
RBI remains fixtures-only and is not retried live.


Robots-status verification on 2026-11-06:

```text
Offline robots unit tests: 129 passed in 0.34s
Full local Docker suite: 308 passed, 188 warnings in 8.66s
Ruff: All checks passed!
Strict mypy: Success: no issues found in 40 source files
```

The warnings are the existing SDK UTC and Starlette/AnyIO deprecations. These
checks used mocked regulator HTTP; RBI was not retried. A one-request SEBI
robots-only probe is performed separately with retries and redirects disabled,
using the normal crawler identity/transport. It does not crawl any content page.


## SEBI verification and source credit

This is a portfolio project, not legal advice.
SEBI material is sourced from the [Securities and Exchange Board of India](https://www.sebi.gov.in/).
The user reviewed the [SEBI website policy](https://www.sebi.gov.in/website-policy.html)
and authorized only a small, slow crawl of public documents for private storage.
Full SEBI circulars and PDFs must not be committed to this repository. Committed
content fixtures must be small trimmed samples that preserve structure and
accuracy and acknowledge SEBI as the source; copyright/permission requirements
remain applicable. This records the user's reviewed constraints, not an
independent legal assessment.

Part 1 captured [SEBI robots.txt](https://www.sebi.gov.in/robots.txt) through one
normal RegWatch robots-only request on 2026-11-06: HTTP 200, 101 bytes.
The exact fixture is `backend/tests/fixtures/sebi_robots.txt`, with SHA-256
`5b67a9fe05a33e682223588db67fce7753041b85ff8264d8ce1ae854f40a366c`.
Git newline conversion is disabled for that file to preserve its bytes.
The PDF path below is only a local policy-check example; no PDF was requested.

| Path | RegWatch policy decision |
| --- | --- |
| `/legal.html` | Allowed |
| `/sebi_data/attachdocs/example-circular.pdf` | Allowed |
| `/js/x` | Disallowed |
| `/css/x` | Disallowed |
| `/hindi/js/x` | Disallowed |
| `/hindi/css/x` | Disallowed |

An empty `Disallow:` line imposes no restriction; it does not erase the other
non-empty restrictions in the same policy. The tests also verify an empty
Disallow by itself allows all three requested example paths.
No listing, circular, or PDF request was made. Part 2 requires a separate go-ahead.

```powershell
docker compose build migrate
docker run --rm --network none --entrypoint pytest regwatch-migrate tests/unit/test_sebi_robots.py -q
```


Part 1 verification in offline Docker:

```text
Exact SEBI policy and empty-rule tests: 9 passed in 0.02s
Combined robots suite: 138 passed in 0.85s
Ruff: All checks passed!
Strict mypy: Success: no issues found in 41 source files
```

The staged fixture was verified byte-for-byte against the captured 101-byte
response. Its original CRLF and trailing space are intentionally preserved.

SEBI discovery currently supports only the first HTML listing page (`sample_1`).
The committed `sebi_circulars_trimmed.html` fixture contains five metadata rows
credited to SEBI; full captures stay private in gitignored `local_only/` and are
excluded from Docker images. No circulars or PDFs are included. Date-only labels
use midnight India converted to UTC. `SebiAdapter.skipped_rows` reports malformed
and duplicate rows from its latest call; a missing table is an explicit failure.
Run its offline tests with:

```sh
docker compose run --rm --no-deps migrate pytest tests/unit/test_sebi_adapter.py -q
```

GitHub Actions runs lint, strict types and tests against local PostgreSQL/MinIO;
`python scripts/check_history.py` scans all reachable/reflog commits for private
paths, known local secret values, token patterns and non-placeholder email addresses,
without printing matched values. Pattern scanning cannot prove absence of every
possible secret.

Phase 2 Batch A step 1 adds a standalone Redis token bucket. Run its real-Redis
checks with `docker compose run --rm --no-deps migrate pytest tests/integration/test_redis_ratelimit.py -q`
after rebuilding `migrate`. The component is now wired into CLI HTTP fetching.

Phase 2 A2 wires shared Redis limits into the explicit CLI. Every request attempt
(including robots/retries/redirects) is admitted; concurrency permits last through
body closure. Configure `DOMAIN_CONCURRENCY`, `RATE_INTERVAL_S`, `RATE_CAPACITY`,
`COORDINATION_WAIT_S` and `CRAWL_TOTAL_TIMEOUT_S` consistently across workers.
Redis failure stops fetching. Run `tests/integration/test_redis_fetcher.py` in
Compose to verify shared limits and cancellation with mocked HTTP.

Phase 2 A3 shares origin-scoped robots policies (including paused outcomes) in
Redis. `ROBOTS_CACHE_TTL_S` defaults to 3600; `ROBOTS_REFRESH_LEASE_S` controls the
renewable refresh lock. Lost ownership, corrupted cache records and Redis outages
pause fetching; there is no fallback that assumes permission. Retry backoff uses
full jitter while honoring capped Retry-After and configured/Crawl-delay pacing.
Run `tests/integration/test_shared_robots.py` and
`tests/unit/test_retry_jitter.py` for real-Redis/mocked-HTTP verification.

A required request/Crawl-delay forces effective token capacity one, even if
`RATE_CAPACITY` is larger; burst configuration cannot relax politeness.

Phase 2 B4 adds URL-scoped ETag/Last-Modified per document. Successful 304 responses
count fetched/unchanged and create no snapshot or version. Missing response
validators clear stored metadata, and 200 bodies are processed even when a server
repeats its validator. A delayed 304 cannot overwrite newer document state.
`0003_document_validators` only adds three nullable columns; no backfill occurs.
Migration downgrade tests use a separate disposable database, never local data.
Run `tests/unit/test_conditional_fetch.py`, `tests/integration/test_crawl_service.py`
and `tests/integration/test_validator_migration.py` in Docker for verification.

For the repeated concurrency/crash gate, mount `scripts/` at `/scripts` in the test
container and run `python /scripts/repeat_tests.py` with the pytest paths and `-k`
selection. It uses 20 fresh pytest processes, rejects skips, stops on the first
failure, and never retries a failing round or adds inter-round sleeps.
