# RegWatch

A deterministic regulatory change monitor, built in gated steps. Currently:
Phase 1: infrastructure, schema, and fixture-based RBI discovery. RBI is the
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
fetching, snapshots, change detection, read endpoints, and CI remain unfinished.

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
