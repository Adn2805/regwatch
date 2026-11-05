# Changelog

## Unreleased

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
