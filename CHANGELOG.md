# Changelog

## Unreleased

- Add a generic discovery protocol, immutable publication records, and a pure
  RBI RSS adapter with synthetic fixtures and failure-path tests.
- Validate URLs, normalize publication timestamps to UTC, deduplicate entries,
  and reject malformed/unsafe feeds without making network requests.
- Add Step 1 Compose infrastructure, core schema, and initial Alembic migration.
- Keep live crawling disabled pending a concrete robots.txt access statement.
