"""HTTP validators are opaque values, scoped to one exact resource URL."""

from collections.abc import Mapping
from dataclasses import dataclass, field
from uuid import UUID

from app.crawl.errors import FetchError


@dataclass(frozen=True)
class Validators:
    url: str | None = field(repr=False)
    etag: str | None = field(repr=False)
    last_modified: str | None = field(repr=False)

    def headers_for(self, url: str) -> dict[str, str]:
        if self.url != url:
            return {}
        headers = {}
        for name, value in (
            ("If-None-Match", self.etag),
            ("If-Modified-Since", self.last_modified),
        ):
            if value is not None:
                if not value.isascii() or any(
                    ord(character) < 32 or ord(character) == 127 for character in value
                ):
                    raise FetchError("Invalid conditional validator header")
                headers[name] = value
        return headers

    @classmethod
    def from_response(cls, url: str, headers: Mapping[str, str]) -> "Validators":
        etag, modified = headers.get("etag"), headers.get("last-modified")
        return cls(url if etag is not None or modified is not None else None, etag, modified)


@dataclass(frozen=True)
class DocumentRequestState:
    validators: Validators
    version_id: UUID | None
