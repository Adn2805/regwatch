"""Discovery contracts; adapters parse data but never make network calls."""

from dataclasses import dataclass
from datetime import datetime
from typing import Protocol
from urllib.parse import urljoin, urlsplit, urlunsplit


class AdapterParseError(ValueError):
    """The input cannot be safely interpreted as a source's publication list."""


@dataclass(frozen=True)
class Publication:
    canonical_url: str
    title: str
    published_at: datetime | None = None


class SourceAdapter(Protocol):
    def discover(self, content: bytes, *, base_url: str) -> list[Publication]:
        """Parse a fetched listing, or raise AdapterParseError for invalid input."""
        ...


def canonicalize_url(value: str, *, base_url: str) -> str:
    """Normalize identity without changing case-sensitive paths or query values."""
    value = value.strip()
    if not value or any(character.isspace() or ord(character) < 32 for character in value):
        raise AdapterParseError(
            "Publication URL is empty or contains whitespace/control characters"
        )
    try:
        parts = urlsplit(urljoin(base_url, value))
        if parts.scheme not in {"http", "https"} or not parts.hostname:
            raise AdapterParseError("Publication URL must be an absolute HTTP(S) URL")
        if parts.username is not None or parts.password is not None:
            raise AdapterParseError("Publication URL must not contain credentials")
        host = parts.hostname.lower()
        if ":" in host:
            host = f"[{host}]"  # urlsplit removes the brackets around IPv6 addresses.
        port = parts.port  # Access validates malformed and out-of-range port numbers.
    except ValueError as error:
        raise AdapterParseError("Invalid publication URL") from error
    default_port = 80 if parts.scheme == "http" else 443
    authority = host if port is None or port == default_port else f"{host}:{port}"
    return urlunsplit((parts.scheme, authority, parts.path or "/", parts.query, ""))
