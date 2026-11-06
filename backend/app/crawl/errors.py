"""Sanitized failure types; messages never include URLs, headers, or credentials."""


class FetchError(Exception):
    """A sanitized failure, with an optional HTTP status for policy decisions."""

    def __init__(self, message: str, *, status_code: int | None = None) -> None:
        super().__init__(message)
        self.status_code = status_code


class SourcePaused(FetchError):
    """Robots policy is unavailable or unsafe; do not fetch source content."""


class RobotsDisallowed(FetchError):
    """A valid robots policy explicitly forbids the requested path."""


class ResponseTooLarge(FetchError):
    """The configured body-size bound was exceeded."""
