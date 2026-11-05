"""Sanitized failure types; messages never include URLs, headers, or credentials."""


class FetchError(Exception):
    """A request failed; the caller decides how to record the failed crawl."""


class SourcePaused(FetchError):
    """Robots policy is unavailable or unsafe; do not fetch source content."""


class RobotsDisallowed(FetchError):
    """A valid robots policy explicitly forbids the requested path."""


class ResponseTooLarge(FetchError):
    """The configured body-size bound was exceeded."""
