"""Content identity is SHA-256 over the exact response bytes, without normalization."""

import hashlib


def content_hash(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()
