"""Conditional, content-addressed snapshot writes through the S3 API."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING

import boto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError

from app.core.config import StorageSettings
from app.diff.hashing import content_hash

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client


class SnapshotStorageError(Exception):
    """A sanitized storage failure; no database version should be recorded."""


@dataclass(frozen=True)
class Snapshot:
    key: str
    content_hash: str
    content_type: str
    size_bytes: int
    created: bool


def make_s3_client(settings: StorageSettings | None = None) -> S3Client:
    settings = settings if settings is not None else StorageSettings()
    # SDK debug logs may contain signing credentials and request headers.
    for name in ("boto3", "botocore", "s3transfer", "urllib3"):
        logging.getLogger(name).setLevel(logging.WARNING)
    return boto3.client(
        "s3",
        endpoint_url=str(settings.s3_endpoint_url),
        region_name=settings.s3_region,
        aws_access_key_id=settings.minio_root_user.get_secret_value(),
        aws_secret_access_key=settings.minio_root_password.get_secret_value(),
        config=Config(
            signature_version="s3v4",
            s3={"addressing_style": "path"},
            connect_timeout=settings.s3_connect_timeout_s,
            read_timeout=settings.s3_read_timeout_s,
            retries={"mode": "standard", "total_max_attempts": settings.s3_max_attempts},
        ),
    )


class SnapshotStore:
    def __init__(self, client: S3Client, bucket: str) -> None:
        self._client, self._bucket = client, bucket

    def put(self, content: bytes, *, content_type: str = "application/octet-stream") -> Snapshot:
        content_type = content_type.strip()
        if not content_type or not content_type.isascii() or not content_type.isprintable():
            raise ValueError("Content type must be non-empty printable ASCII")
        digest = content_hash(content)
        key = f"sha256/{digest}"
        try:
            # Do not replace this with HEAD-then-PUT: that races with other writers.
            self._client.put_object(
                Bucket=self._bucket,
                Key=key,
                Body=content,
                ContentType=content_type,
                Metadata={
                    "content-type": content_type,
                    "byte-size": str(len(content)),
                    "sha256": digest,
                },
                IfNoneMatch="*",
            )
        except ClientError as error:
            if error.response.get("ResponseMetadata", {}).get("HTTPStatusCode") != 412:
                raise SnapshotStorageError("Snapshot upload failed") from None
            return self._existing(key, digest, len(content))
        except BotoCoreError:
            raise SnapshotStorageError("Snapshot upload failed") from None
        return Snapshot(key, digest, content_type, len(content), created=True)

    def _existing(self, key: str, digest: str, size: int) -> Snapshot:
        try:
            head = self._client.head_object(Bucket=self._bucket, Key=key)
        except (BotoCoreError, ClientError):
            raise SnapshotStorageError("Existing snapshot verification failed") from None
        metadata = head.get("Metadata", {})
        content_type = head.get("ContentType", "")
        if (
            head.get("ContentLength") != size
            or metadata.get("byte-size") != str(size)
            or metadata.get("sha256") != digest
            or not content_type
            or metadata.get("content-type") != content_type
        ):
            raise SnapshotStorageError("Existing snapshot metadata is inconsistent")
        # Identical bytes may be served with a different Content-Type later.
        # Preserve the original object's metadata rather than rewriting it.
        return Snapshot(key, digest, content_type, size, created=False)
