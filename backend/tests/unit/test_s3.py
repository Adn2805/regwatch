from __future__ import annotations

from collections.abc import Iterator
from typing import TYPE_CHECKING, Any

import pytest
from botocore.stub import Stubber
from pydantic import SecretStr

from app.core.config import StorageSettings
from app.core.s3 import SnapshotStorageError, SnapshotStore, make_s3_client
from app.diff.hashing import content_hash

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client

BUCKET = "unit-snapshots"
BODY = b"raw circular bytes"
TYPE = "application/pdf"
DIGEST = content_hash(BODY)
KEY = f"sha256/{DIGEST}"
META = {"content-type": TYPE, "byte-size": str(len(BODY)), "sha256": DIGEST}
PARAMS: dict[str, Any] = {
    "Bucket": BUCKET,
    "Key": KEY,
    "Body": BODY,
    "ContentType": TYPE,
    "Metadata": META,
    "IfNoneMatch": "*",
}


@pytest.fixture
def client() -> Iterator[S3Client]:
    settings = StorageSettings(
        minio_root_user=SecretStr("unit-access-key"),
        minio_root_password=SecretStr("unit-secret-key"),
    )
    value = make_s3_client(settings)
    yield value
    value.close()


def test_conditional_put_contains_content_identity_and_metadata(client: S3Client) -> None:
    with Stubber(client) as stub:
        stub.add_response("put_object", {}, PARAMS)
        snapshot = SnapshotStore(client, BUCKET).put(BODY, content_type=TYPE)
        stub.assert_no_pending_responses()
    assert snapshot.key == KEY
    assert snapshot.content_hash == DIGEST
    assert snapshot.size_bytes == len(BODY)
    assert snapshot.content_type == TYPE
    assert snapshot.created


def test_existing_object_preserves_original_metadata(client: S3Client) -> None:
    params = {
        **PARAMS,
        "ContentType": "text/plain",
        "Metadata": {**META, "content-type": "text/plain"},
    }
    with Stubber(client) as stub:
        stub.add_client_error(
            "put_object",
            service_error_code="PreconditionFailed",
            http_status_code=412,
            expected_params=params,
        )
        stub.add_response(
            "head_object",
            {"ContentLength": len(BODY), "ContentType": TYPE, "Metadata": META},
            {"Bucket": BUCKET, "Key": KEY},
        )
        snapshot = SnapshotStore(client, BUCKET).put(BODY, content_type="text/plain")
        stub.assert_no_pending_responses()
    assert not snapshot.created
    assert snapshot.content_type == TYPE


@pytest.mark.parametrize("status", [403, 409, 500, 503])
def test_upload_failure_is_sanitized_and_not_mistaken_for_duplicate(
    client: S3Client, status: int
) -> None:
    with Stubber(client) as stub:
        stub.add_client_error(
            "put_object",
            service_error_code="StorageFailed",
            http_status_code=status,
            service_message="private-credential",
            expected_params=PARAMS,
        )
        with pytest.raises(SnapshotStorageError, match="Snapshot upload failed") as captured:
            SnapshotStore(client, BUCKET).put(BODY, content_type=TYPE)
        stub.assert_no_pending_responses()
    assert "private-credential" not in str(captured.value)
    assert captured.value.__suppress_context__


def test_existing_object_with_wrong_metadata_is_not_overwritten(client: S3Client) -> None:
    with Stubber(client) as stub:
        stub.add_client_error(
            "put_object",
            service_error_code="PreconditionFailed",
            http_status_code=412,
            expected_params=PARAMS,
        )
        stub.add_response(
            "head_object",
            {"ContentLength": 0, "ContentType": TYPE, "Metadata": META},
            {"Bucket": BUCKET, "Key": KEY},
        )
        with pytest.raises(SnapshotStorageError, match="metadata is inconsistent"):
            SnapshotStore(client, BUCKET).put(BODY, content_type=TYPE)
        stub.assert_no_pending_responses()


def test_head_failure_is_surfaced(client: S3Client) -> None:
    with Stubber(client) as stub:
        stub.add_client_error(
            "put_object",
            service_error_code="PreconditionFailed",
            http_status_code=412,
            expected_params=PARAMS,
        )
        stub.add_client_error(
            "head_object",
            service_error_code="AccessDenied",
            http_status_code=403,
            expected_params={"Bucket": BUCKET, "Key": KEY},
        )
        with pytest.raises(SnapshotStorageError, match="verification failed"):
            SnapshotStore(client, BUCKET).put(BODY, content_type=TYPE)


def test_sdk_has_explicit_timeouts_and_bounded_retries(client: S3Client) -> None:
    config = client.meta.config
    assert getattr(config, "connect_timeout") == 3
    assert getattr(config, "read_timeout") == 10
    assert getattr(config, "retries") == {"mode": "standard", "total_max_attempts": 3}


@pytest.mark.parametrize("content_type", ["", "text/plain\r\nSecret: a", "not-ascii-\u2603"])
def test_invalid_content_type_is_rejected_before_call(client: S3Client, content_type: str) -> None:
    with Stubber(client):
        with pytest.raises(ValueError, match="Content type"):
            SnapshotStore(client, BUCKET).put(BODY, content_type=content_type)
