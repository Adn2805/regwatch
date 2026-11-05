from app.diff.hashing import content_hash


def test_known_sha256_vectors() -> None:
    assert content_hash(b"") == "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
    assert (
        content_hash(b"abc") == "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
    )


def test_hash_is_repeatable_and_sensitive_to_raw_bytes() -> None:
    assert content_hash(b"notice") == content_hash(b"notice")
    assert content_hash(b"notice") != content_hash(b"notice\n")
    assert content_hash(b"\xff\x00") != content_hash(b"\x00\xff")
