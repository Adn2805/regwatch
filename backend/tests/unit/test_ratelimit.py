import pytest
from pydantic import ValidationError

from app.core.config import RedisSettings
from app.crawl.ratelimit import host_key, normalized_host


@pytest.mark.parametrize(
    ("host", "expected"),
    [
        ("WWW.Example.COM.", "www.example.com"),
        ("127.0.0.1", "127.0.0.1"),
        ("bücher.example", "xn--bcher-kva.example"),
        ("2001:db8::1", "2001:db8::1"),
    ],
)
def test_host_normalization(host: str, expected: str) -> None:
    assert normalized_host(host) == expected
    assert host_key("test", host) == host_key("test", expected)


@pytest.mark.parametrize("host", ["", "a/b", "user@host", "a:80", "bad host", "a..b", "-a.example"])
def test_invalid_host(host: str) -> None:
    with pytest.raises(ValueError):
        normalized_host(host)


@pytest.mark.parametrize(
    "settings",
    [
        {"rate_interval_s": 0},
        {"rate_interval_s": float("nan")},
        {"rate_capacity": 0},
        {"redis_timeout_s": 0},
    ],
)
def test_invalid_settings(settings: dict[str, float]) -> None:
    with pytest.raises(ValidationError):
        RedisSettings.model_validate(settings)
