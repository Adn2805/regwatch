import pytest
from pydantic import ValidationError

from app.core.config import LeaseSettings


def test_heartbeat_interval_is_one_third_of_lease() -> None:
    assert LeaseSettings(lease_duration_s=60).heartbeat_interval_s == 20


@pytest.mark.parametrize("value", [0, -1, float("nan"), float("inf")])
def test_invalid_lease_settings_fail_fast(value: float) -> None:
    with pytest.raises(ValidationError):
        LeaseSettings(lease_duration_s=value)
