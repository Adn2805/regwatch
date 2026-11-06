import pytest
from pydantic import ValidationError

from app.core.config import WorkerSettings


def test_scheduler_is_disabled_by_default() -> None:
    assert WorkerSettings().scheduler_enabled is False


@pytest.mark.parametrize(
    "values",
    [
        {"worker_max_jobs": 0},
        {"scheduler_poll_s": 0},
        {"worker_job_timeout_s": float("inf")},
        {"queue_name": "invalid queue"},
    ],
)
def test_bad_worker_settings_fail_fast(values: dict[str, object]) -> None:
    with pytest.raises(ValidationError):
        WorkerSettings.model_validate(values)
