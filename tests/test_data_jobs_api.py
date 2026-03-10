from __future__ import annotations

from datetime import UTC, datetime

from fastapi.testclient import TestClient

from src.api.dependencies import get_data_job_service
from src.main import create_app
from src.schemas.data_jobs import DataJobRunFeedEntry


class FakeDataJobService:
    def list_runs_feed(
        self,
        *,
        page: int,
        page_size: int,
        include_totals: bool,
        job_key: str | None,
        run_status: str | None,
    ) -> tuple[list[DataJobRunFeedEntry], int]:
        _ = page, page_size, include_totals, job_key, run_status
        now = datetime.now(UTC)
        return (
            [
                DataJobRunFeedEntry(
                    id="run-1",
                    job_id="job-1",
                    job_key="fx-rates-pull",
                    display_name="FX Rates Pull",
                    run_key="fx-rates-pull:run-1",
                    run_status="success",
                    trigger_type="scheduler",
                    trigger_source="scheduler_tick",
                    requested_by="system:scheduler",
                    requested_at=now,
                    started_at=now,
                    finished_at=now,
                    blocked_reason=None,
                    error_code=None,
                    error_message=None,
                    output={},
                    metadata={},
                    created_at=now,
                    updated_at=now,
                )
            ],
            1,
        )


def _client() -> TestClient:
    app = create_app()
    app.dependency_overrides[get_data_job_service] = FakeDataJobService
    return TestClient(app)


def test_data_jobs_run_feed_returns_enveloped_rows() -> None:
    client = _client()
    response = client.get("/api/v1/data-jobs/run-feed?page=1&page_size=25")
    assert response.status_code == 200
    payload = response.json()
    assert payload["data"][0]["jobKey"] == "fx-rates-pull"
    assert payload["data"][0]["runStatus"] == "success"
    assert payload["meta"]["source"] == "data_job_runs"


def test_data_jobs_run_feed_rejects_invalid_status_filter() -> None:
    client = _client()
    response = client.get("/api/v1/data-jobs/run-feed?run_status=not_a_status")
    assert response.status_code == 422
