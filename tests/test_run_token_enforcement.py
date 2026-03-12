from __future__ import annotations

from typing import Any

from fastapi.testclient import TestClient

from src.api.authz import get_current_user_access
from src.api.dependencies import get_ai_insights_service, get_data_job_service, get_fx_service
from src.core.config import get_settings
from src.core.rate_limit import rate_limiter
from src.main import create_app
from src.schemas.auth_access import AuthenticatedUserAccess
from src.schemas.data_jobs import DataJobDueResult
from src.schemas.fx import FxManualRunResult


class _FakeAiInsightsService:
    def run_manual_generation(self, trigger: str = "manual") -> dict[str, Any]:
        _ = trigger
        return {"status": "ok"}


class _FakeFxService:
    def run_signals(self, request: Any) -> FxManualRunResult:
        _ = request
        return FxManualRunResult(
            run_id="run-1",
            status="success",
            records_processed=1,
            records_created=1,
            message="ok",
        )


class _FakeDataJobService:
    def run_due_jobs(self, *, max_jobs: int, trigger_source: str) -> DataJobDueResult:
        _ = max_jobs, trigger_source
        return DataJobDueResult(
            selected_job_keys=["job-1"],
            dispatched_run_ids=["run-1"],
            blocked_job_keys=[],
            skipped_job_keys=[],
        )


def _client() -> TestClient:
    app = create_app()
    app.dependency_overrides[get_ai_insights_service] = _FakeAiInsightsService
    app.dependency_overrides[get_fx_service] = _FakeFxService
    app.dependency_overrides[get_data_job_service] = _FakeDataJobService
    app.dependency_overrides[get_current_user_access] = lambda: AuthenticatedUserAccess(
        user_id="test-admin-id",
        email="test-admin@example.com",
        role="admin",
        is_admin=True,
        is_active=True,
        permission_keys=["ai_insights", "fx_command", "settings_job_controls"],
        can_manage_access=True,
    )
    return TestClient(app)


def test_ai_manual_run_requires_valid_token(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    settings = get_settings()
    monkeypatch.setattr(settings, "ai_manual_run_token", "ai-secret", raising=False)
    client = _client()

    missing = client.post("/api/v1/ai-insights/run")
    assert missing.status_code == 401
    invalid = client.post("/api/v1/ai-insights/run", headers={"x-ai-run-token": "bad"})
    assert invalid.status_code == 401
    valid = client.post("/api/v1/ai-insights/run", headers={"x-ai-run-token": "ai-secret"})
    assert valid.status_code == 200


def test_fx_run_requires_valid_token(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    settings = get_settings()
    monkeypatch.setattr(settings, "fx_manual_run_token", "fx-secret", raising=False)
    client = _client()

    body = {"runType": "manual"}
    missing = client.post("/api/v1/fx/signals/run", json=body)
    assert missing.status_code == 401
    invalid = client.post(
        "/api/v1/fx/signals/run",
        json=body,
        headers={"x-fx-run-token": "bad"},
    )
    assert invalid.status_code == 401
    valid = client.post(
        "/api/v1/fx/signals/run",
        json=body,
        headers={"x-fx-run-token": "fx-secret"},
    )
    assert valid.status_code == 200


def test_scheduler_tick_requires_valid_token(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    settings = get_settings()
    monkeypatch.setattr(settings, "data_jobs_scheduler_token", "scheduler-secret", raising=False)
    client = _client()

    missing = client.post("/api/v1/data-jobs/scheduler/tick")
    assert missing.status_code == 401
    invalid = client.post(
        "/api/v1/data-jobs/scheduler/tick",
        headers={"x-scheduler-token": "bad"},
    )
    assert invalid.status_code == 401
    valid = client.post(
        "/api/v1/data-jobs/scheduler/tick",
        headers={"x-scheduler-token": "scheduler-secret"},
    )
    assert valid.status_code == 200


def test_run_endpoint_rate_limit_enforced(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    settings = get_settings()
    rate_limiter.reset()
    monkeypatch.setattr(settings, "ai_manual_run_token", "ai-secret", raising=False)
    monkeypatch.setattr(settings, "expensive_run_rate_limit_per_minute", 1, raising=False)
    client = _client()

    first = client.post("/api/v1/ai-insights/run", headers={"x-ai-run-token": "ai-secret"})
    second = client.post("/api/v1/ai-insights/run", headers={"x-ai-run-token": "ai-secret"})
    assert first.status_code == 200
    assert second.status_code == 429
    rate_limiter.reset()


def test_non_production_allows_missing_run_token_when_unconfigured(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    settings = get_settings()
    monkeypatch.setattr(settings, "environment", "development", raising=False)
    monkeypatch.setattr(settings, "ai_manual_run_token", None, raising=False)
    rate_limiter.reset()
    client = _client()

    response = client.post("/api/v1/ai-insights/run")
    assert response.status_code == 200
