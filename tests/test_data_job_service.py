from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

import httpx

from src.schemas.data_jobs import DataJob, DataJobRun, DataJobRunRequest
from src.services.data_job_service import DataJobService
from src.services.job_runners.base import RunnerResult


class _StubRunner:
    def run(self, job_key: str, run_id: str, metadata: dict[str, Any]) -> RunnerResult:
        return RunnerResult(status="success", message="ok", output={})


class _FakeRepository:
    def __init__(self, job: DataJob) -> None:
        self.job = job
        self.running_runs: list[DataJobRun] = []
        self.created_runs: list[DataJobRun] = []
        self.runs_for_job: list[DataJobRun] = []
        self.schedule_next_calls = 0
        self.set_next_run_calls: list[datetime] = []
        self.raise_running_conflict = False

    def get_job_by_key(self, job_key: str) -> DataJob | None:
        return self.job if job_key == self.job.job_key else None

    def ensure_next_run_at_for_recurring_jobs(self) -> int:
        return 0

    def list_due_jobs(self, *, max_jobs: int, now: datetime | None = None) -> list[DataJob]:
        return [self.job][:max_jobs]

    def list_running_runs_for_job(self, job_id: str) -> list[DataJobRun]:
        return [
            run
            for run in self.running_runs
            if run.job_id == job_id and run.run_status == "running"
        ]

    def update_run(self, run_id: str, payload: dict[str, Any]) -> DataJobRun | None:
        all_runs = [*self.running_runs, *self.created_runs]
        for run in all_runs:
            if run.id != run_id:
                continue
            updates = dict(payload)
            updates.setdefault("updated_at", datetime.now(UTC))
            updates.pop("id", None)
            updated = run.model_copy(update=updates)
            if run in self.running_runs:
                self.running_runs[self.running_runs.index(run)] = updated
            if run in self.created_runs:
                self.created_runs[self.created_runs.index(run)] = updated
            return updated
        return None

    def create_run(
        self,
        *,
        job: DataJob,
        trigger_type: str,
        trigger_source: str | None,
        requested_by: str | None,
        metadata: dict[str, Any],
        status: str = "queued",
        blocked_reason: str | None = None,
    ) -> DataJobRun:
        if status == "running" and self.raise_running_conflict:
            request = httpx.Request("POST", "https://example.com")
            response = httpx.Response(
                409,
                request=request,
                text='{"message":"duplicate key value violates unique constraint '
                '\\"idx_data_job_runs_single_running_per_job\\""}',
            )
            raise httpx.HTTPStatusError("conflict", request=request, response=response)
        run = _make_run(
            run_id=f"run-{len(self.created_runs) + 1}",
            job_id=job.id,
            status=status,
            trigger_type=trigger_type,
            started_at=datetime.now(UTC) if status == "running" else None,
            blocked_reason=blocked_reason,
        )
        self.created_runs.append(run)
        return run

    def create_run_step(
        self,
        *,
        run_id: str,
        step_key: str,
        step_name: str,
        step_order: int,
        status: str = "running",
        output: dict[str, Any] | None = None,
    ) -> Any:
        class _Step:
            id = "step-1"

        return _Step()

    def update_run_step(self, step_id: str, payload: dict[str, Any]) -> Any:
        return None

    def list_job_dependencies(self, job_id: str) -> list[dict[str, Any]]:
        return []

    def list_jobs_by_ids(self, ids: list[str]) -> list[DataJob]:
        return []

    def list_runs_for_job(
        self,
        job_id: str,
        *,
        limit: int,
        offset: int,
        include_totals: bool,
    ) -> tuple[list[DataJobRun], int]:
        if self.runs_for_job:
            return (self.runs_for_job[:limit], len(self.runs_for_job))
        return ([], 0)

    def schedule_next_run(self, job: DataJob) -> DataJob | None:
        self.schedule_next_calls += 1
        return job

    def set_next_run_at(self, job_id: str, next_run_at: datetime) -> DataJob | None:
        self.set_next_run_calls.append(next_run_at)
        self.job = self.job.model_copy(update={"next_run_at": next_run_at})
        return self.job


def _make_job(*, max_runtime_seconds: int = 3600) -> DataJob:
    now = datetime.now(UTC)
    return DataJob(
        id="job-1",
        job_key="test-job",
        runner_key="test.runner",
        display_name="Test Job",
        job_kind="maintenance",
        schedule_mode="recurring",
        enabled=True,
        schedule_cron="*/15 * * * *",
        schedule_timezone="UTC",
        next_run_at=now,
        max_runtime_seconds=max_runtime_seconds,
        freshness_sla_minutes=None,
        stale_after_minutes=None,
        timeout_after_minutes=None,
        retry_backoff_minutes=30,
        owner=None,
        tags=[],
        config={},
        created_at=now,
        updated_at=now,
    )


def _make_run(
    *,
    run_id: str,
    job_id: str,
    status: str,
    trigger_type: str = "scheduler",
    started_at: datetime | None = None,
    blocked_reason: str | None = None,
) -> DataJobRun:
    now = datetime.now(UTC)
    return DataJobRun(
        id=run_id,
        job_id=job_id,
        run_key=f"{job_id}:{run_id}",
        run_status=status,
        trigger_type=trigger_type,
        trigger_source="test",
        requested_by="test",
        requested_at=now,
        started_at=started_at,
        finished_at=None,
        blocked_reason=blocked_reason,
        error_code=None,
        error_message=None,
        output={},
        metadata={},
        created_at=now,
        updated_at=now,
    )


def test_scheduler_marks_stale_running_run_failed_before_dispatch() -> None:
    job = _make_job(max_runtime_seconds=60)
    repository = _FakeRepository(job)
    repository.running_runs = [
        _make_run(
            run_id="stale-1",
            job_id=job.id,
            status="running",
            started_at=datetime.now(UTC) - timedelta(seconds=600),
        )
    ]
    service = DataJobService(repository=repository, runner_registry={"test.runner": _StubRunner()})

    run = service.run_job(
        job.job_key,
        DataJobRunRequest(
            trigger_type="scheduler",
            trigger_source="test",
            requested_by="test",
            metadata={},
        ),
    )

    assert run.run_status == "success"
    assert repository.running_runs[0].run_status == "failed"
    assert repository.running_runs[0].error_code == "runner_timeout"
    assert repository.schedule_next_calls == 1


def test_scheduler_conflict_returns_blocked_and_reschedules() -> None:
    job = _make_job()
    repository = _FakeRepository(job)
    repository.raise_running_conflict = True
    service = DataJobService(repository=repository, runner_registry={"test.runner": _StubRunner()})

    run = service.run_job(
        job.job_key,
        DataJobRunRequest(
            trigger_type="scheduler",
            trigger_source="test",
            requested_by="test",
            metadata={},
        ),
    )

    assert run.run_status == "blocked"
    assert run.blocked_reason == "Job already has an active run"
    assert repository.schedule_next_calls == 1


def test_run_due_jobs_skips_failed_jobs_until_backoff_elapsed() -> None:
    job = _make_job()
    repository = _FakeRepository(job)
    repository.runs_for_job = [
        _make_run(
            run_id="failed-1",
            job_id=job.id,
            status="failed",
            started_at=datetime.now(UTC) - timedelta(minutes=2),
        ).model_copy(
            update={
                "finished_at": datetime.now(UTC) - timedelta(minutes=1),
            }
        )
    ]
    service = DataJobService(repository=repository, runner_registry={"test.runner": _StubRunner()})

    result = service.run_due_jobs(max_jobs=5, trigger_source="test")

    assert result.selected_job_keys == [job.job_key]
    assert result.skipped_job_keys == [job.job_key]
    assert result.dispatched_run_ids == []
    assert len(repository.set_next_run_calls) == 1
    assert repository.created_runs == []
