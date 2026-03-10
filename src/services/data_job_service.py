from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx

from src.core.errors import BadRequestError, NotFoundError
from src.repositories.data_job_repository import DataJobRepository
from src.schemas.data_jobs import (
    DataJob,
    DataJobDependency,
    DataJobDueResult,
    DataJobHealth,
    DataJobRun,
    DataJobRunDetail,
    DataJobRunFeedEntry,
    DataJobRunRequest,
    DataJobRunStatus,
    DataJobUpdateRequest,
)
from src.services.job_runners.base import DataJobRunner
from src.services.job_runners.registry import build_default_runner_registry


class DataJobService:
    def __init__(
        self,
        repository: DataJobRepository,
        runner_registry: dict[str, DataJobRunner] | None = None,
    ) -> None:
        self.repository = repository
        self.runner_registry = runner_registry or build_default_runner_registry()

    def list_jobs(
        self,
        *,
        include_disabled: bool,
        page: int,
        page_size: int,
        include_totals: bool,
    ) -> tuple[list[DataJob], int]:
        offset = max(page - 1, 0) * page_size
        return self.repository.list_jobs(
            include_disabled=include_disabled,
            limit=page_size,
            offset=offset,
            include_totals=include_totals,
        )

    def get_job(self, job_key: str) -> tuple[DataJob, list[DataJobDependency]]:
        job = self.repository.get_job_by_key(job_key)
        if not job:
            raise NotFoundError("Data job not found")
        deps = self._get_dependencies(job)
        return job, deps

    def update_job(self, job_key: str, payload: DataJobUpdateRequest) -> DataJob:
        job = self.repository.get_job_by_key(job_key)
        if not job:
            raise NotFoundError("Data job not found")
        update_payload = payload.model_dump(exclude_none=True, by_alias=False)
        if not update_payload:
            return job
        updated = self.repository.update_job(job.id, update_payload)
        if not updated:
            raise NotFoundError("Data job not found")
        return updated

    def list_runs_for_job(
        self,
        job_key: str,
        *,
        page: int,
        page_size: int,
        include_totals: bool,
    ) -> tuple[DataJob, list[DataJobRun], int]:
        job = self.repository.get_job_by_key(job_key)
        if not job:
            raise NotFoundError("Data job not found")
        offset = max(page - 1, 0) * page_size
        runs, total = self.repository.list_runs_for_job(
            job.id,
            limit=page_size,
            offset=offset,
            include_totals=include_totals,
        )
        return job, runs, total

    def list_runs_feed(
        self,
        *,
        page: int,
        page_size: int,
        include_totals: bool,
        job_key: str | None,
        run_status: DataJobRunStatus | None,
    ) -> tuple[list[DataJobRunFeedEntry], int]:
        job_id: str | None = None
        if job_key:
            job = self.repository.get_job_by_key(job_key)
            if not job:
                raise NotFoundError("Data job not found")
            job_id = job.id
        offset = max(page - 1, 0) * page_size
        runs, total = self.repository.list_runs_feed(
            job_id=job_id,
            run_status=run_status,
            limit=page_size,
            offset=offset,
            include_totals=include_totals,
        )
        job_map = {
            job.id: job
            for job in self.repository.list_jobs_by_ids(
                list({run.job_id for run in runs if run.job_id})
            )
        }
        entries: list[DataJobRunFeedEntry] = []
        for run in runs:
            job = job_map.get(run.job_id)
            entries.append(
                DataJobRunFeedEntry(
                    **run.model_dump(),
                    job_key=job.job_key if job else "unknown",
                    display_name=job.display_name if job else "Unknown Job",
                )
            )
        return entries, total

    def get_run_detail(self, run_id: str) -> DataJobRunDetail:
        run = self.repository.get_run(run_id)
        if not run:
            raise NotFoundError("Data run not found")
        steps = self.repository.list_run_steps(run.id)
        return DataJobRunDetail(run=run, steps=steps)

    def list_health(self) -> list[DataJobHealth]:
        return self.repository.list_health()

    def run_job(self, job_key: str, payload: DataJobRunRequest) -> DataJobRun:
        job = self.repository.get_job_by_key(job_key)
        if not job:
            raise NotFoundError("Data job not found")
        if not job.enabled:
            raise BadRequestError("Job is disabled")

        active_runs = self.repository.list_running_runs_for_job(job.id)
        self._expire_stale_active_runs(job, active_runs)
        active_runs = self.repository.list_running_runs_for_job(job.id)
        if active_runs:
            blocked_run = self.repository.create_run(
                job=job,
                trigger_type=payload.trigger_type,
                trigger_source=payload.trigger_source,
                requested_by=payload.requested_by,
                metadata=payload.metadata,
                status="blocked",
                blocked_reason="Job already has an active run",
            )
            if payload.trigger_type == "scheduler":
                self.repository.schedule_next_run(job)
            return blocked_run

        dependency_block = self._find_dependency_block(job)
        if dependency_block:
            blocked_run = self.repository.create_run(
                job=job,
                trigger_type=payload.trigger_type,
                trigger_source=payload.trigger_source,
                requested_by=payload.requested_by,
                metadata=payload.metadata,
                status="blocked",
                blocked_reason=dependency_block,
            )
            if payload.trigger_type == "scheduler":
                self.repository.schedule_next_run(job)
            return blocked_run

        try:
            run = self.repository.create_run(
                job=job,
                trigger_type=payload.trigger_type,
                trigger_source=payload.trigger_source,
                requested_by=payload.requested_by,
                metadata=payload.metadata,
                status="running",
            )
        except httpx.HTTPStatusError as exc:
            if self._is_single_running_conflict(exc):
                blocked_run = self.repository.create_run(
                    job=job,
                    trigger_type=payload.trigger_type,
                    trigger_source=payload.trigger_source,
                    requested_by=payload.requested_by,
                    metadata=payload.metadata,
                    status="blocked",
                    blocked_reason="Job already has an active run",
                )
                if payload.trigger_type == "scheduler":
                    self.repository.schedule_next_run(job)
                return blocked_run
            raise
        step = self.repository.create_run_step(
            run_id=run.id,
            step_key=job.runner_key,
            step_name=job.display_name,
            step_order=1,
            status="running",
            output={},
        )
        runner = self.runner_registry.get(job.runner_key)
        if runner is None:
            self.repository.update_run_step(
                step.id,
                {
                    "status": "failed",
                    "finished_at": self._now_iso(),
                    "error_message": f"No runner registered for {job.runner_key}",
                    "output": {},
                },
            )
            failed = self.repository.update_run(
                run.id,
                {
                    "run_status": "failed",
                    "finished_at": self._now_iso(),
                    "error_code": "runner_not_registered",
                    "error_message": f"No runner registered for {job.runner_key}",
                    "duration_seconds": self._duration_seconds(run.started_at),
                    "output_size_bytes": self._estimate_output_size_bytes({}),
                },
            )
            if not failed:
                raise NotFoundError("Data run not found")
            return failed

        result = runner.run(job_key=job.job_key, run_id=run.id, metadata=payload.metadata)
        step_status = "success" if result.status == "success" else "failed"
        self.repository.update_run_step(
            step.id,
            {
                "status": step_status,
                "finished_at": self._now_iso(),
                "error_message": None if step_status == "success" else result.message,
                "output": result.output,
            },
        )
        update_payload: dict[str, Any] = {
            "run_status": "success" if result.status == "success" else "failed",
            "finished_at": self._now_iso(),
            "output": result.output,
            "error_message": None if result.status == "success" else result.message,
            "error_code": None if result.status == "success" else "runner_failed",
            "duration_seconds": self._duration_seconds(run.started_at),
            "output_size_bytes": self._estimate_output_size_bytes(result.output),
        }
        updated = self.repository.update_run(run.id, update_payload)
        if not updated:
            raise NotFoundError("Data run not found")
        if payload.trigger_type == "scheduler":
            self.repository.schedule_next_run(job)
        return updated

    def run_due_jobs(self, *, max_jobs: int, trigger_source: str) -> DataJobDueResult:
        # Bootstrap recurring jobs that have schedule metadata but no next_run_at yet.
        self.repository.ensure_next_run_at_for_recurring_jobs()
        due_jobs = self.repository.list_due_jobs(max_jobs=max_jobs)
        selected: list[str] = []
        dispatched: list[str] = []
        blocked: list[str] = []
        skipped: list[str] = []
        for job in due_jobs:
            selected.append(job.job_key)
            backoff_until = self._retry_backoff_until(job)
            if backoff_until is not None:
                self.repository.set_next_run_at(job.id, backoff_until)
                skipped.append(job.job_key)
                continue
            run = self.run_job(
                job.job_key,
                DataJobRunRequest(
                    trigger_type="scheduler",
                    trigger_source=trigger_source,
                    requested_by="system:scheduler",
                    metadata={},
                ),
            )
            if run.run_status in {"success", "running", "queued"}:
                dispatched.append(run.id)
            elif run.run_status == "blocked":
                blocked.append(job.job_key)
            else:
                skipped.append(job.job_key)
        return DataJobDueResult(
            selected_job_keys=selected,
            dispatched_run_ids=dispatched,
            blocked_job_keys=blocked,
            skipped_job_keys=skipped,
        )

    def _find_dependency_block(self, job: DataJob) -> str | None:
        dependencies = self._get_dependencies(job)
        for dependency in dependencies:
            dep_job = self.repository.get_job_by_key(dependency.depends_on_job_key)
            if not dep_job:
                return f"Dependency job missing: {dependency.depends_on_job_key}"
            runs, _ = self.repository.list_runs_for_job(
                dep_job.id,
                limit=1,
                offset=0,
                include_totals=False,
            )
            if not runs:
                return f"Dependency has no successful run yet: {dependency.depends_on_job_key}"
            latest = runs[0]
            if latest.run_status != "success":
                return f"Dependency latest run is not successful: {dependency.depends_on_job_key}"
            if dependency.max_dependency_age_minutes and latest.finished_at:
                age = datetime.now(UTC) - latest.finished_at
                if age.total_seconds() > dependency.max_dependency_age_minutes * 60:
                    return f"Dependency is stale: {dependency.depends_on_job_key}"
        return None

    def _get_dependencies(self, job: DataJob) -> list[DataJobDependency]:
        rows = self.repository.list_job_dependencies(job.id)
        depends_on_ids = [
            str(row.get("depends_on_job_id")) for row in rows if row.get("depends_on_job_id")
        ]
        job_map = {dep.id: dep for dep in self.repository.list_jobs_by_ids(depends_on_ids)}
        dependencies: list[DataJobDependency] = []
        for row in rows:
            dep_id = str(row.get("depends_on_job_id"))
            dep_job = job_map.get(dep_id)
            if not dep_job:
                continue
            dependencies.append(
                DataJobDependency(
                    job_key=job.job_key,
                    depends_on_job_key=dep_job.job_key,
                    required=bool(row.get("required", True)),
                    allow_stale_dependency=bool(row.get("allow_stale_dependency", False)),
                    max_dependency_age_minutes=row.get("max_dependency_age_minutes"),
                )
            )
        return dependencies

    @staticmethod
    def _now_iso() -> str:
        return datetime.now(UTC).isoformat()

    @staticmethod
    def _is_single_running_conflict(error: httpx.HTTPStatusError) -> bool:
        if error.response.status_code != 409:
            return False
        body = (error.response.text or "").lower()
        return "idx_data_job_runs_single_running_per_job" in body or (
            "duplicate key value violates unique constraint" in body and "data_job_runs" in body
        )

    def _expire_stale_active_runs(self, job: DataJob, active_runs: list[DataJobRun]) -> None:
        now = datetime.now(UTC)
        timeout_seconds = max(job.max_runtime_seconds, 60)
        for run in active_runs:
            if not run.started_at:
                continue
            elapsed_seconds = (now - run.started_at).total_seconds()
            if elapsed_seconds <= timeout_seconds:
                continue
            self.repository.update_run(
                run.id,
                {
                    "run_status": "failed",
                    "finished_at": self._now_iso(),
                    "error_code": "runner_timeout",
                    "error_message": (
                        "Marked failed after exceeding "
                        f"max_runtime_seconds ({job.max_runtime_seconds})."
                    ),
                    "duration_seconds": int(elapsed_seconds),
                },
            )

    def _retry_backoff_until(self, job: DataJob) -> datetime | None:
        if job.retry_backoff_minutes <= 0:
            return None
        runs, _ = self.repository.list_runs_for_job(
            job.id,
            limit=1,
            offset=0,
            include_totals=False,
        )
        if not runs:
            return None
        latest = runs[0]
        if latest.run_status != "failed":
            return None
        reference = latest.finished_at or latest.created_at
        backoff_until = reference + timedelta(minutes=job.retry_backoff_minutes)
        if datetime.now(UTC) < backoff_until:
            return backoff_until
        return None

    @staticmethod
    def _duration_seconds(started_at: datetime | None) -> int | None:
        if not started_at:
            return None
        elapsed = (datetime.now(UTC) - started_at).total_seconds()
        if elapsed < 0:
            return None
        return int(round(elapsed))

    @staticmethod
    def _estimate_output_size_bytes(output: Any) -> int:
        try:
            serialized = json.dumps(output, separators=(",", ":"), default=str)
        except (TypeError, ValueError):
            serialized = str(output)
        return len(serialized.encode("utf-8"))
