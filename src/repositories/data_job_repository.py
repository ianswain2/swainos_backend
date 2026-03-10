from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import uuid4
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from croniter import croniter

from src.core.supabase import SupabaseClient
from src.schemas.data_jobs import (
    DataJob,
    DataJobHealth,
    DataJobRun,
    DataJobRunStep,
)


class DataJobRepository:
    def __init__(self) -> None:
        self.client = SupabaseClient()

    @staticmethod
    def _now_utc() -> datetime:
        return datetime.now(UTC)

    @staticmethod
    def _to_iso(value: datetime) -> str:
        return value.astimezone(UTC).isoformat()

    @staticmethod
    def _build_run_key(job_key: str) -> str:
        return f"{job_key}:{uuid4()}"

    @staticmethod
    def _resolve_timezone(timezone_value: str) -> ZoneInfo:
        normalized = (timezone_value or "UTC").strip()
        try:
            return ZoneInfo(normalized)
        except ZoneInfoNotFoundError:
            return ZoneInfo("UTC")

    def list_jobs(
        self,
        *,
        include_disabled: bool,
        limit: int,
        offset: int,
        include_totals: bool,
    ) -> tuple[list[DataJob], int]:
        filters: list[tuple[str, str]] = [("deleted_at", "is.null")]
        if not include_disabled:
            filters.append(("enabled", "eq.true"))
        rows, total_count = self.client.select(
            table="data_jobs",
            select=(
                "id,job_key,runner_key,display_name,job_kind,schedule_mode,enabled,schedule_cron,"
                "schedule_timezone,next_run_at,max_runtime_seconds,freshness_sla_minutes,"
                "stale_after_minutes,timeout_after_minutes,retry_backoff_minutes,owner,tags,config,"
                "created_at,updated_at"
            ),
            filters=filters,
            order="job_key.asc",
            limit=limit,
            offset=offset,
            count="exact" if include_totals else "planned",
        )
        jobs = [DataJob.model_validate(row) for row in rows]
        estimated_total = max(offset + len(jobs), len(jobs))
        return jobs, (total_count if total_count is not None else estimated_total)

    def get_job_by_key(self, job_key: str) -> DataJob | None:
        rows, _ = self.client.select(
            table="data_jobs",
            select=(
                "id,job_key,runner_key,display_name,job_kind,schedule_mode,enabled,schedule_cron,"
                "schedule_timezone,next_run_at,max_runtime_seconds,freshness_sla_minutes,"
                "stale_after_minutes,timeout_after_minutes,retry_backoff_minutes,owner,tags,config,"
                "created_at,updated_at"
            ),
            filters=[("deleted_at", "is.null"), ("job_key", f"eq.{job_key}")],
            limit=1,
        )
        if not rows:
            return None
        return DataJob.model_validate(rows[0])

    def update_job(self, job_id: str, payload: dict[str, Any]) -> DataJob | None:
        rows = self.client.update(
            table="data_jobs",
            payload={**payload, "updated_at": self._to_iso(self._now_utc())},
            filters=[("id", f"eq.{job_id}")],
        )
        if not rows:
            return None
        return DataJob.model_validate(rows[0])

    def list_job_dependencies(self, job_id: str) -> list[dict[str, Any]]:
        rows, _ = self.client.select(
            table="data_job_dependencies",
            select=(
                "id,job_id,depends_on_job_id,required,allow_stale_dependency,max_dependency_age_minutes,created_at"
            ),
            filters=[("job_id", f"eq.{job_id}")],
            order="created_at.asc",
            limit=200,
        )
        return rows

    def list_jobs_by_ids(self, ids: list[str]) -> list[DataJob]:
        if not ids:
            return []
        in_filter = ",".join(ids)
        rows, _ = self.client.select(
            table="data_jobs",
            select=(
                "id,job_key,runner_key,display_name,job_kind,schedule_mode,enabled,schedule_cron,"
                "schedule_timezone,next_run_at,max_runtime_seconds,freshness_sla_minutes,"
                "stale_after_minutes,timeout_after_minutes,retry_backoff_minutes,owner,tags,config,"
                "created_at,updated_at"
            ),
            filters=[("id", f"in.({in_filter})"), ("deleted_at", "is.null")],
            limit=max(len(ids), 1),
        )
        return [DataJob.model_validate(row) for row in rows]

    def list_runs_for_job(
        self,
        job_id: str,
        *,
        limit: int,
        offset: int,
        include_totals: bool,
    ) -> tuple[list[DataJobRun], int]:
        rows, total_count = self.client.select(
            table="data_job_runs",
            select=(
                "id,job_id,run_key,run_status,trigger_type,trigger_source,requested_by,requested_at,started_at,"
                "finished_at,blocked_reason,error_code,error_message,output,metadata,created_at,updated_at"
            ),
            filters=[("job_id", f"eq.{job_id}")],
            order="created_at.desc",
            limit=limit,
            offset=offset,
            count="exact" if include_totals else "planned",
        )
        runs = [DataJobRun.model_validate(row) for row in rows]
        estimated_total = max(offset + len(runs), len(runs))
        return runs, (total_count if total_count is not None else estimated_total)

    def get_run(self, run_id: str) -> DataJobRun | None:
        rows, _ = self.client.select(
            table="data_job_runs",
            select=(
                "id,job_id,run_key,run_status,trigger_type,trigger_source,requested_by,requested_at,started_at,"
                "finished_at,blocked_reason,error_code,error_message,output,metadata,created_at,updated_at"
            ),
            filters=[("id", f"eq.{run_id}")],
            limit=1,
        )
        if not rows:
            return None
        return DataJobRun.model_validate(rows[0])

    def list_run_steps(self, run_id: str) -> list[DataJobRunStep]:
        rows, _ = self.client.select(
            table="data_job_run_steps",
            select=(
                "id,run_id,step_key,step_name,step_order,status,started_at,finished_at,error_message,"
                "output,created_at,updated_at"
            ),
            filters=[("run_id", f"eq.{run_id}")],
            order="step_order.asc",
            limit=500,
        )
        return [DataJobRunStep.model_validate(row) for row in rows]

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
        now = self._now_utc()
        payload = {
            "job_id": job.id,
            "run_key": self._build_run_key(job.job_key),
            "run_status": status,
            "trigger_type": trigger_type,
            "trigger_source": trigger_source,
            "requested_by": requested_by,
            "requested_at": self._to_iso(now),
            "metadata": metadata,
            "blocked_reason": blocked_reason,
        }
        if status == "running":
            payload["started_at"] = self._to_iso(now)
        rows = self.client.insert(table="data_job_runs", payload=payload)
        return DataJobRun.model_validate(rows[0])

    def update_run(self, run_id: str, payload: dict[str, Any]) -> DataJobRun | None:
        rows = self.client.update(
            table="data_job_runs",
            payload={**payload, "updated_at": self._to_iso(self._now_utc())},
            filters=[("id", f"eq.{run_id}")],
        )
        if not rows:
            return None
        return DataJobRun.model_validate(rows[0])

    def create_run_step(
        self,
        *,
        run_id: str,
        step_key: str,
        step_name: str,
        step_order: int,
        status: str = "running",
        output: dict[str, Any] | None = None,
    ) -> DataJobRunStep:
        now = self._now_utc()
        rows = self.client.insert(
            table="data_job_run_steps",
            payload={
                "run_id": run_id,
                "step_key": step_key,
                "step_name": step_name,
                "step_order": step_order,
                "status": status,
                "started_at": self._to_iso(now),
                "output": output or {},
            },
        )
        return DataJobRunStep.model_validate(rows[0])

    def update_run_step(self, step_id: str, payload: dict[str, Any]) -> DataJobRunStep | None:
        rows = self.client.update(
            table="data_job_run_steps",
            payload={**payload, "updated_at": self._to_iso(self._now_utc())},
            filters=[("id", f"eq.{step_id}")],
        )
        if not rows:
            return None
        return DataJobRunStep.model_validate(rows[0])

    def list_due_jobs(self, *, max_jobs: int, now: datetime | None = None) -> list[DataJob]:
        now_value = now or self._now_utc()
        rows, _ = self.client.select(
            table="data_jobs",
            select=(
                "id,job_key,runner_key,display_name,job_kind,schedule_mode,enabled,schedule_cron,"
                "schedule_timezone,next_run_at,max_runtime_seconds,freshness_sla_minutes,"
                "stale_after_minutes,timeout_after_minutes,retry_backoff_minutes,owner,tags,config,"
                "created_at,updated_at"
            ),
            filters=[
                ("deleted_at", "is.null"),
                ("enabled", "eq.true"),
                ("schedule_mode", "eq.recurring"),
                ("next_run_at", f"lte.{self._to_iso(now_value)}"),
            ],
            order="next_run_at.asc",
            limit=max(max_jobs, 1),
        )
        return [DataJob.model_validate(row) for row in rows]

    def list_running_runs_for_job(self, job_id: str) -> list[DataJobRun]:
        rows, _ = self.client.select(
            table="data_job_runs",
            select=(
                "id,job_id,run_key,run_status,trigger_type,trigger_source,requested_by,requested_at,started_at,"
                "finished_at,blocked_reason,error_code,error_message,output,metadata,created_at,updated_at"
            ),
            filters=[("job_id", f"eq.{job_id}"), ("run_status", "eq.running")],
            limit=5,
        )
        return [DataJobRun.model_validate(row) for row in rows]

    def schedule_next_run(self, job: DataJob) -> DataJob | None:
        next_run = self.compute_next_run_at(
            job,
            from_value=max(job.next_run_at or self._now_utc(), self._now_utc()),
        )
        return self.update_job(
            job.id,
            payload={"next_run_at": self._to_iso(next_run) if next_run else None},
        )

    def set_next_run_at(self, job_id: str, next_run_at: datetime) -> DataJob | None:
        return self.update_job(
            job_id,
            payload={"next_run_at": self._to_iso(next_run_at)},
        )

    def compute_next_run_at(
        self,
        job: DataJob,
        *,
        from_value: datetime | None = None,
    ) -> datetime | None:
        if not job.schedule_cron:
            return None
        reference_utc = (from_value or self._now_utc()).astimezone(UTC)
        job_timezone = self._resolve_timezone(job.schedule_timezone)
        localized_reference = reference_utc.astimezone(job_timezone)
        try:
            itr = croniter(job.schedule_cron, localized_reference)
            next_local = itr.get_next(datetime)
            if next_local.tzinfo is None:
                next_local = next_local.replace(tzinfo=job_timezone)
            return next_local.astimezone(UTC)
        except ValueError:
            # Guardrail fallback for malformed cron expressions.
            return reference_utc + timedelta(minutes=60)

    def ensure_next_run_at_for_recurring_jobs(self) -> int:
        rows, _ = self.client.select(
            table="data_jobs",
            select=(
                "id,job_key,runner_key,display_name,job_kind,schedule_mode,enabled,schedule_cron,"
                "schedule_timezone,next_run_at,max_runtime_seconds,freshness_sla_minutes,"
                "stale_after_minutes,timeout_after_minutes,retry_backoff_minutes,owner,tags,config,"
                "created_at,updated_at"
            ),
            filters=[
                ("deleted_at", "is.null"),
                ("enabled", "eq.true"),
                ("schedule_mode", "eq.recurring"),
                ("next_run_at", "is.null"),
            ],
            order="job_key.asc",
            limit=500,
        )
        updated_count = 0
        for row in rows:
            job = DataJob.model_validate(row)
            next_run = self.compute_next_run_at(job, from_value=self._now_utc())
            if not next_run:
                continue
            updated = self.update_job(job.id, payload={"next_run_at": self._to_iso(next_run)})
            if updated:
                updated_count += 1
        return updated_count

    def list_health(self) -> list[DataJobHealth]:
        rows, _ = self.client.select(
            table="data_job_health_v1",
            select=(
                "job_id,job_key,display_name,job_kind,schedule_mode,enabled,schedule_cron,schedule_timezone,"
                "next_run_at,last_run_id,last_run_status,last_started_at,last_finished_at,last_duration_seconds,due_now"
            ),
            order="job_key.asc",
            limit=500,
        )
        return [DataJobHealth.model_validate(row) for row in rows]
