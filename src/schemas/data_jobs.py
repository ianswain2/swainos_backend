from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import Field

from src.shared.base import BaseSchema

DataJobScheduleMode = Literal["recurring", "manual_only", "backfill_only", "system_managed"]
DataJobRunStatus = Literal["queued", "running", "success", "failed", "blocked", "cancelled"]
DataJobKind = Literal[
    "source_ingestion",
    "rollup_refresh",
    "derived_compute",
    "manual_import",
    "maintenance",
]
DataJobTriggerType = Literal["manual", "scheduler", "system"]


class DataJob(BaseSchema):
    id: str
    job_key: str
    runner_key: str
    display_name: str
    job_kind: DataJobKind
    schedule_mode: DataJobScheduleMode
    enabled: bool
    schedule_cron: str | None = None
    schedule_timezone: str
    next_run_at: datetime | None = None
    max_runtime_seconds: int = 3600
    freshness_sla_minutes: int | None = None
    stale_after_minutes: int | None = None
    timeout_after_minutes: int | None = None
    retry_backoff_minutes: int = 30
    owner: str | None = None
    tags: list[str] = Field(default_factory=list)
    config: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime
    updated_at: datetime


class DataJobDependency(BaseSchema):
    job_key: str
    depends_on_job_key: str
    required: bool = True
    allow_stale_dependency: bool = False
    max_dependency_age_minutes: int | None = None


class DataJobRun(BaseSchema):
    id: str
    job_id: str
    run_key: str
    run_status: DataJobRunStatus
    trigger_type: DataJobTriggerType
    trigger_source: str | None = None
    requested_by: str | None = None
    requested_at: datetime
    started_at: datetime | None = None
    finished_at: datetime | None = None
    blocked_reason: str | None = None
    error_code: str | None = None
    error_message: str | None = None
    output: dict[str, Any] = Field(default_factory=dict)
    metadata: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime
    updated_at: datetime


class DataJobRunStep(BaseSchema):
    id: str
    run_id: str
    step_key: str
    step_name: str
    step_order: int
    status: DataJobRunStatus
    started_at: datetime | None = None
    finished_at: datetime | None = None
    error_message: str | None = None
    output: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime
    updated_at: datetime


class DataJobHealth(BaseSchema):
    job_id: str
    job_key: str
    display_name: str
    job_kind: DataJobKind
    schedule_mode: DataJobScheduleMode
    enabled: bool
    schedule_cron: str | None = None
    schedule_timezone: str
    next_run_at: datetime | None = None
    last_run_id: str | None = None
    last_run_status: DataJobRunStatus | None = None
    last_started_at: datetime | None = None
    last_finished_at: datetime | None = None
    last_duration_seconds: int | None = None
    due_now: bool = False


class DataJobUpdateRequest(BaseSchema):
    enabled: bool | None = None
    schedule_cron: str | None = None
    schedule_timezone: str | None = None
    next_run_at: datetime | None = None
    max_runtime_seconds: int | None = Field(default=None, ge=30, le=86400)
    freshness_sla_minutes: int | None = Field(default=None, ge=1, le=10080)
    stale_after_minutes: int | None = Field(default=None, ge=1, le=10080)
    timeout_after_minutes: int | None = Field(default=None, ge=1, le=10080)
    retry_backoff_minutes: int | None = Field(default=None, ge=0, le=10080)


class DataJobRunRequest(BaseSchema):
    trigger_type: DataJobTriggerType = "manual"
    trigger_source: str | None = None
    requested_by: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class DataJobRunDetail(BaseSchema):
    run: DataJobRun
    steps: list[DataJobRunStep] = Field(default_factory=list)


class DataJobDueResult(BaseSchema):
    selected_job_keys: list[str] = Field(default_factory=list)
    dispatched_run_ids: list[str] = Field(default_factory=list)
    blocked_job_keys: list[str] = Field(default_factory=list)
    skipped_job_keys: list[str] = Field(default_factory=list)
