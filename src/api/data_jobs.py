from __future__ import annotations

from datetime import date

from fastapi import APIRouter, Depends, Header, Query
from fastapi.concurrency import run_in_threadpool

from src.api.dependencies import get_data_job_service
from src.core.config import get_settings
from src.core.errors import BadRequestError
from src.schemas.data_jobs import (
    DataJob,
    DataJobHealth,
    DataJobRun,
    DataJobRunFeedEntry,
    DataJobRunRequest,
    DataJobRunStatus,
    DataJobUpdateRequest,
)
from src.services.data_job_service import DataJobService
from src.shared.response import Meta, ResponseEnvelope, build_pagination

router = APIRouter(prefix="/data-jobs", tags=["data-jobs"])
DATA_JOB_SERVICE_DEP = Depends(get_data_job_service)
RUN_STATUS_QUERY = Query(default=None)


def _meta(source: str) -> Meta:
    return Meta(
        as_of_date=date.today().isoformat(),
        source=source,
        time_window="",
        calculation_version="v1",
        data_status="live",
        is_stale=False,
        degraded=False,
    )


@router.get("")
async def data_jobs_list(
    include_disabled: bool = Query(default=True),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=200),
    include_totals: bool = Query(default=False),
    service: DataJobService = DATA_JOB_SERVICE_DEP,
) -> ResponseEnvelope[list[DataJob]]:
    jobs, total = await run_in_threadpool(
        service.list_jobs,
        include_disabled=include_disabled,
        page=page,
        page_size=page_size,
        include_totals=include_totals,
    )
    return ResponseEnvelope(
        data=jobs,
        pagination=build_pagination(page=page, page_size=page_size, total_items=total),
        meta=_meta("data_jobs"),
    )


@router.get("/health")
async def data_jobs_health(
    service: DataJobService = DATA_JOB_SERVICE_DEP,
) -> ResponseEnvelope[list[DataJobHealth]]:
    rows = await run_in_threadpool(service.list_health)
    return ResponseEnvelope(data=rows, pagination=None, meta=_meta("data_job_health_v1"))


@router.post("/scheduler/tick")
async def data_jobs_scheduler_tick(
    max_jobs: int = Query(default=5, ge=1, le=50),
    x_scheduler_token: str | None = Header(default=None),
    service: DataJobService = DATA_JOB_SERVICE_DEP,
):
    configured = (get_settings().data_jobs_scheduler_token or "").strip()
    if configured and x_scheduler_token != configured:
        raise BadRequestError("Invalid scheduler token")
    result = await run_in_threadpool(
        service.run_due_jobs,
        max_jobs=max_jobs,
        trigger_source="scheduler_tick",
    )
    return ResponseEnvelope(data=result, pagination=None, meta=_meta("scheduler_tick"))


@router.get("/run-feed")
async def data_jobs_run_feed(
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=200),
    include_totals: bool = Query(default=False),
    job_key: str | None = Query(default=None),
    run_status: DataJobRunStatus | None = RUN_STATUS_QUERY,
    service: DataJobService = DATA_JOB_SERVICE_DEP,
) -> ResponseEnvelope[list[DataJobRunFeedEntry]]:
    runs, total = await run_in_threadpool(
        service.list_runs_feed,
        page=page,
        page_size=page_size,
        include_totals=include_totals,
        job_key=job_key,
        run_status=run_status,
    )
    return ResponseEnvelope(
        data=runs,
        pagination=build_pagination(page=page, page_size=page_size, total_items=total),
        meta=_meta("data_job_runs"),
    )


@router.get("/{job_key}")
async def data_jobs_get(
    job_key: str,
    service: DataJobService = DATA_JOB_SERVICE_DEP,
) -> ResponseEnvelope[DataJob]:
    job, _deps = await run_in_threadpool(service.get_job, job_key)
    return ResponseEnvelope(data=job, pagination=None, meta=_meta("data_jobs"))


@router.patch("/{job_key}")
async def data_jobs_patch(
    job_key: str,
    request: DataJobUpdateRequest,
    service: DataJobService = DATA_JOB_SERVICE_DEP,
) -> ResponseEnvelope[DataJob]:
    updated = await run_in_threadpool(service.update_job, job_key, request)
    return ResponseEnvelope(data=updated, pagination=None, meta=_meta("data_jobs"))


@router.post("/{job_key}/runs")
async def data_jobs_run(
    job_key: str,
    request: DataJobRunRequest,
    service: DataJobService = DATA_JOB_SERVICE_DEP,
) -> ResponseEnvelope[DataJobRun]:
    run = await run_in_threadpool(service.run_job, job_key, request)
    return ResponseEnvelope(data=run, pagination=None, meta=_meta("data_jobs"))


@router.get("/{job_key}/runs")
async def data_jobs_runs(
    job_key: str,
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=200),
    include_totals: bool = Query(default=False),
    service: DataJobService = DATA_JOB_SERVICE_DEP,
) -> ResponseEnvelope[list[DataJobRun]]:
    _, runs, total = await run_in_threadpool(
        service.list_runs_for_job,
        job_key,
        page=page,
        page_size=page_size,
        include_totals=include_totals,
    )
    return ResponseEnvelope(
        data=runs,
        pagination=build_pagination(page=page, page_size=page_size, total_items=total),
        meta=_meta("data_job_runs"),
    )
