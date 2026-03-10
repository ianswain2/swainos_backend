from __future__ import annotations

from datetime import date

from fastapi import APIRouter, Depends
from fastapi.concurrency import run_in_threadpool

from src.api.dependencies import get_data_job_service
from src.schemas.data_jobs import DataJobRunDetail
from src.services.data_job_service import DataJobService
from src.shared.response import Meta, ResponseEnvelope

router = APIRouter(prefix="/data-job-runs", tags=["data-jobs"])
DATA_JOB_SERVICE_DEP = Depends(get_data_job_service)


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


@router.get("/{run_id}")
async def data_job_runs_get(
    run_id: str,
    service: DataJobService = DATA_JOB_SERVICE_DEP,
) -> ResponseEnvelope[DataJobRunDetail]:
    detail = await run_in_threadpool(service.get_run_detail, run_id)
    return ResponseEnvelope(data=detail, pagination=None, meta=_meta("data_job_runs"))
