from __future__ import annotations

from datetime import date

from fastapi import APIRouter
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import JSONResponse

from src.core.errors import ErrorDetail, ErrorEnvelope
from src.core.supabase import SupabaseClient
from src.shared.response import Meta, ResponseEnvelope


router = APIRouter(tags=["health"])


@router.get("/health")
def health_check() -> ResponseEnvelope[dict]:
    meta = Meta(
        as_of_date=date.today().isoformat(),
        source="system",
        time_window="now",
        calculation_version="v1",
    )
    return ResponseEnvelope(data={"status": "ok"}, meta=meta)


@router.get("/healthz")
def health_check_liveness() -> ResponseEnvelope[dict]:
    meta = Meta(
        as_of_date=date.today().isoformat(),
        source="system",
        time_window="now",
        calculation_version="v1",
    )
    return ResponseEnvelope(data={"status": "ok"}, meta=meta)


@router.get("/health/ready", response_model=None)
async def health_check_readiness() -> JSONResponse:
    meta = Meta(
        as_of_date=date.today().isoformat(),
        source="system",
        time_window="now",
        calculation_version="v1",
    )
    try:
        client = SupabaseClient()
        await run_in_threadpool(client.select, "fx_rates", "id", None, 1)
    except Exception as exc:
        _ = exc
        envelope = ErrorEnvelope(
            error=ErrorDetail(
                code="dependency_unavailable",
                message="Readiness check failed",
                details={"dependency": "supabase"},
            )
        )
        return JSONResponse(status_code=503, content=envelope.model_dump())
    return JSONResponse(
        status_code=200,
        content=ResponseEnvelope(data={"status": "ready"}, meta=meta).model_dump(),
    )
