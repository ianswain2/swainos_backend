from __future__ import annotations

from datetime import date
import logging
import time

from fastapi import APIRouter, Depends, Header, Request
from fastapi.concurrency import run_in_threadpool

from src.api.dependencies import get_fx_service
from src.api.rate_limits import enforce_expensive_run_limit
from src.core.config import get_settings
from src.core.errors import UnauthorizedError
from src.schemas.fx import FxManualRunResult, FxSignalRunRequest
from src.services.fx_service import FxService
from src.shared.response import Meta, ResponseEnvelope

router = APIRouter(prefix="/fx", tags=["fx"])
FX_SERVICE_DEP = Depends(get_fx_service)
logger = logging.getLogger(__name__)


def _meta(source: str, data_status: str = "live") -> Meta:
    return Meta(
        as_of_date=date.today().isoformat(),
        source=source,
        time_window="point_in_time",
        calculation_version="v1",
        data_status=data_status,
        is_stale=False,
        degraded=False,
    )


@router.post("/signals/run")
async def fx_signals_run(
    request_ctx: Request,
    request: FxSignalRunRequest,
    x_fx_run_token: str | None = Header(default=None),
    service: FxService = FX_SERVICE_DEP,
) -> ResponseEnvelope[FxManualRunResult]:
    enforce_expensive_run_limit(request_ctx, "fx_signals_run")
    settings = get_settings()
    configured = (settings.fx_manual_run_token or "").strip()
    is_production = settings.environment.strip().lower() == "production"
    if is_production and not configured:
        raise UnauthorizedError("FX run token is not configured")
    if configured and x_fx_run_token != configured:
        raise UnauthorizedError("Invalid FX run token")
    started = time.perf_counter()
    result = await run_in_threadpool(service.run_signals, request)
    elapsed_ms = int((time.perf_counter() - started) * 1000)
    logger.info(
        "fx_signals_run_completed",
        extra={
            "endpoint": "/api/v1/fx/signals/run",
            "durationMs": elapsed_ms,
            "status": result.status,
            "recordsProcessed": result.records_processed,
            "clientIp": request_ctx.client.host if request_ctx.client else "unknown",
        },
    )
    return ResponseEnvelope(
        data=result,
        pagination=None,
        meta=_meta("fx_signal_runs", data_status=result.status),
    )

