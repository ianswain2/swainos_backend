from __future__ import annotations

from datetime import date

from fastapi import APIRouter, Depends, Header
from fastapi.concurrency import run_in_threadpool

from src.api.dependencies import get_fx_service
from src.core.config import get_settings
from src.core.errors import BadRequestError
from src.schemas.fx import FxManualRunResult, FxSignalRunRequest
from src.services.fx_service import FxService
from src.shared.response import Meta, ResponseEnvelope

router = APIRouter(prefix="/fx", tags=["fx"])
FX_SERVICE_DEP = Depends(get_fx_service)


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
    request: FxSignalRunRequest,
    x_fx_run_token: str | None = Header(default=None),
    service: FxService = FX_SERVICE_DEP,
) -> ResponseEnvelope[FxManualRunResult]:
    configured = (get_settings().fx_manual_run_token or "").strip()
    if configured and x_fx_run_token != configured:
        raise BadRequestError("Invalid FX run token")
    result = await run_in_threadpool(service.run_signals, request)
    return ResponseEnvelope(
        data=result,
        pagination=None,
        meta=_meta("fx_signal_runs", data_status=result.status),
    )

