from __future__ import annotations

from datetime import date
from typing import Any

from fastapi import APIRouter, Depends, Header
from fastapi.concurrency import run_in_threadpool

from src.api.dependencies import get_ai_insights_service
from src.core.config import get_settings
from src.core.errors import BadRequestError
from src.services.ai_insights_service import AiInsightsService
from src.shared.response import Meta, ResponseEnvelope

router = APIRouter(prefix="/ai-insights", tags=["ai-insights"])
AI_INSIGHTS_SERVICE_DEP = Depends(get_ai_insights_service)


def _meta(source: str) -> Meta:
    return Meta(
        as_of_date=date.today().isoformat(),
        source=source,
        time_window="point_in_time",
        calculation_version="v1",
        data_status="live",
        is_stale=False,
        degraded=False,
    )


@router.post("/run")
async def ai_manual_run(
    x_ai_run_token: str | None = Header(default=None),
    service: AiInsightsService = AI_INSIGHTS_SERVICE_DEP,
) -> ResponseEnvelope[Any]:
    configured = (get_settings().ai_manual_run_token or "").strip()
    if configured and x_ai_run_token != configured:
        raise BadRequestError("Invalid AI run token")
    result = await run_in_threadpool(service.run_manual_generation, "manual_api")
    return ResponseEnvelope(data=result, pagination=None, meta=_meta("ai_manual_run"))

