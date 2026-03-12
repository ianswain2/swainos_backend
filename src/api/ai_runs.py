from __future__ import annotations

from datetime import date
import logging
import time
from typing import Any

from fastapi import APIRouter, Depends, Header, Request
from fastapi.concurrency import run_in_threadpool

from src.api.dependencies import get_ai_insights_service
from src.api.rate_limits import enforce_expensive_run_limit
from src.core.config import get_settings
from src.core.errors import UnauthorizedError
from src.services.ai_insights_service import AiInsightsService
from src.shared.response import Meta, ResponseEnvelope

router = APIRouter(prefix="/ai-insights", tags=["ai-insights"])
AI_INSIGHTS_SERVICE_DEP = Depends(get_ai_insights_service)
logger = logging.getLogger(__name__)


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
    request: Request,
    x_ai_run_token: str | None = Header(default=None),
    service: AiInsightsService = AI_INSIGHTS_SERVICE_DEP,
) -> ResponseEnvelope[Any]:
    enforce_expensive_run_limit(request, "ai_manual_run")
    settings = get_settings()
    configured = (settings.ai_manual_run_token or "").strip()
    is_production = settings.environment.strip().lower() == "production"
    if is_production and not configured:
        raise UnauthorizedError("AI run token is not configured")
    if configured and x_ai_run_token != configured:
        raise UnauthorizedError("Invalid AI run token")
    started = time.perf_counter()
    result = await run_in_threadpool(service.run_manual_generation, "manual_api")
    elapsed_ms = int((time.perf_counter() - started) * 1000)
    logger.info(
        "ai_manual_run_completed",
        extra={
            "endpoint": "/api/v1/ai-insights/run",
            "durationMs": elapsed_ms,
            "modelCallsUsed": result.get("budget", {}).get("modelCallsUsed")
            if isinstance(result, dict)
            else None,
            "tokensUsed": result.get("budget", {}).get("tokensUsed") if isinstance(result, dict) else None,
            "status": result.get("status") if isinstance(result, dict) else None,
            "clientIp": request.client.host if request.client else "unknown",
        },
    )
    return ResponseEnvelope(data=result, pagination=None, meta=_meta("ai_manual_run"))

