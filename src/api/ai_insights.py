from __future__ import annotations

from datetime import date
from typing import Any, Dict, Optional

from fastapi import APIRouter, Depends, Header, Query

from src.api.dependencies import get_ai_insights_service
from src.core.config import get_settings
from src.core.errors import BadRequestError
from src.schemas.ai_insights import (
    AiBriefingDaily,
    AiEntityInsightsResponse,
    AiInsightFeedResponse,
    AiInsightHistoryResponse,
    AiInsightsFeedFilters,
    AiInsightsHistoryFilters,
    AiRecommendationFilters,
    AiRecommendationQueueResponse,
    AiRecommendationUpdateRequest,
)
from src.services.ai_insights_service import AiInsightsService
from src.shared.response import Meta, ResponseEnvelope

router = APIRouter(prefix="/ai-insights", tags=["ai-insights"])


def get_feed_filters(
    domain: Optional[str] = Query(default=None),
    insight_type: Optional[str] = Query(default=None),
    severity: Optional[str] = Query(default=None),
    status: Optional[str] = Query(default=None),
    entity_type: Optional[str] = Query(default=None),
    entity_id: Optional[str] = Query(default=None),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=25, ge=1, le=100),
    include_totals: bool = Query(default=False),
) -> AiInsightsFeedFilters:
    return AiInsightsFeedFilters(
        domain=domain,
        insight_type=insight_type,
        severity=severity,
        status=status,
        entity_type=entity_type,
        entity_id=entity_id,
        page=page,
        page_size=page_size,
        include_totals=include_totals,
    )


def get_recommendation_filters(
    domain: Optional[str] = Query(default=None),
    status: Optional[str] = Query(default=None),
    priority_min: Optional[int] = Query(default=None, ge=1, le=5),
    priority_max: Optional[int] = Query(default=None, ge=1, le=5),
    owner_user_id: Optional[str] = Query(default=None),
    entity_type: Optional[str] = Query(default=None),
    entity_id: Optional[str] = Query(default=None),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=25, ge=1, le=100),
    include_totals: bool = Query(default=False),
) -> AiRecommendationFilters:
    return AiRecommendationFilters(
        domain=domain,
        status=status,
        priority_min=priority_min,
        priority_max=priority_max,
        owner_user_id=owner_user_id,
        entity_type=entity_type,
        entity_id=entity_id,
        page=page,
        page_size=page_size,
        include_totals=include_totals,
    )


def get_history_filters(
    domain: Optional[str] = Query(default=None),
    insight_type: Optional[str] = Query(default=None),
    status: Optional[str] = Query(default=None),
    date_from: Optional[date] = Query(default=None),
    date_to: Optional[date] = Query(default=None),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=200),
    include_totals: bool = Query(default=False),
) -> AiInsightsHistoryFilters:
    return AiInsightsHistoryFilters(
        domain=domain,
        insight_type=insight_type,
        status=status,
        date_from=date_from,
        date_to=date_to,
        page=page,
        page_size=page_size,
        include_totals=include_totals,
    )


def require_manual_run_access(
    x_ai_run_token: Optional[str] = Header(default=None),
) -> None:
    settings = get_settings()
    expected = settings.ai_manual_run_token
    if not expected:
        raise BadRequestError("Manual AI run endpoint is disabled")
    if x_ai_run_token != expected:
        raise BadRequestError("Invalid manual run token")


@router.get("/briefing")
def ai_briefing(
    briefing_date: Optional[date] = Query(default=None),
    service: AiInsightsService = Depends(get_ai_insights_service),
) -> ResponseEnvelope[AiBriefingDaily]:
    data = service.get_briefing(briefing_date=briefing_date)
    meta = Meta(
        as_of_date=date.today().isoformat(),
        source="ai_briefings_daily",
        time_window="daily",
        calculation_version="v1",
        currency=None,
    )
    return ResponseEnvelope(data=data, pagination=None, meta=meta)


@router.get("/feed")
def ai_feed(
    filters: AiInsightsFeedFilters = Depends(get_feed_filters),
    service: AiInsightsService = Depends(get_ai_insights_service),
) -> ResponseEnvelope[AiInsightFeedResponse]:
    data, pagination = service.get_feed(filters)
    meta = Meta(
        as_of_date=date.today().isoformat(),
        source="ai_insight_events",
        time_window="rolling",
        calculation_version="v1",
        currency=None,
    )
    return ResponseEnvelope(data=data, pagination=pagination, meta=meta)


@router.get("/recommendations")
def ai_recommendations(
    filters: AiRecommendationFilters = Depends(get_recommendation_filters),
    service: AiInsightsService = Depends(get_ai_insights_service),
) -> ResponseEnvelope[AiRecommendationQueueResponse]:
    data, pagination = service.get_recommendations(filters)
    meta = Meta(
        as_of_date=date.today().isoformat(),
        source="ai_recommendation_queue",
        time_window="rolling",
        calculation_version="v1",
        currency=None,
    )
    return ResponseEnvelope(data=data, pagination=pagination, meta=meta)


@router.patch("/recommendations/{recommendation_id}")
def ai_recommendation_transition(
    recommendation_id: str,
    request: AiRecommendationUpdateRequest,
    service: AiInsightsService = Depends(get_ai_insights_service),
) -> ResponseEnvelope[Any]:
    data = service.update_recommendation(recommendation_id, request)
    meta = Meta(
        as_of_date=date.today().isoformat(),
        source="ai_recommendation_queue",
        time_window="point_in_time",
        calculation_version="v1",
        currency=None,
    )
    return ResponseEnvelope(data=data, pagination=None, meta=meta)


@router.get("/history")
def ai_history(
    filters: AiInsightsHistoryFilters = Depends(get_history_filters),
    service: AiInsightsService = Depends(get_ai_insights_service),
) -> ResponseEnvelope[AiInsightHistoryResponse]:
    data, pagination = service.get_history(filters)
    meta = Meta(
        as_of_date=date.today().isoformat(),
        source="ai_insight_events",
        time_window="historical",
        calculation_version="v1",
        currency=None,
    )
    return ResponseEnvelope(data=data, pagination=pagination, meta=meta)


@router.get("/entities/{entity_type}/{entity_id}")
def ai_entity_insights(
    entity_type: str,
    entity_id: str,
    service: AiInsightsService = Depends(get_ai_insights_service),
) -> ResponseEnvelope[AiEntityInsightsResponse]:
    data = service.get_entity_insights(entity_type, entity_id)
    meta = Meta(
        as_of_date=date.today().isoformat(),
        source="ai_insight_events",
        time_window="entity",
        calculation_version="v1",
        currency=None,
    )
    return ResponseEnvelope(data=data, pagination=None, meta=meta)


@router.post("/run")
def ai_run_manual_generation(
    _: None = Depends(require_manual_run_access),
    service: AiInsightsService = Depends(get_ai_insights_service),
) -> ResponseEnvelope[Dict[str, Any]]:
    data = service.run_manual_generation(trigger="manual_api")
    meta = Meta(
        as_of_date=date.today().isoformat(),
        source="ai_context_*",
        time_window="manual",
        calculation_version="v1",
        currency=None,
    )
    return ResponseEnvelope(data=data, pagination=None, meta=meta)

