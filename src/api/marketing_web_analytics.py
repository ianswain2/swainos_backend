from __future__ import annotations

from datetime import date

from fastapi import APIRouter, Depends, Header, Query
from fastapi.concurrency import run_in_threadpool

from src.api.dependencies import get_marketing_web_analytics_service
from src.core.config import get_settings
from src.core.errors import BadRequestError
from src.schemas.marketing_web_analytics import (
    MarketingAiInsight,
    MarketingEventCatalog,
    MarketingGeoBreakdown,
    MarketingHealth,
    MarketingOverview,
    MarketingPageActivity,
    MarketingSearchConsoleInsights,
    MarketingSearchPerformance,
    MarketingWebAnalyticsSyncResult,
)
from src.services.marketing_web_analytics_service import MarketingWebAnalyticsService
from src.shared.response import Meta, ResponseEnvelope

router = APIRouter(prefix="/marketing/web-analytics", tags=["marketing-web-analytics"])

MARKETING_CALCULATION_VERSION = "v1"
MARKETING_SERVICE_DEP = Depends(get_marketing_web_analytics_service)
MARKETING_RUN_TOKEN_HEADER = Header(default=None)


def _normalize_country_scope(country: str | None) -> str | None:
    normalized = (country or "").strip()
    if not normalized or normalized.lower() == "all":
        return None
    return normalized


def _market_label(country: str | None) -> str:
    return country if country else "All markets"


def _build_meta(
    source: str,
    *,
    data_status: str = "live",
    time_window: str = "30d",
    country: str | None = None,
) -> Meta:
    return Meta(
        as_of_date=date.today().isoformat(),
        source=source,
        time_window=time_window,
        calculation_version=MARKETING_CALCULATION_VERSION,
        market_scope=country or "all",
        market_label=_market_label(country),
        currency=None,
        data_status=data_status,
        is_stale=False,
        degraded=data_status in {"partial", "degraded"},
    )


@router.get("/overview")
async def marketing_overview(
    country: str | None = Query(default=None),
    service: MarketingWebAnalyticsService = MARKETING_SERVICE_DEP,
) -> ResponseEnvelope[MarketingOverview]:
    normalized_country = _normalize_country_scope(country)
    data = await run_in_threadpool(service.get_overview, country=normalized_country)
    return ResponseEnvelope(
        data=data,
        pagination=None,
        meta=_build_meta("ga4", country=normalized_country),
    )


@router.get("/search")
async def marketing_search_performance(
    days_back: int = Query(default=30, ge=7, le=365, alias="days_back"),
    country: str | None = Query(default=None),
    service: MarketingWebAnalyticsService = MARKETING_SERVICE_DEP,
) -> ResponseEnvelope[MarketingSearchPerformance]:
    normalized_country = _normalize_country_scope(country)
    data = await run_in_threadpool(
        service.get_search_performance,
        days_back=days_back,
        country=normalized_country,
    )
    return ResponseEnvelope(
        data=data,
        pagination=None,
        meta=_build_meta(
            "ga4",
            data_status="live",
            time_window=f"{days_back}d",
            country=normalized_country,
        ),
    )


@router.get("/search-console")
async def marketing_search_console_insights(
    days_back: int = Query(default=30, ge=7, le=365, alias="days_back"),
    country: str | None = Query(default=None),
    service: MarketingWebAnalyticsService = MARKETING_SERVICE_DEP,
) -> ResponseEnvelope[MarketingSearchConsoleInsights]:
    normalized_country = _normalize_country_scope(country)
    data = await run_in_threadpool(
        service.get_search_console_insights,
        days_back=days_back,
        country=normalized_country,
    )
    status = "live" if data.search_console_connected else "partial"
    return ResponseEnvelope(
        data=data,
        pagination=None,
        meta=_build_meta(
            "gsc + ga4",
            data_status=status,
            time_window=f"{days_back}d",
            country=normalized_country,
        ),
    )


@router.get("/ai-insights")
async def marketing_ai_insights(
    country: str | None = Query(default=None),
    service: MarketingWebAnalyticsService = MARKETING_SERVICE_DEP,
) -> ResponseEnvelope[list[MarketingAiInsight]]:
    normalized_country = _normalize_country_scope(country)
    data = await run_in_threadpool(service.get_ai_insights, country=normalized_country)
    return ResponseEnvelope(
        data=data,
        pagination=None,
        meta=_build_meta("ga4", country=normalized_country),
    )


@router.get("/page-activity")
async def marketing_page_activity(
    page_path_contains: str | None = Query(default=None, alias="page_path_contains"),
    limit: int = Query(default=100, ge=10, le=300),
    days_back: int = Query(default=30, ge=7, le=365, alias="days_back"),
    country: str | None = Query(default=None),
    service: MarketingWebAnalyticsService = MARKETING_SERVICE_DEP,
) -> ResponseEnvelope[MarketingPageActivity]:
    normalized_country = _normalize_country_scope(country)
    data = await run_in_threadpool(
        service.get_page_activity,
        page_path_contains=page_path_contains,
        limit=limit,
        days_back=days_back,
        country=normalized_country,
    )
    return ResponseEnvelope(
        data=data,
        pagination=None,
        meta=_build_meta("ga4", time_window=f"{days_back}d", country=normalized_country),
    )


@router.get("/geo")
async def marketing_geo(
    days_back: int = Query(default=30, ge=7, le=365, alias="days_back"),
    country: str | None = Query(default=None),
    service: MarketingWebAnalyticsService = MARKETING_SERVICE_DEP,
) -> ResponseEnvelope[MarketingGeoBreakdown]:
    normalized_country = _normalize_country_scope(country)
    data = await run_in_threadpool(
        service.get_geo_breakdown,
        days_back=days_back,
        country=normalized_country,
    )
    return ResponseEnvelope(
        data=data,
        pagination=None,
        meta=_build_meta("ga4", time_window=f"{days_back}d", country=normalized_country),
    )


@router.get("/events")
async def marketing_events(
    country: str | None = Query(default=None),
    service: MarketingWebAnalyticsService = MARKETING_SERVICE_DEP,
) -> ResponseEnvelope[MarketingEventCatalog]:
    normalized_country = _normalize_country_scope(country)
    data = await run_in_threadpool(service.get_event_catalog, country=normalized_country)
    return ResponseEnvelope(
        data=data,
        pagination=None,
        meta=_build_meta("ga4", country=normalized_country),
    )


@router.get("/health")
async def marketing_health(
    service: MarketingWebAnalyticsService = MARKETING_SERVICE_DEP,
) -> ResponseEnvelope[MarketingHealth]:
    data = await run_in_threadpool(service.get_health)
    status = "live" if data.latest_run_status == "success" else "partial"
    return ResponseEnvelope(data=data, pagination=None, meta=_build_meta("ga4", data_status=status))


@router.post("/sync/run")
async def marketing_sync_run(
    service: MarketingWebAnalyticsService = MARKETING_SERVICE_DEP,
    x_marketing_run_token: str | None = MARKETING_RUN_TOKEN_HEADER,
) -> ResponseEnvelope[MarketingWebAnalyticsSyncResult]:
    configured_token = (get_settings().marketing_manual_run_token or "").strip()
    if configured_token and x_marketing_run_token != configured_token:
        raise BadRequestError("Invalid marketing run token")
    result = await run_in_threadpool(service.run_sync)
    return ResponseEnvelope(
        data=result,
        pagination=None,
        meta=_build_meta("ga4_sync", data_status=result.status),
    )
