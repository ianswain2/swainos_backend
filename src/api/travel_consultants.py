from __future__ import annotations

from datetime import date

from fastapi import APIRouter, Depends, Query

from src.api.dependencies import get_travel_consultants_service
from src.schemas.travel_consultants import (
    TravelConsultantForecastFilters,
    TravelConsultantForecastResponse,
    TravelConsultantLeaderboardFilters,
    TravelConsultantLeaderboardResponse,
    TravelConsultantProfileFilters,
    TravelConsultantProfileResponse,
)
from src.services.travel_consultants_service import TravelConsultantsService
from src.shared.response import Meta, ResponseEnvelope

router = APIRouter(prefix="/travel-consultants", tags=["travel-consultants"])


def get_travel_consultant_leaderboard_filters(
    period_type: str = Query(default="monthly", pattern="^(monthly|rolling12)$"),
    domain: str = Query(default="travel", pattern="^(travel|funnel)$"),
    year: int | None = Query(default=None, ge=2000, le=2100),
    month: int | None = Query(default=None, ge=1, le=12),
    sort_by: str = Query(
        default="booked_revenue",
        pattern="^(conversion_rate|close_rate|booked_revenue|margin_pct)$",
    ),
    sort_order: str = Query(default="desc", pattern="^(asc|desc)$"),
    currency_code: str | None = Query(default=None),
) -> TravelConsultantLeaderboardFilters:
    return TravelConsultantLeaderboardFilters(
        period_type=period_type,
        domain=domain,
        year=year,
        month=month,
        sort_by=sort_by,
        sort_order=sort_order,
        currency_code=currency_code,
    )


def get_travel_consultant_profile_filters(
    period_type: str = Query(default="rolling12", pattern="^(monthly|rolling12)$"),
    year: int | None = Query(default=None, ge=2000, le=2100),
    month: int | None = Query(default=None, ge=1, le=12),
    yoy_mode: str = Query(default="same_period", pattern="^(same_period|full_year)$"),
    currency_code: str | None = Query(default=None),
) -> TravelConsultantProfileFilters:
    return TravelConsultantProfileFilters(
        period_type=period_type,
        year=year,
        month=month,
        yoy_mode=yoy_mode,
        currency_code=currency_code,
    )


def get_travel_consultant_forecast_filters(
    horizon_months: int = Query(default=12, ge=1, le=24),
    currency_code: str | None = Query(default=None),
) -> TravelConsultantForecastFilters:
    return TravelConsultantForecastFilters(
        horizon_months=horizon_months,
        currency_code=currency_code,
    )


@router.get("/leaderboard")
def travel_consultant_leaderboard(
    filters: TravelConsultantLeaderboardFilters = Depends(get_travel_consultant_leaderboard_filters),
    service: TravelConsultantsService = Depends(get_travel_consultants_service),
) -> ResponseEnvelope[TravelConsultantLeaderboardResponse]:
    data = service.get_leaderboard(filters)
    meta = Meta(
        as_of_date=date.today().isoformat(),
        source="mv_travel_consultant_leaderboard_monthly,mv_travel_consultant_funnel_monthly",
        time_window=filters.period_type,
        calculation_version="v1",
        currency=filters.currency_code,
    )
    return ResponseEnvelope(data=data, pagination=None, meta=meta)


@router.get("/{employee_id}/profile")
def travel_consultant_profile(
    employee_id: str,
    filters: TravelConsultantProfileFilters = Depends(get_travel_consultant_profile_filters),
    service: TravelConsultantsService = Depends(get_travel_consultants_service),
) -> ResponseEnvelope[TravelConsultantProfileResponse]:
    data = service.get_profile(employee_id, filters)
    meta = Meta(
        as_of_date=date.today().isoformat(),
        source=(
            "mv_travel_consultant_profile_monthly,mv_travel_consultant_funnel_monthly,"
            "mv_travel_consultant_compensation_monthly"
        ),
        time_window=filters.period_type,
        calculation_version="v1",
        currency=filters.currency_code,
    )
    return ResponseEnvelope(data=data, pagination=None, meta=meta)


@router.get("/{employee_id}/forecast")
def travel_consultant_forecast(
    employee_id: str,
    filters: TravelConsultantForecastFilters = Depends(get_travel_consultant_forecast_filters),
    service: TravelConsultantsService = Depends(get_travel_consultants_service),
) -> ResponseEnvelope[TravelConsultantForecastResponse]:
    data = service.get_forecast(employee_id, filters)
    meta = Meta(
        as_of_date=date.today().isoformat(),
        source="mv_travel_consultant_profile_monthly",
        time_window=f"{filters.horizon_months}m",
        calculation_version="v1",
        currency=filters.currency_code,
    )
    return ResponseEnvelope(data=data, pagination=None, meta=meta)
