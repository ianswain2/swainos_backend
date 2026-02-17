from __future__ import annotations

from datetime import date

from fastapi import APIRouter, Depends, Query

from src.api.dependencies import get_travel_agencies_service
from src.schemas.travel_agencies import (
    TravelAgencyLeaderboardFilters,
    TravelAgencyLeaderboardResponse,
    TravelAgencyProfileFilters,
    TravelAgencyProfileResponse,
)
from src.services.travel_agencies_service import TravelAgenciesService
from src.shared.response import Meta, ResponseEnvelope

router = APIRouter(prefix="/travel-agencies", tags=["travel-agencies"])


def get_travel_agency_leaderboard_filters(
    period_type: str = Query(default="year", pattern="^(monthly|rolling12|year)$"),
    year: int | None = Query(default=None, ge=2000, le=2100),
    month: int | None = Query(default=None, ge=1, le=12),
    top_n: int = Query(default=10, ge=1, le=50),
    sort_by: str = Query(
        default="gross_profit",
        pattern="^(gross_profit|gross|converted_leads|booked_itineraries|leads)$",
    ),
    sort_order: str = Query(default="desc", pattern="^(asc|desc)$"),
    currency_code: str | None = Query(default=None),
) -> TravelAgencyLeaderboardFilters:
    return TravelAgencyLeaderboardFilters(
        period_type=period_type,
        year=year,
        month=month,
        top_n=top_n,
        sort_by=sort_by,
        sort_order=sort_order,
        currency_code=currency_code,
    )


def get_travel_agency_profile_filters(
    period_type: str = Query(default="year", pattern="^(monthly|rolling12|year)$"),
    year: int | None = Query(default=None, ge=2000, le=2100),
    month: int | None = Query(default=None, ge=1, le=12),
    top_n: int = Query(default=10, ge=1, le=50),
    currency_code: str | None = Query(default=None),
) -> TravelAgencyProfileFilters:
    return TravelAgencyProfileFilters(
        period_type=period_type,
        year=year,
        month=month,
        top_n=top_n,
        currency_code=currency_code,
    )


@router.get("/leaderboard")
def travel_agencies_leaderboard(
    filters: TravelAgencyLeaderboardFilters = Depends(get_travel_agency_leaderboard_filters),
    service: TravelAgenciesService = Depends(get_travel_agencies_service),
) -> ResponseEnvelope[TravelAgencyLeaderboardResponse]:
    data = service.get_leaderboard(filters)
    return ResponseEnvelope(
        data=data,
        pagination=None,
        meta=Meta(
            as_of_date=date.today().isoformat(),
            source="travel_trade_lead_monthly_rollup,travel_trade_booked_itinerary_monthly_rollup,travel_agency_monthly_rollup",
            time_window=filters.period_type,
            calculation_version="v1",
            currency=filters.currency_code,
        ),
    )


@router.get("/{agency_id}/profile")
def travel_agencies_profile(
    agency_id: str,
    filters: TravelAgencyProfileFilters = Depends(get_travel_agency_profile_filters),
    service: TravelAgenciesService = Depends(get_travel_agencies_service),
) -> ResponseEnvelope[TravelAgencyProfileResponse]:
    data = service.get_profile(agency_id, filters)
    return ResponseEnvelope(
        data=data,
        pagination=None,
        meta=Meta(
            as_of_date=date.today().isoformat(),
            source=(
                "travel_trade_lead_monthly_rollup,travel_trade_booked_itinerary_monthly_rollup,"
                "travel_agency_monthly_rollup,travel_agent_monthly_rollup"
            ),
            time_window=filters.period_type,
            calculation_version="v1",
            currency=filters.currency_code,
        ),
    )
