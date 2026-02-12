from __future__ import annotations

from datetime import date

from fastapi import APIRouter, Depends, Query

from src.api.dependencies import get_itinerary_revenue_service
from src.schemas.itinerary_revenue import (
    ItineraryActualsYoyResponse,
    ItineraryChannelsResponse,
    ItineraryConversionResponse,
    ItineraryDepositsResponse,
    ItineraryRevenueFilters,
    ItineraryRevenueOutlookResponse,
)
from src.services.itinerary_revenue_service import ItineraryRevenueService
from src.shared.response import Meta, ResponseEnvelope


router = APIRouter(prefix="/itinerary-revenue", tags=["itinerary-revenue"])


def get_itinerary_revenue_filters(
    time_window: str = Query(default="12m", alias="time_window"),
    grain: str = Query(default="monthly", pattern="^(weekly|monthly)$"),
    currency_code: str | None = Query(default=None, alias="currency_code"),
    years_back: int = Query(default=2, alias="years_back", ge=2, le=5),
    actuals_year: int | None = Query(default=None, alias="actuals_year", ge=2000, le=2100),
) -> ItineraryRevenueFilters:
    return ItineraryRevenueFilters(
        time_window=time_window,
        grain=grain,
        currency_code=currency_code,
        years_back=years_back,
        actuals_year=actuals_year,
    )


@router.get("/outlook")
def itinerary_revenue_outlook(
    filters: ItineraryRevenueFilters = Depends(get_itinerary_revenue_filters),
    service: ItineraryRevenueService = Depends(get_itinerary_revenue_service),
) -> ResponseEnvelope[ItineraryRevenueOutlookResponse]:
    data = service.get_outlook(filters.time_window, filters.grain)
    meta = Meta(
        as_of_date=date.today().isoformat(),
        source="mv_itinerary_revenue_monthly,mv_itinerary_revenue_weekly,mv_itinerary_pipeline_stages",
        time_window=filters.time_window,
        calculation_version="v2",
        currency=filters.currency_code,
    )
    return ResponseEnvelope(data=data, pagination=None, meta=meta)


@router.get("/deposits")
def itinerary_revenue_deposits(
    filters: ItineraryRevenueFilters = Depends(get_itinerary_revenue_filters),
    service: ItineraryRevenueService = Depends(get_itinerary_revenue_service),
) -> ResponseEnvelope[ItineraryDepositsResponse]:
    data = service.get_deposits(filters.time_window)
    meta = Meta(
        as_of_date=date.today().isoformat(),
        source="mv_itinerary_deposit_monthly",
        time_window=filters.time_window,
        calculation_version="v2",
        currency=filters.currency_code,
    )
    return ResponseEnvelope(data=data, pagination=None, meta=meta)


@router.get("/conversion")
def itinerary_revenue_conversion(
    filters: ItineraryRevenueFilters = Depends(get_itinerary_revenue_filters),
    service: ItineraryRevenueService = Depends(get_itinerary_revenue_service),
) -> ResponseEnvelope[ItineraryConversionResponse]:
    data = service.get_conversion(filters.time_window, filters.grain)
    meta = Meta(
        as_of_date=date.today().isoformat(),
        source="mv_itinerary_pipeline_stages",
        time_window=filters.time_window,
        calculation_version="v2",
        currency=filters.currency_code,
    )
    return ResponseEnvelope(data=data, pagination=None, meta=meta)


@router.get("/channels")
def itinerary_revenue_channels(
    filters: ItineraryRevenueFilters = Depends(get_itinerary_revenue_filters),
    service: ItineraryRevenueService = Depends(get_itinerary_revenue_service),
) -> ResponseEnvelope[ItineraryChannelsResponse]:
    data = service.get_channels(filters.time_window)
    meta = Meta(
        as_of_date=date.today().isoformat(),
        source="mv_itinerary_consortia_monthly,mv_itinerary_trade_agency_monthly",
        time_window=filters.time_window,
        calculation_version="v2",
        currency=filters.currency_code,
    )
    return ResponseEnvelope(data=data, pagination=None, meta=meta)


@router.get("/actuals-yoy")
def itinerary_revenue_actuals_yoy(
    filters: ItineraryRevenueFilters = Depends(get_itinerary_revenue_filters),
    service: ItineraryRevenueService = Depends(get_itinerary_revenue_service),
) -> ResponseEnvelope[ItineraryActualsYoyResponse]:
    data = service.get_actuals_yoy(filters.years_back)
    meta = Meta(
        as_of_date=date.today().isoformat(),
        source="mv_itinerary_revenue_monthly,mv_itinerary_consortia_actuals_monthly",
        time_window=f"{filters.years_back}y",
        calculation_version="v2",
        currency=filters.currency_code,
    )
    return ResponseEnvelope(data=data, pagination=None, meta=meta)


@router.get("/actuals-channels")
def itinerary_revenue_actuals_channels(
    filters: ItineraryRevenueFilters = Depends(get_itinerary_revenue_filters),
    service: ItineraryRevenueService = Depends(get_itinerary_revenue_service),
) -> ResponseEnvelope[ItineraryChannelsResponse]:
    data = service.get_actuals_channels(filters.years_back, filters.actuals_year)
    if filters.actuals_year is not None:
        time_window = f"{filters.actuals_year}"
    else:
        time_window = f"{filters.years_back}y"
    meta = Meta(
        as_of_date=date.today().isoformat(),
        source="mv_itinerary_consortia_actuals_monthly,mv_itinerary_trade_agency_actuals_monthly",
        time_window=time_window,
        calculation_version="v2",
        currency=filters.currency_code,
    )
    return ResponseEnvelope(data=data, pagination=None, meta=meta)
