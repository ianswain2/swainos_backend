from __future__ import annotations

from datetime import date

from fastapi import APIRouter, Depends, Query
from fastapi.concurrency import run_in_threadpool

from src.api.dependencies import get_itinerary_destinations_service
from src.schemas.itinerary_destinations import (
    ItineraryDestinationBreakdownResponse,
    ItineraryDestinationFilters,
    ItineraryDestinationMatrixResponse,
    ItineraryDestinationSummaryResponse,
    ItineraryDestinationTrendsResponse,
)
from src.services.itinerary_destinations_service import ItineraryDestinationsService
from src.shared.response import Meta, ResponseEnvelope


router = APIRouter(prefix="/itinerary-destinations", tags=["itinerary-destinations"])


def get_itinerary_destination_filters(
    year: int = Query(default=date.today().year, ge=2000, le=2100),
    country: str | None = Query(default=None),
    city: str | None = Query(default=None),
    top_n: int = Query(default=10, alias="top_n", ge=1, le=100),
) -> ItineraryDestinationFilters:
    return ItineraryDestinationFilters(year=year, country=country, city=city, top_n=top_n)


@router.get("/summary")
async def itinerary_destination_summary(
    filters: ItineraryDestinationFilters = Depends(get_itinerary_destination_filters),
    service: ItineraryDestinationsService = Depends(get_itinerary_destinations_service),
) -> ResponseEnvelope[ItineraryDestinationSummaryResponse]:
    data = await run_in_threadpool(service.get_summary, filters.year, filters.top_n)
    meta = Meta(
        as_of_date=date.today().isoformat(),
        source="mv_itinerary_destination_booked_monthly",
        time_window=f"{filters.year}",
        calculation_version="v1",
        currency="USD",
    )
    return ResponseEnvelope(data=data, pagination=None, meta=meta)


@router.get("/trends")
async def itinerary_destination_trends(
    filters: ItineraryDestinationFilters = Depends(get_itinerary_destination_filters),
    service: ItineraryDestinationsService = Depends(get_itinerary_destinations_service),
) -> ResponseEnvelope[ItineraryDestinationTrendsResponse]:
    data = await run_in_threadpool(service.get_trends, filters.year, filters.country, filters.city)
    meta = Meta(
        as_of_date=date.today().isoformat(),
        source="mv_itinerary_destination_booked_monthly",
        time_window=f"{filters.year}",
        calculation_version="v1",
        currency="USD",
    )
    return ResponseEnvelope(data=data, pagination=None, meta=meta)


@router.get("/breakdown")
async def itinerary_destination_breakdown(
    filters: ItineraryDestinationFilters = Depends(get_itinerary_destination_filters),
    service: ItineraryDestinationsService = Depends(get_itinerary_destinations_service),
) -> ResponseEnvelope[ItineraryDestinationBreakdownResponse]:
    data = await run_in_threadpool(service.get_breakdown, filters.year, filters.country, filters.top_n)
    meta = Meta(
        as_of_date=date.today().isoformat(),
        source="mv_itinerary_destination_booked_monthly",
        time_window=f"{filters.year}",
        calculation_version="v1",
        currency="USD",
    )
    return ResponseEnvelope(data=data, pagination=None, meta=meta)


@router.get("/matrix")
async def itinerary_destination_matrix(
    filters: ItineraryDestinationFilters = Depends(get_itinerary_destination_filters),
    service: ItineraryDestinationsService = Depends(get_itinerary_destinations_service),
) -> ResponseEnvelope[ItineraryDestinationMatrixResponse]:
    data = await run_in_threadpool(service.get_matrix, filters.year, filters.country, filters.top_n)
    meta = Meta(
        as_of_date=date.today().isoformat(),
        source="mv_itinerary_destination_booked_monthly",
        time_window=f"{filters.year}",
        calculation_version="v1",
        currency="USD",
    )
    return ResponseEnvelope(data=data, pagination=None, meta=meta)
