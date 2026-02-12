from __future__ import annotations

from datetime import date

from fastapi import APIRouter, Depends, Query

from src.api.dependencies import get_revenue_bookings_service
from src.schemas.revenue_bookings import ItineraryTrendsFilters, ItineraryTrendsResponse
from src.services.revenue_bookings_service import RevenueBookingsService
from src.shared.response import Meta, ResponseEnvelope
from src.shared.time import parse_time_window


router = APIRouter(prefix="/itinerary-trends", tags=["itinerary-trends"])


@router.get("")
def itinerary_trends(
    time_window: str = Query(default="12m"),
    service: RevenueBookingsService = Depends(get_revenue_bookings_service),
) -> ResponseEnvelope[ItineraryTrendsResponse]:
    filters = ItineraryTrendsFilters(time_window=time_window)
    start_date, end_date = parse_time_window(filters.time_window)
    data = service.get_itinerary_trends(start_date, end_date)
    meta = Meta(
        as_of_date=date.today().isoformat(),
        source="salesforce_kaptio",
        time_window=filters.time_window,
        calculation_version="v1",
    )
    return ResponseEnvelope(data=data, pagination=None, meta=meta)
