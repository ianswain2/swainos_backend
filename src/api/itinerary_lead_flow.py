from __future__ import annotations

from datetime import date

from fastapi import APIRouter, Depends, Query

from src.api.dependencies import get_revenue_bookings_service
from src.schemas.revenue_bookings import ItineraryLeadFlowFilters, ItineraryLeadFlowResponse
from src.services.revenue_bookings_service import RevenueBookingsService
from src.shared.response import Meta, ResponseEnvelope
from src.shared.time import parse_time_window


router = APIRouter(prefix="/itinerary-lead-flow", tags=["itinerary-lead-flow"])


@router.get("")
def itinerary_lead_flow(
    time_window: str = Query(default="12m"),
    service: RevenueBookingsService = Depends(get_revenue_bookings_service),
) -> ResponseEnvelope[ItineraryLeadFlowResponse]:
    filters = ItineraryLeadFlowFilters(time_window=time_window)
    start_date, end_date = parse_time_window(filters.time_window)
    data = service.get_itinerary_lead_flow(start_date, end_date)
    meta = Meta(
        as_of_date=date.today().isoformat(),
        source="salesforce_kaptio",
        time_window=filters.time_window,
        calculation_version="v1",
    )
    return ResponseEnvelope(data=data, pagination=None, meta=meta)
