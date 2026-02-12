from __future__ import annotations

from datetime import date
from typing import List

from fastapi import APIRouter, Depends

from src.api.dependencies import get_revenue_bookings_service
from src.schemas.revenue_bookings import BookingForecastPoint, ForecastFilters
from src.services.revenue_bookings_service import RevenueBookingsService
from src.shared.response import Meta, ResponseEnvelope, paginate_list


router = APIRouter(prefix="/booking-forecasts", tags=["booking-forecasts"])


@router.get("")
def booking_forecasts(
    filters: ForecastFilters = Depends(),
    service: RevenueBookingsService = Depends(get_revenue_bookings_service),
) -> ResponseEnvelope[List[BookingForecastPoint]]:
    data = service.get_booking_forecasts(filters.lookback_months, filters.horizon_months)
    paged_data, pagination = paginate_list(data, filters.page, filters.page_size)
    meta = Meta(
        as_of_date=date.today().isoformat(),
        source="salesforce_kaptio",
        time_window=f"{filters.lookback_months}m",
        calculation_version="v1",
    )
    return ResponseEnvelope(data=paged_data, pagination=pagination, meta=meta)
