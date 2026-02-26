from __future__ import annotations

from datetime import date
from typing import List

from fastapi import APIRouter, Depends, Query
from fastapi.concurrency import run_in_threadpool

from src.api.dependencies import get_revenue_bookings_service
from src.schemas.revenue_bookings import BookingForecastPoint, ForecastFilters
from src.services.revenue_bookings_service import RevenueBookingsService
from src.shared.response import Meta, ResponseEnvelope, paginate_list


router = APIRouter(prefix="/booking-forecasts", tags=["booking-forecasts"])


def get_forecast_filters(
    lookback_months: int = Query(default=12, alias="lookback_months", ge=3, le=36),
    horizon_months: int = Query(default=3, alias="horizon_months", ge=1, le=12),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, alias="page_size", ge=1, le=500),
) -> ForecastFilters:
    return ForecastFilters(
        lookback_months=lookback_months,
        horizon_months=horizon_months,
        page=page,
        page_size=page_size,
    )


@router.get("")
async def booking_forecasts(
    filters: ForecastFilters = Depends(get_forecast_filters),
    service: RevenueBookingsService = Depends(get_revenue_bookings_service),
) -> ResponseEnvelope[List[BookingForecastPoint]]:
    data = await run_in_threadpool(
        service.get_booking_forecasts, filters.lookback_months, filters.horizon_months
    )
    paged_data, pagination = paginate_list(data, filters.page, filters.page_size)
    meta = Meta(
        as_of_date=date.today().isoformat(),
        source="salesforce_kaptio",
        time_window=f"{filters.lookback_months}m",
        calculation_version="v1",
    )
    return ResponseEnvelope(data=paged_data, pagination=pagination, meta=meta)
