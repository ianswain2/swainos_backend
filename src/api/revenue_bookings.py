from __future__ import annotations

from datetime import date
from typing import List

from fastapi import APIRouter, Depends, Query
from fastapi.concurrency import run_in_threadpool

from src.api.dependencies import get_revenue_bookings_service
from src.schemas.revenue_bookings import BookingDetail, BookingSummary
from src.services.revenue_bookings_service import RevenueBookingsService
from src.shared.response import Meta, ResponseEnvelope, build_pagination


router = APIRouter(prefix="/revenue-bookings", tags=["revenue-bookings"])


@router.get("")
async def revenue_bookings(
    start_date: date | None = Query(default=None, alias="start_date"),
    end_date: date | None = Query(default=None, alias="end_date"),
    currency_code: str | None = Query(default=None, alias="currency_code"),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, alias="page_size", ge=1, le=500),
    service: RevenueBookingsService = Depends(get_revenue_bookings_service),
) -> ResponseEnvelope[List[BookingSummary]]:
    data, total_items = await run_in_threadpool(
        service.list_bookings,
        start_date=start_date,
        end_date=end_date,
        currency_code=currency_code,
        page=page,
        page_size=page_size,
    )
    meta = Meta(
        as_of_date=date.today().isoformat(),
        source="salesforce_kaptio",
        time_window="na",
        calculation_version="v1",
        currency=currency_code,
    )
    return ResponseEnvelope(
        data=data,
        pagination=build_pagination(page=page, page_size=page_size, total_items=total_items),
        meta=meta,
    )


@router.get("/{booking_id}")
async def revenue_booking_detail(
    booking_id: str,
    service: RevenueBookingsService = Depends(get_revenue_bookings_service),
) -> ResponseEnvelope[BookingDetail]:
    data = await run_in_threadpool(service.get_booking, booking_id)
    meta = Meta(
        as_of_date=date.today().isoformat(),
        source="salesforce_kaptio",
        time_window="na",
        calculation_version="v1",
        currency=data.currency_code,
    )
    return ResponseEnvelope(data=data, pagination=None, meta=meta)
