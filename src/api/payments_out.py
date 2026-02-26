from __future__ import annotations

from datetime import date
from typing import List

from fastapi import APIRouter, Depends, Query
from fastapi.concurrency import run_in_threadpool

from src.api.dependencies import get_revenue_bookings_service
from src.schemas.revenue_bookings import CashFlowFilters, PaymentOutSummary
from src.services.revenue_bookings_service import RevenueBookingsService
from src.shared.response import Meta, ResponseEnvelope, paginate_list
from src.shared.time import parse_time_window


router = APIRouter(prefix="/payments-out", tags=["payments-out"])


def get_payments_out_filters(
    time_window: str = Query(default="90d", alias="time_window"),
    currency_code: str | None = Query(default=None, alias="currency_code"),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, alias="page_size", ge=1, le=500),
) -> CashFlowFilters:
    return CashFlowFilters(
        time_window=time_window,
        currency_code=currency_code,
        page=page,
        page_size=page_size,
    )


@router.get("/summary")
async def payments_out_summary(
    filters: CashFlowFilters = Depends(get_payments_out_filters),
    service: RevenueBookingsService = Depends(get_revenue_bookings_service),
) -> ResponseEnvelope[List[PaymentOutSummary]]:
    start_date, end_date = parse_time_window(filters.time_window)
    data = await run_in_threadpool(
        service.get_payments_out_summary, start_date, end_date, filters.currency_code
    )
    paged_data, pagination = paginate_list(data, filters.page, filters.page_size)
    meta = Meta(
        as_of_date=date.today().isoformat(),
        source="salesforce_kaptio",
        time_window=filters.time_window,
        calculation_version="v1",
        currency=filters.currency_code,
    )
    return ResponseEnvelope(data=paged_data, pagination=pagination, meta=meta)
