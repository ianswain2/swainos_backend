from __future__ import annotations

from datetime import date
from typing import List

from fastapi import APIRouter, Depends

from src.api.dependencies import get_revenue_bookings_service
from src.schemas.revenue_bookings import CashFlowFilters, CashFlowSummary, CashFlowTimeseriesPoint
from src.services.revenue_bookings_service import RevenueBookingsService
from src.shared.response import Meta, ResponseEnvelope, paginate_list
from src.shared.time import parse_time_window


router = APIRouter(prefix="/cash-flow", tags=["cash-flow"])


@router.get("/summary")
def cash_flow_summary(
    filters: CashFlowFilters = Depends(),
    service: RevenueBookingsService = Depends(get_revenue_bookings_service),
) -> ResponseEnvelope[List[CashFlowSummary]]:
    start_date, end_date = parse_time_window(filters.time_window)
    data = service.get_cashflow_summary(start_date, end_date, filters.currency_code)
    paged_data, pagination = paginate_list(data, filters.page, filters.page_size)
    meta = Meta(
        as_of_date=date.today().isoformat(),
        source="salesforce_kaptio",
        time_window=filters.time_window,
        calculation_version="v1",
        currency=filters.currency_code,
    )
    return ResponseEnvelope(data=paged_data, pagination=pagination, meta=meta)


@router.get("/timeseries")
def cash_flow_timeseries(
    filters: CashFlowFilters = Depends(),
    service: RevenueBookingsService = Depends(get_revenue_bookings_service),
) -> ResponseEnvelope[List[CashFlowTimeseriesPoint]]:
    start_date, end_date = parse_time_window(filters.time_window)
    data = service.get_cashflow_timeseries(start_date, end_date, filters.currency_code)
    paged_data, pagination = paginate_list(data, filters.page, filters.page_size)
    meta = Meta(
        as_of_date=date.today().isoformat(),
        source="salesforce_kaptio",
        time_window=filters.time_window,
        calculation_version="v1",
        currency=filters.currency_code,
    )
    return ResponseEnvelope(data=paged_data, pagination=pagination, meta=meta)
