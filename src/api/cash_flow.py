from __future__ import annotations

from datetime import date
from typing import List

from fastapi import APIRouter, Depends, Query
from fastapi.concurrency import run_in_threadpool

from src.api.dependencies import get_revenue_bookings_service
from src.schemas.revenue_bookings import (
    CashFlowApMonthlyOutflowPoint,
    CashFlowApSchedulePoint,
    CashFlowFilters,
    CashFlowForecastResponse,
    CashFlowRiskOverview,
    CashFlowScenarioSummary,
    CashFlowSummary,
    CashFlowTimeseriesPoint,
)
from src.services.revenue_bookings_service import RevenueBookingsService
from src.shared.response import Meta, ResponseEnvelope, paginate_list
from src.shared.time import parse_forward_time_window, parse_time_window


router = APIRouter(prefix="/cash-flow", tags=["cash-flow"])


def get_cash_flow_filters(
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
async def cash_flow_summary(
    filters: CashFlowFilters = Depends(get_cash_flow_filters),
    service: RevenueBookingsService = Depends(get_revenue_bookings_service),
) -> ResponseEnvelope[List[CashFlowSummary]]:
    start_date, end_date = parse_time_window(filters.time_window)
    data = await run_in_threadpool(
        service.get_cashflow_summary, start_date, end_date, filters.currency_code
    )
    paged_data, pagination = paginate_list(data, filters.page, filters.page_size)
    meta = Meta(
        as_of_date=date.today().isoformat(),
        source="customer_payments + ap_payment_calendar_v1",
        time_window=filters.time_window,
        calculation_version="v1",
        currency=filters.currency_code,
    )
    return ResponseEnvelope(data=paged_data, pagination=pagination, meta=meta)


@router.get("/timeseries")
async def cash_flow_timeseries(
    filters: CashFlowFilters = Depends(get_cash_flow_filters),
    service: RevenueBookingsService = Depends(get_revenue_bookings_service),
) -> ResponseEnvelope[List[CashFlowTimeseriesPoint]]:
    start_date, end_date = parse_time_window(filters.time_window)
    data = await run_in_threadpool(
        service.get_cashflow_timeseries, start_date, end_date, filters.currency_code
    )
    paged_data, pagination = paginate_list(data, filters.page, filters.page_size)
    meta = Meta(
        as_of_date=date.today().isoformat(),
        source="customer_payments + ap_payment_calendar_v1",
        time_window=filters.time_window,
        calculation_version="v1",
        currency=filters.currency_code,
    )
    return ResponseEnvelope(data=paged_data, pagination=pagination, meta=meta)


@router.get("/risk-overview")
async def cash_flow_risk_overview(
    filters: CashFlowFilters = Depends(get_cash_flow_filters),
    service: RevenueBookingsService = Depends(get_revenue_bookings_service),
) -> ResponseEnvelope[List[CashFlowRiskOverview]]:
    start_date, end_date = parse_forward_time_window(filters.time_window)
    data = await run_in_threadpool(
        service.get_cashflow_risk_overview,
        start_date,
        end_date,
        filters.currency_code,
        filters.time_window,
    )
    paged_data, pagination = paginate_list(data, filters.page, filters.page_size)
    meta = Meta(
        as_of_date=date.today().isoformat(),
        source="customer_payments + ap_payment_calendar_v1",
        time_window=filters.time_window,
        calculation_version="v1",
        currency=filters.currency_code,
    )
    return ResponseEnvelope(data=paged_data, pagination=pagination, meta=meta)


@router.get("/forecast")
async def cash_flow_forecast(
    filters: CashFlowFilters = Depends(get_cash_flow_filters),
    service: RevenueBookingsService = Depends(get_revenue_bookings_service),
) -> ResponseEnvelope[List[CashFlowForecastResponse]]:
    start_date, end_date = parse_forward_time_window(filters.time_window)
    data = await run_in_threadpool(
        service.get_cashflow_forecast,
        start_date,
        end_date,
        filters.currency_code,
        filters.time_window,
    )
    paged_data, pagination = paginate_list(data, filters.page, filters.page_size)
    meta = Meta(
        as_of_date=date.today().isoformat(),
        source="customer_payments + ap_payment_calendar_v1",
        time_window=filters.time_window,
        calculation_version="v1",
        currency=filters.currency_code,
    )
    return ResponseEnvelope(data=paged_data, pagination=pagination, meta=meta)


@router.get("/ap-schedule")
async def cash_flow_ap_schedule(
    filters: CashFlowFilters = Depends(get_cash_flow_filters),
    service: RevenueBookingsService = Depends(get_revenue_bookings_service),
) -> ResponseEnvelope[List[CashFlowApSchedulePoint]]:
    start_date, end_date = parse_forward_time_window(filters.time_window)
    data = await run_in_threadpool(
        service.get_cashflow_ap_schedule,
        start_date,
        end_date,
        filters.currency_code,
    )
    paged_data, pagination = paginate_list(data, filters.page, filters.page_size)
    meta = Meta(
        as_of_date=date.today().isoformat(),
        source="ap_payment_calendar_v1",
        time_window=filters.time_window,
        calculation_version="v1",
        currency=filters.currency_code,
    )
    return ResponseEnvelope(data=paged_data, pagination=pagination, meta=meta)


@router.get("/ap-monthly-outflow")
async def cash_flow_ap_monthly_outflow(
    filters: CashFlowFilters = Depends(get_cash_flow_filters),
    service: RevenueBookingsService = Depends(get_revenue_bookings_service),
) -> ResponseEnvelope[List[CashFlowApMonthlyOutflowPoint]]:
    start_date, end_date = parse_forward_time_window(filters.time_window)
    data = await run_in_threadpool(
        service.get_cashflow_ap_monthly_outflow,
        start_date,
        end_date,
        filters.currency_code,
    )
    paged_data, pagination = paginate_list(data, filters.page, filters.page_size)
    meta = Meta(
        as_of_date=date.today().isoformat(),
        source="ap_monthly_outflow_v1",
        time_window=filters.time_window,
        calculation_version="v1",
        currency=filters.currency_code,
    )
    return ResponseEnvelope(data=paged_data, pagination=pagination, meta=meta)


@router.get("/scenarios")
async def cash_flow_scenarios(
    filters: CashFlowFilters = Depends(get_cash_flow_filters),
    service: RevenueBookingsService = Depends(get_revenue_bookings_service),
) -> ResponseEnvelope[List[CashFlowScenarioSummary]]:
    start_date, end_date = parse_forward_time_window(filters.time_window)
    data = await run_in_threadpool(
        service.get_cashflow_scenarios,
        start_date,
        end_date,
        filters.currency_code,
        filters.time_window,
    )
    paged_data, pagination = paginate_list(data, filters.page, filters.page_size)
    meta = Meta(
        as_of_date=date.today().isoformat(),
        source="synthetic_scenarios_v1",
        time_window=filters.time_window,
        calculation_version="v1",
        currency=filters.currency_code,
    )
    return ResponseEnvelope(data=paged_data, pagination=pagination, meta=meta)
