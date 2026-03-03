from __future__ import annotations

from datetime import date
from typing import List, Optional

from pydantic import Field

from src.schemas.common import Lineage
from src.shared.base import BaseSchema


class BookingSummary(BaseSchema):
    id: str
    booking_number: Optional[str] = None
    service_start_date: Optional[date] = None
    service_end_date: Optional[date] = None
    gross_amount: Optional[float] = None
    net_amount: Optional[float] = None
    currency_code: Optional[str] = None
    itinerary_id: Optional[str] = None
    lineage: Lineage


class BookingDetail(BookingSummary):
    supplier_id: Optional[str] = None
    booking_type: Optional[str] = None
    service_name: Optional[str] = None
    location_country: Optional[str] = None
    location_city: Optional[str] = None
    confirmation_number: Optional[str] = None


class DepositSummary(BaseSchema):
    currency_code: str
    total_deposits: float
    received_deposits: float
    outstanding_deposits: float
    available_cash_after_liability: float


class PaymentOutSummary(BaseSchema):
    currency_code: str
    open_line_count: int
    total_outstanding_amount: float
    due_30d_amount: float
    next_due_date: Optional[date] = None


class ApSummary(BaseSchema):
    currency_code: str
    open_line_count: int
    open_booking_count: int
    open_supplier_count: int
    total_outstanding_amount: float
    due_7d_amount: float
    due_30d_amount: float
    due_60d_amount: float
    due_90d_amount: float
    next_due_date: Optional[date] = None


class ApAging(BaseSchema):
    currency_code: str
    open_line_count: int
    total_outstanding_amount: float
    current_not_due_amount: float
    overdue_1_30_amount: float
    overdue_31_60_amount: float
    overdue_61_90_amount: float
    overdue_90_plus_amount: float


class ApPaymentCalendarPoint(BaseSchema):
    payment_date: Optional[date] = None
    currency_code: str
    line_count: int
    supplier_count: int
    amount_due: float


class CashFlowSummary(BaseSchema):
    currency_code: str
    cash_in_total: float
    cash_out_total: float
    net_cash_total: float


class CashFlowTimeseriesPoint(BaseSchema):
    period_start: date
    cash_in: float
    cash_out: float
    net_cash: float


class CashFlowRiskDriver(BaseSchema):
    code: str
    message: str


class CashFlowRiskOverview(BaseSchema):
    currency_code: str
    risk_status: str
    first_risk_date: Optional[date] = None
    time_to_risk_days: Optional[int] = None
    projected_ending_cash: float
    projected_min_cash: float
    cash_buffer_amount: float
    coverage_ratio: float
    risk_drivers: List[CashFlowRiskDriver]


class CashFlowForecastPoint(BaseSchema):
    period_start: date
    period_end: date
    cash_in: float
    cash_out: float
    net_cash: float
    projected_ending_cash: float
    coverage_ratio: float
    at_risk: bool


class CashFlowForecastResponse(BaseSchema):
    currency_code: str
    time_window: str
    points: List[CashFlowForecastPoint]


class CashFlowApSchedulePoint(BaseSchema):
    payment_date: date
    currency_code: str
    amount_due: float
    line_count: int
    supplier_count: int


class CashFlowApMonthlyOutflowPoint(BaseSchema):
    month_start: date
    currency_code: str
    amount_due: float
    line_count: int
    supplier_count: int


class CashFlowScenarioSummary(BaseSchema):
    scenario_name: str
    currency_code: str
    description: str
    projected_ending_cash: float
    first_risk_date: Optional[date] = None
    risk_status: str


class BookingForecastPoint(BaseSchema):
    period_start: date
    projected_bookings: int
    confidence: float = Field(..., ge=0.0, le=1.0)


class CashFlowFilters(BaseSchema):
    time_window: str = "90d"
    currency_code: Optional[str] = None
    page: int = 1
    page_size: int = Field(default=50, ge=1, le=500)


class ForecastFilters(BaseSchema):
    lookback_months: int = Field(default=12, ge=3, le=36)
    horizon_months: int = Field(default=3, ge=1, le=12)
    page: int = 1
    page_size: int = Field(default=50, ge=1, le=500)


class ItineraryTrendPoint(BaseSchema):
    period_start: date
    created_count: int = 0
    closed_count: int = 0
    travel_start_count: int = 0
    travel_end_count: int = 0


class ItineraryTrendsSummary(BaseSchema):
    created_itineraries: int
    closed_itineraries: int
    travel_start_itineraries: int
    travel_end_itineraries: int


class ItineraryTrendsResponse(BaseSchema):
    summary: ItineraryTrendsSummary
    timeline: List[ItineraryTrendPoint]


class ItineraryTrendsFilters(BaseSchema):
    time_window: str = "12m"


class ItineraryLeadFlowPoint(BaseSchema):
    period_start: date
    created_count: int = 0
    closed_won_count: int = 0
    closed_lost_count: int = 0
    conversion_rate: float = 0.0


class ItineraryLeadFlowSummary(BaseSchema):
    created_itineraries: int
    closed_won_itineraries: int
    closed_lost_itineraries: int
    conversion_rate: float


class ItineraryLeadFlowResponse(BaseSchema):
    summary: ItineraryLeadFlowSummary
    timeline: List[ItineraryLeadFlowPoint]


class ItineraryLeadFlowFilters(BaseSchema):
    time_window: str = "12m"