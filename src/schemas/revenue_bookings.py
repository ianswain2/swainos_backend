from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import List, Optional

from pydantic import Field

from src.schemas.common import Lineage
from src.shared.base import BaseSchema


class BookingSummary(BaseSchema):
    id: str
    booking_number: Optional[str] = None
    service_start_date: Optional[date] = None
    service_end_date: Optional[date] = None
    gross_amount: Optional[Decimal] = None
    net_amount: Optional[Decimal] = None
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
    total_deposits: Decimal
    received_deposits: Decimal
    outstanding_deposits: Decimal


class PaymentOutSummary(BaseSchema):
    currency_code: str
    total_invoices: Decimal
    paid_amount: Decimal
    outstanding_amount: Decimal


class CashFlowSummary(BaseSchema):
    currency_code: str
    cash_in_total: Decimal
    cash_out_total: Decimal
    net_cash_total: Decimal


class CashFlowTimeseriesPoint(BaseSchema):
    period_start: date
    cash_in: Decimal
    cash_out: Decimal
    net_cash: Decimal


class BookingForecastPoint(BaseSchema):
    period_start: date
    projected_bookings: int
    confidence: float = Field(..., ge=0.0, le=1.0)


class BookingListFilters(BaseSchema):
    start_date: Optional[date] = None
    end_date: Optional[date] = None
    currency_code: Optional[str] = None
    page: int = 1
    page_size: int = Field(default=50, ge=1, le=500)


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