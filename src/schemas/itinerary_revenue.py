from __future__ import annotations

from datetime import date
from typing import List, Optional

from pydantic import ConfigDict, Field

from src.shared.base import BaseSchema


class ItineraryRevenueFilters(BaseSchema):
    # Keep query parameter names in snake_case for API contract consistency.
    model_config = ConfigDict(populate_by_name=True)
    time_window: str = "12m"
    grain: str = Field(default="monthly", pattern="^(weekly|monthly)$")
    currency_code: Optional[str] = None
    years_back: int = Field(default=2, ge=2, le=5)
    actuals_year: Optional[int] = Field(default=None, ge=2000, le=2100)


class ItineraryRevenueOutlookPoint(BaseSchema):
    period_start: date
    period_end: date
    on_books_gross_amount: float
    potential_gross_amount: float
    expected_gross_amount: float
    on_books_gross_profit_amount: float
    potential_gross_profit_amount: float
    expected_gross_profit_amount: float
    on_books_pax_count: int
    potential_pax_count: float
    expected_pax_count: float
    expected_margin_amount: float
    expected_margin_pct: float
    forecast_gross_amount: float
    target_gross_amount: float
    forecast_gross_profit_amount: float
    target_gross_profit_amount: float
    forecast_pax_count: float
    target_pax_count: float


class ItineraryRevenueOutlookSummary(BaseSchema):
    total_on_books_gross_amount: float
    total_potential_gross_amount: float
    total_expected_gross_amount: float
    total_expected_gross_profit_amount: float
    total_expected_margin_amount: float
    total_on_books_pax_count: int
    total_potential_pax_count: float
    total_expected_pax_count: float
    total_forecast_gross_amount: float
    total_target_gross_amount: float
    total_forecast_gross_profit_amount: float
    total_target_gross_profit_amount: float
    total_forecast_pax_count: float
    total_target_pax_count: float


class ItineraryRevenueOutlookResponse(BaseSchema):
    summary: ItineraryRevenueOutlookSummary
    timeline: List[ItineraryRevenueOutlookPoint]
    close_ratio: float


class ItineraryDepositTrendPoint(BaseSchema):
    period_start: date
    period_end: date
    closed_itinerary_count: int
    closed_gross_amount: float
    deposit_received_amount: float
    target_deposit_amount: float
    deposit_gap_amount: float
    deposit_coverage_ratio: float


class ItineraryDepositsResponse(BaseSchema):
    timeline: List[ItineraryDepositTrendPoint]


class ItineraryConversionPoint(BaseSchema):
    period_start: date
    period_end: date
    quoted_count: int
    confirmed_count: int
    close_ratio: float
    projected_confirmed_count: float
    projected_gross_profit_expected: float
    projected_gross_profit_best_case: float
    projected_gross_profit_worst_case: float


class ItineraryConversionResponse(BaseSchema):
    timeline: List[ItineraryConversionPoint]
    lookback_close_ratio: float


class ItineraryChannelPoint(BaseSchema):
    label: str
    itinerary_count: int
    pax_count: int
    gross_amount: float
    gross_profit_amount: float
    margin_amount: float
    trade_commission_amount: float = 0.0


class ItineraryChannelsResponse(BaseSchema):
    top_consortia: List[ItineraryChannelPoint]
    top_trade_agencies: List[ItineraryChannelPoint]


class ItineraryActualsYoyMonthPoint(BaseSchema):
    year: int
    month: int
    month_label: str
    itinerary_count: int
    pax_count: int
    gross_amount: float
    gross_profit_amount: float
    margin_amount: float
    trade_commission_amount: float
    margin_pct: float
    avg_gross_per_itinerary: float
    avg_gross_profit_per_itinerary: float
    avg_gross_per_pax: float
    avg_gross_profit_per_pax: float
    avg_number_of_days: float
    avg_number_of_nights: float
    gross_share_of_year_pct: float
    itinerary_share_of_year_pct: float


class ItineraryActualsYoyYearSummary(BaseSchema):
    year: int
    itinerary_count: int
    pax_count: int
    gross_amount: float
    gross_profit_amount: float
    margin_amount: float
    trade_commission_amount: float
    margin_pct: float
    avg_gross_per_itinerary: float
    avg_gross_profit_per_itinerary: float
    avg_gross_per_pax: float
    avg_gross_profit_per_pax: float
    avg_number_of_days: float
    avg_number_of_nights: float


class ItineraryTradeDirectMonthPoint(BaseSchema):
    period_start: date
    period_end: date
    direct_itinerary_count: int
    trade_itinerary_count: int
    direct_pax_count: int
    trade_pax_count: int
    direct_gross_amount: float
    trade_gross_amount: float
    direct_gross_profit_amount: float
    trade_gross_profit_amount: float
    direct_margin_amount: float
    trade_margin_amount: float


class ItineraryTradeDirectTotals(BaseSchema):
    direct_itinerary_count: int
    trade_itinerary_count: int
    direct_pax_count: int
    trade_pax_count: int
    direct_gross_amount: float
    trade_gross_amount: float
    direct_gross_profit_amount: float
    trade_gross_profit_amount: float
    direct_margin_amount: float
    trade_margin_amount: float


class ItineraryTradeDirectBreakdown(BaseSchema):
    timeline: List[ItineraryTradeDirectMonthPoint]
    totals: ItineraryTradeDirectTotals


class ItineraryActualsYoyResponse(BaseSchema):
    years: List[int]
    timeline: List[ItineraryActualsYoyMonthPoint]
    year_summaries: List[ItineraryActualsYoyYearSummary]
    trade_vs_direct: ItineraryTradeDirectBreakdown
