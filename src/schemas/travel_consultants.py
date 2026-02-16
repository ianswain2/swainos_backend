from __future__ import annotations

from datetime import date
from typing import List, Optional

from pydantic import ConfigDict, Field

from src.shared.base import BaseSchema


class TravelConsultantLeaderboardFilters(BaseSchema):
    # Keep query parameter names in snake_case for API contract consistency.
    model_config = ConfigDict(populate_by_name=True, extra="forbid")

    period_type: str = Field(default="monthly", pattern="^(monthly|rolling12|year)$")
    domain: str = Field(default="travel", pattern="^(travel|funnel)$")
    year: Optional[int] = Field(default=None, ge=2000, le=2100)
    month: Optional[int] = Field(default=None, ge=1, le=12)
    sort_by: str = Field(
        default="booked_revenue",
        pattern="^(conversion_rate|close_rate|booked_revenue|margin_pct)$",
    )
    sort_order: str = Field(default="desc", pattern="^(asc|desc)$")
    currency_code: Optional[str] = None


class TravelConsultantProfileFilters(BaseSchema):
    # Keep query parameter names in snake_case for API contract consistency.
    model_config = ConfigDict(populate_by_name=True, extra="forbid")

    period_type: str = Field(default="rolling12", pattern="^(monthly|rolling12|year)$")
    year: Optional[int] = Field(default=None, ge=2000, le=2100)
    month: Optional[int] = Field(default=None, ge=1, le=12)
    yoy_mode: str = Field(default="same_period", pattern="^(same_period|full_year)$")
    currency_code: Optional[str] = None


class TravelConsultantForecastFilters(BaseSchema):
    # Keep query parameter names in snake_case for API contract consistency.
    model_config = ConfigDict(populate_by_name=True, extra="forbid")

    horizon_months: int = Field(default=12, ge=1, le=24)
    currency_code: Optional[str] = None


class TravelConsultantIdentity(BaseSchema):
    employee_id: str
    employee_external_id: str
    first_name: str
    last_name: str
    email: str


class TravelConsultantLeaderboardRow(BaseSchema):
    rank: int
    employee_id: str
    employee_external_id: str
    first_name: str
    last_name: str
    email: str
    itinerary_count: int
    pax_count: int
    booked_revenue: float
    commission_income: float
    margin_amount: float
    margin_pct: float
    lead_count: int
    closed_won_count: int
    closed_lost_count: int
    conversion_rate: float
    close_rate: float
    avg_speed_to_book_days: Optional[float] = None
    spend_to_book: Optional[float] = None
    growth_target_variance_pct: float
    yoy_to_date_variance_pct: float


class TravelConsultantHighlight(BaseSchema):
    key: str
    title: str
    description: str
    trend_direction: str
    trend_strength: str


class TravelConsultantLeaderboardResponse(BaseSchema):
    period_start: date
    period_end: date
    period_type: str
    domain: str
    sort_by: str
    sort_order: str
    rankings: List[TravelConsultantLeaderboardRow]
    highlights: List[TravelConsultantHighlight]


class TravelConsultantKpiCard(BaseSchema):
    key: str
    display_label: str
    description: str
    value: float
    trend_direction: str
    trend_strength: str
    is_lagging_indicator: bool = False


class TravelConsultantTrendStoryPoint(BaseSchema):
    period_start: date
    period_end: date
    month_label: str
    current_value: float
    baseline_value: float
    yoy_delta_pct: float


class TravelConsultantTrendStory(BaseSchema):
    points: List[TravelConsultantTrendStoryPoint]
    current_total: float
    baseline_total: float
    yoy_delta_pct: float


class TravelConsultantThreeYearSeries(BaseSchema):
    year: int
    monthly_values: List[float]
    total: float


class TravelConsultantThreeYearVariance(BaseSchema):
    label: str
    monthly_variance_pct: List[float]
    total_variance_pct: float


class TravelConsultantThreeYearMatrix(BaseSchema):
    key: str
    title: str
    metric_label: str
    series: List[TravelConsultantThreeYearSeries]
    variances: List[TravelConsultantThreeYearVariance]


class TravelConsultantThreeYearPerformance(BaseSchema):
    travel_closed_files: TravelConsultantThreeYearMatrix
    lead_funnel: TravelConsultantThreeYearMatrix


class TravelConsultantFunnelHealth(BaseSchema):
    lead_count: int
    closed_won_count: int
    closed_lost_count: int
    conversion_rate: float
    close_rate: float
    avg_speed_to_book_days: Optional[float] = None


class TravelConsultantForecastPoint(BaseSchema):
    period_start: date
    period_end: date
    projected_revenue_amount: float
    target_revenue_amount: float
    growth_gap_pct: float


class TravelConsultantForecastSummary(BaseSchema):
    total_projected_revenue_amount: float
    total_target_revenue_amount: float
    total_growth_gap_pct: float


class TravelConsultantForecastSection(BaseSchema):
    timeline: List[TravelConsultantForecastPoint]
    summary: TravelConsultantForecastSummary


class TravelConsultantCompensationImpact(BaseSchema):
    salary_annual_amount: float
    salary_period_amount: float
    commission_rate: float
    estimated_commission_amount: float
    estimated_total_pay_amount: float


class TravelConsultantSignal(BaseSchema):
    key: str
    display_label: str
    description: str
    trend_direction: str
    trend_strength: str
    is_lagging_indicator: bool = True


class TravelConsultantInsightCard(BaseSchema):
    title: str
    description: str
    trend_direction: str
    trend_strength: str


class TravelConsultantComparisonContext(BaseSchema):
    current_period: str
    baseline_period: str
    yoy_mode: str


class TravelConsultantOperationalItinerary(BaseSchema):
    itinerary_id: str
    itinerary_number: str
    itinerary_name: Optional[str] = None
    itinerary_status: str
    primary_country: Optional[str] = None
    travel_start_date: Optional[date] = None
    travel_end_date: Optional[date] = None
    gross_amount: float
    pax_count: int


class TravelConsultantOperationalSnapshot(BaseSchema):
    current_traveling_files: List[TravelConsultantOperationalItinerary]
    top_open_itineraries: List[TravelConsultantOperationalItinerary]


class TravelConsultantProfileResponse(BaseSchema):
    employee: TravelConsultantIdentity
    section_order: List[str]
    hero_kpis: List[TravelConsultantKpiCard]
    trend_story: TravelConsultantTrendStory
    three_year_performance: TravelConsultantThreeYearPerformance
    ytd_variance_pct: float
    funnel_health: TravelConsultantFunnelHealth
    forecast_and_target: TravelConsultantForecastSection
    compensation_impact: TravelConsultantCompensationImpact
    operational_snapshot: TravelConsultantOperationalSnapshot
    signals: List[TravelConsultantSignal]
    insight_cards: List[TravelConsultantInsightCard]
    comparison_context: TravelConsultantComparisonContext


class TravelConsultantForecastResponse(BaseSchema):
    employee: TravelConsultantIdentity
    timeline: List[TravelConsultantForecastPoint]
    summary: TravelConsultantForecastSummary
