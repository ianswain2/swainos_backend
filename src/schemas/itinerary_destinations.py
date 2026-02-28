from __future__ import annotations

from datetime import date
from typing import List, Optional

from pydantic import ConfigDict, Field

from src.shared.base import BaseSchema


class ItineraryDestinationFilters(BaseSchema):
    model_config = ConfigDict(populate_by_name=True)
    year: int = Field(default_factory=lambda: date.today().year, ge=2000, le=2100)
    country: Optional[str] = None
    city: Optional[str] = None
    top_n: int = Field(default=10, ge=1, le=100)


class DestinationKpis(BaseSchema):
    active_item_count: int
    booked_itineraries_count: int
    booked_total_price: float
    booked_total_cost: float
    booked_gross_margin: float
    booked_margin_pct: float
    country_count: int
    city_count: int


class DestinationCountrySummaryPoint(BaseSchema):
    country: str
    active_item_count: int
    booked_itineraries_count: int
    booked_total_price: float
    booked_gross_margin: float
    booked_margin_pct: float
    booked_share_pct: float


class ItineraryDestinationSummaryResponse(BaseSchema):
    year: int
    kpis: DestinationKpis
    top_countries: List[DestinationCountrySummaryPoint]


class DestinationTrendPoint(BaseSchema):
    period_start: date
    period_end: date
    active_item_count: int
    booked_itineraries_count: int
    booked_total_price: float
    booked_gross_margin: float
    booked_margin_pct: float


class ItineraryDestinationTrendsResponse(BaseSchema):
    year: int
    country: Optional[str]
    city: Optional[str]
    timeline: List[DestinationTrendPoint]


class DestinationCityBreakdownPoint(BaseSchema):
    city: str
    active_item_count: int
    booked_itineraries_count: int
    booked_total_price: float
    booked_gross_margin: float
    booked_margin_pct: float


class DestinationCountryBreakdownPoint(BaseSchema):
    country: str
    active_item_count: int
    booked_itineraries_count: int
    booked_total_price: float
    booked_gross_margin: float
    booked_margin_pct: float
    top_cities: List[DestinationCityBreakdownPoint]


class ItineraryDestinationBreakdownResponse(BaseSchema):
    year: int
    country: Optional[str]
    countries: List[DestinationCountryBreakdownPoint]


class DestinationMatrixCell(BaseSchema):
    month: int
    revenue_amount: float
    passenger_count: float
    cost_amount: float
    margin_amount: float
    margin_pct: float
    revenue_yoy_pct: Optional[float]
    passenger_yoy_pct: Optional[float]
    cost_yoy_pct: Optional[float]
    margin_yoy_pct: Optional[float]


class DestinationMatrixTotals(BaseSchema):
    revenue_amount: float
    passenger_count: float
    cost_amount: float
    margin_amount: float
    revenue_yoy_pct: Optional[float]
    passenger_yoy_pct: Optional[float]
    cost_yoy_pct: Optional[float]
    margin_yoy_pct: Optional[float]


class DestinationCountryMatrixRow(BaseSchema):
    country: str
    months: List[DestinationMatrixCell]
    totals: DestinationMatrixTotals


class DestinationCityMatrixRow(BaseSchema):
    city: str
    months: List[DestinationMatrixCell]
    totals: DestinationMatrixTotals


class ItineraryDestinationMatrixResponse(BaseSchema):
    year: int
    country: Optional[str]
    months: List[int]
    country_matrix: List[DestinationCountryMatrixRow]
    city_matrix: List[DestinationCityMatrixRow]
