from __future__ import annotations

from datetime import date
from typing import List, Optional

from pydantic import ConfigDict, Field

from src.shared.base import BaseSchema


class TravelAgentLeaderboardFilters(BaseSchema):
    model_config = ConfigDict(populate_by_name=True, extra="forbid")

    period_type: str = Field(default="year", pattern="^(monthly|rolling12|year)$")
    year: Optional[int] = Field(default=None, ge=2000, le=2100)
    month: Optional[int] = Field(default=None, ge=1, le=12)
    top_n: int = Field(default=10, ge=1, le=50)
    sort_by: str = Field(
        default="gross_profit",
        pattern="^(gross_profit|gross|converted_leads|booked_itineraries|leads)$",
    )
    sort_order: str = Field(default="desc", pattern="^(asc|desc)$")
    currency_code: Optional[str] = None


class TravelAgentProfileFilters(BaseSchema):
    model_config = ConfigDict(populate_by_name=True, extra="forbid")

    period_type: str = Field(default="year", pattern="^(monthly|rolling12|year)$")
    year: Optional[int] = Field(default=None, ge=2000, le=2100)
    month: Optional[int] = Field(default=None, ge=1, le=12)
    top_n: int = Field(default=10, ge=1, le=50)
    currency_code: Optional[str] = None


class TravelAgentLeaderboardRow(BaseSchema):
    rank: int
    agent_id: str
    agent_external_id: str
    agent_name: str
    agent_email: Optional[str] = None
    agency_id: str
    agency_external_id: str
    agency_name: str
    leads_count: int
    converted_leads_count: int
    booked_itineraries_count: int
    gross_amount: float
    gross_profit_amount: float
    conversion_rate: float


class TravelAgentLeaderboardResponse(BaseSchema):
    period_start: date
    period_end: date
    period_type: str
    sort_by: str
    sort_order: str
    top_n: int
    rankings: List[TravelAgentLeaderboardRow]


class TravelAgentIdentity(BaseSchema):
    agent_id: str
    agent_external_id: str
    agent_name: str
    agent_email: Optional[str] = None
    agency_id: str
    agency_external_id: str
    agency_name: str


class TravelAgentKpis(BaseSchema):
    leads_count: int
    converted_leads_count: int
    booked_itineraries_count: int
    gross_amount: float
    gross_profit_amount: float
    conversion_rate: float


class TravelAgentYoyPoint(BaseSchema):
    month: int
    month_label: str
    current_year_value: float
    prior_year_value: float


class TravelAgentYoySeries(BaseSchema):
    metric: str
    current_year: int
    prior_year: int
    points: List[TravelAgentYoyPoint]
    total_current_year_value: float
    total_prior_year_value: float
    yoy_delta_pct: float


class TravelAgentConsultantAffinity(BaseSchema):
    employee_id: str
    employee_external_id: str
    employee_name: str
    converted_leads_count: int
    closed_won_itineraries_count: int


class TravelAgentOperationalItinerary(BaseSchema):
    itinerary_id: str
    itinerary_number: str
    itinerary_name: Optional[str] = None
    itinerary_status: str
    travel_start_date: Optional[date] = None
    travel_end_date: Optional[date] = None
    gross_amount: float
    gross_profit_amount: float


class TravelAgentProfileResponse(BaseSchema):
    agent: TravelAgentIdentity
    period_start: date
    period_end: date
    period_type: str
    kpis: TravelAgentKpis
    yoy_series: List[TravelAgentYoySeries]
    primary_travel_consultants: List[TravelAgentConsultantAffinity]
    current_traveling_files: List[TravelAgentOperationalItinerary]
    top_open_itineraries: List[TravelAgentOperationalItinerary]
