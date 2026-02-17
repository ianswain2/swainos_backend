from __future__ import annotations

from datetime import date
from typing import List, Optional

from pydantic import ConfigDict, Field

from src.schemas.travel_agents import TravelAgentYoySeries
from src.shared.base import BaseSchema


class TravelAgencyLeaderboardFilters(BaseSchema):
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


class TravelAgencyProfileFilters(BaseSchema):
    model_config = ConfigDict(populate_by_name=True, extra="forbid")

    period_type: str = Field(default="year", pattern="^(monthly|rolling12|year)$")
    year: Optional[int] = Field(default=None, ge=2000, le=2100)
    month: Optional[int] = Field(default=None, ge=1, le=12)
    top_n: int = Field(default=10, ge=1, le=50)
    currency_code: Optional[str] = None


class TravelAgencyLeaderboardRow(BaseSchema):
    rank: int
    agency_id: str
    agency_external_id: str
    agency_name: str
    leads_count: int
    converted_leads_count: int
    booked_itineraries_count: int
    gross_amount: float
    gross_profit_amount: float
    active_agents_count: int
    conversion_rate: float


class TravelAgencyLeaderboardResponse(BaseSchema):
    period_start: date
    period_end: date
    period_type: str
    sort_by: str
    sort_order: str
    top_n: int
    rankings: List[TravelAgencyLeaderboardRow]


class TravelAgencyIdentity(BaseSchema):
    agency_id: str
    agency_external_id: str
    agency_name: str
    iata_code: Optional[str] = None
    host_identifier: Optional[str] = None


class TravelAgencyKpis(BaseSchema):
    leads_count: int
    converted_leads_count: int
    booked_itineraries_count: int
    gross_amount: float
    gross_profit_amount: float
    active_agents_count: int
    conversion_rate: float


class TravelAgencyTopAgent(BaseSchema):
    rank: int
    agent_id: str
    agent_external_id: str
    agent_name: str
    agent_email: Optional[str] = None
    leads_count: int
    converted_leads_count: int
    booked_itineraries_count: int
    gross_amount: float
    gross_profit_amount: float


class TravelAgencyProfileResponse(BaseSchema):
    agency: TravelAgencyIdentity
    period_start: date
    period_end: date
    period_type: str
    kpis: TravelAgencyKpis
    yoy_series: List[TravelAgentYoySeries]
    top_agents: List[TravelAgencyTopAgent]
