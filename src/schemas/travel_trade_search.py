from __future__ import annotations

from typing import List, Optional

from pydantic import ConfigDict, Field

from src.shared.base import BaseSchema


class TravelTradeSearchFilters(BaseSchema):
    model_config = ConfigDict(populate_by_name=True, extra="forbid")

    q: str = Field(min_length=1, max_length=120)
    entity_type: str = Field(default="all", pattern="^(all|agent|agency)$")
    limit: int = Field(default=10, ge=1, le=50)


class TravelTradeSearchRow(BaseSchema):
    entity_type: str
    entity_id: str
    entity_external_id: str
    display_name: str
    email: Optional[str] = None
    agency_name: Optional[str] = None
    iata_code: Optional[str] = None
    host_identifier: Optional[str] = None
    rank_score: float


class TravelTradeSearchResponse(BaseSchema):
    query: str
    entity_type: str
    results: List[TravelTradeSearchRow]
