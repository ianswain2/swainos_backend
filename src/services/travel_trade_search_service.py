from __future__ import annotations

from src.repositories.travel_trade_search_repository import TravelTradeSearchRepository
from src.schemas.travel_trade_search import (
    TravelTradeSearchFilters,
    TravelTradeSearchResponse,
    TravelTradeSearchRow,
)


class TravelTradeSearchService:
    def __init__(self, repository: TravelTradeSearchRepository) -> None:
        self.repository = repository

    def search(self, filters: TravelTradeSearchFilters) -> TravelTradeSearchResponse:
        rows = self.repository.search(filters.q, filters.entity_type, filters.limit)
        return TravelTradeSearchResponse(
            query=filters.q,
            entity_type=filters.entity_type,
            results=[
                TravelTradeSearchRow(
                    entity_type=str(row.get("entity_type") or ""),
                    entity_id=str(row.get("entity_id") or ""),
                    entity_external_id=str(row.get("entity_external_id") or ""),
                    display_name=str(row.get("display_name") or ""),
                    email=row.get("email"),
                    agency_name=row.get("agency_name"),
                    iata_code=row.get("iata_code"),
                    host_identifier=row.get("host_identifier"),
                    rank_score=float(row.get("rank_score") or 0),
                )
                for row in rows
            ],
        )
