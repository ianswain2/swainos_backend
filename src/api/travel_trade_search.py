from __future__ import annotations

from datetime import date

from fastapi import APIRouter, Depends, Query

from src.api.dependencies import get_travel_trade_search_service
from src.schemas.travel_trade_search import (
    TravelTradeSearchFilters,
    TravelTradeSearchResponse,
)
from src.services.travel_trade_search_service import TravelTradeSearchService
from src.shared.response import Meta, ResponseEnvelope

router = APIRouter(prefix="/travel-trade", tags=["travel-trade"])


def get_travel_trade_search_filters(
    q: str = Query(min_length=1, max_length=120),
    entity_type: str = Query(default="all", pattern="^(all|agent|agency)$"),
    limit: int = Query(default=10, ge=1, le=50),
) -> TravelTradeSearchFilters:
    return TravelTradeSearchFilters(q=q, entity_type=entity_type, limit=limit)


@router.get("/search")
def travel_trade_search(
    filters: TravelTradeSearchFilters = Depends(get_travel_trade_search_filters),
    service: TravelTradeSearchService = Depends(get_travel_trade_search_service),
) -> ResponseEnvelope[TravelTradeSearchResponse]:
    data = service.search(filters)
    return ResponseEnvelope(
        data=data,
        pagination=None,
        meta=Meta(
            as_of_date=date.today().isoformat(),
            source="travel_trade_search_index",
            time_window="n/a",
            calculation_version="v1",
            currency=None,
        ),
    )
