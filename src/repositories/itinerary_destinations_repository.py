from __future__ import annotations

from datetime import date
from typing import Any, Dict, List, Optional, Tuple

from src.core.supabase import SupabaseClient


class ItineraryDestinationsRepository:
    def __init__(self) -> None:
        self.client = SupabaseClient()

    def list_destination_rollups(
        self,
        year: int,
        country: Optional[str] = None,
        city: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        start_date = date(year, 1, 1)
        end_date = date(year, 12, 31)
        filters: List[Tuple[str, str]] = [
            ("period_start", f"gte.{start_date.isoformat()}"),
            ("period_start", f"lte.{end_date.isoformat()}"),
        ]
        if country:
            filters.append(("location_country", f"ilike.{country}"))
        if city:
            filters.append(("location_city", f"ilike.{city}"))

        rows, _ = self.client.select(
            table="mv_itinerary_destination_booked_monthly",
            select=(
                "period_start,period_end,location_country,location_city,"
                "total_item_count,cancelled_item_count,deleted_item_count,active_item_count,"
                "booked_itinerary_count,booked_quantity,booked_total_cost,booked_total_price,"
                "booked_gross_margin,booked_avg_profit_margin_percent,booked_margin_pct"
            ),
            filters=filters,
            order="period_start.asc",
        )
        return rows
