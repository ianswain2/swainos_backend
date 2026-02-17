from __future__ import annotations

from datetime import date
from typing import Any, Dict, List, Optional, Tuple

from src.core.supabase import SupabaseClient

MAX_QUERY_ROWS = 5000


class TravelAgenciesRepository:
    def __init__(self) -> None:
        self.client = SupabaseClient()

    def get_agency(self, agency_id: str) -> Optional[Dict[str, Any]]:
        rows, _ = self.client.select(
            table="travel_agencies",
            select="id,external_id,agency_name,iata_code,host_identifier",
            filters=[("id", f"eq.{agency_id}")],
            limit=1,
        )
        return rows[0] if rows else None

    def list_rollup_rows(
        self,
        start_date: date,
        end_date: date,
        agency_id: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        filters: List[Tuple[str, str]] = [
            ("period_start", f"gte.{start_date.isoformat()}"),
            ("period_start", f"lte.{end_date.isoformat()}"),
        ]
        if agency_id:
            filters.append(("agency_id", f"eq.{agency_id}"))
        rows, _ = self.client.select(
            table="travel_agency_monthly_rollup",
            select=(
                "period_start,period_end,agency_id,agency_external_id,agency_name,"
                "leads_count,converted_leads_count,traveled_itineraries_count,gross_amount,"
                "gross_profit_amount,active_agents_count"
            ),
            filters=filters,
            limit=MAX_QUERY_ROWS,
            order="period_start.asc",
        )
        return rows

    def list_top_agent_rows(
        self,
        start_date: date,
        end_date: date,
        agency_id: str,
    ) -> List[Dict[str, Any]]:
        rows, _ = self.client.select(
            table="travel_agent_monthly_rollup",
            select=(
                "period_start,agent_id,agent_external_id,agent_name,agent_email,leads_count,converted_leads_count,"
                "traveled_itineraries_count,gross_amount,gross_profit_amount"
            ),
            filters=[
                ("period_start", f"gte.{start_date.isoformat()}"),
                ("period_start", f"lte.{end_date.isoformat()}"),
                ("agency_id", f"eq.{agency_id}"),
            ],
            limit=MAX_QUERY_ROWS,
            order="period_start.asc",
        )
        return rows
