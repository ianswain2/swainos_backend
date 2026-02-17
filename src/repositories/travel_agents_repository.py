from __future__ import annotations

from datetime import date
from typing import Any, Dict, List, Optional, Tuple

from src.core.supabase import SupabaseClient

MAX_QUERY_ROWS = 5000


class TravelAgentsRepository:
    def __init__(self) -> None:
        self.client = SupabaseClient()

    def get_agent(self, agent_id: str) -> Optional[Dict[str, Any]]:
        rows, _ = self.client.select(
            table="travel_agents",
            select="id,external_id,first_name,last_name,email,agency_id",
            filters=[("id", f"eq.{agent_id}")],
            limit=1,
        )
        return rows[0] if rows else None

    def get_agency_for_agent(self, agency_id: str) -> Optional[Dict[str, Any]]:
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
        agent_id: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        filters: List[Tuple[str, str]] = [
            ("period_start", f"gte.{start_date.isoformat()}"),
            ("period_start", f"lte.{end_date.isoformat()}"),
        ]
        if agent_id:
            filters.append(("agent_id", f"eq.{agent_id}"))
        rows, _ = self.client.select(
            table="travel_agent_monthly_rollup",
            select=(
                "period_start,period_end,agent_id,agent_external_id,agent_name,agent_email,"
                "agency_id,agency_external_id,agency_name,leads_count,converted_leads_count,"
                "traveled_itineraries_count,gross_amount,gross_profit_amount"
            ),
            filters=filters,
            limit=MAX_QUERY_ROWS,
            order="period_start.asc",
        )
        return rows

    def list_affinity_rows(
        self,
        start_date: date,
        end_date: date,
        agent_id: str,
    ) -> List[Dict[str, Any]]:
        rows, _ = self.client.select(
            table="travel_agent_consultant_affinity_monthly_rollup",
            select=(
                "period_start,agent_id,employee_id,employee_external_id,employee_first_name,"
                "employee_last_name,converted_leads_count,closed_won_itineraries_count"
            ),
            filters=[
                ("period_start", f"gte.{start_date.isoformat()}"),
                ("period_start", f"lte.{end_date.isoformat()}"),
                ("agent_id", f"eq.{agent_id}"),
            ],
            limit=MAX_QUERY_ROWS,
            order="period_start.asc",
        )
        return rows

    def list_current_traveling_itineraries(
        self,
        contact_external_id: str,
        as_of: date,
        limit: int = 10,
    ) -> List[Dict[str, Any]]:
        contact_ids = self._list_contact_ids_by_external_id(contact_external_id)
        if contact_ids:
            contact_filter = self._build_uuid_filter(contact_ids)
            if not contact_filter:
                return []
            rows, _ = self.client.select(
                table="itineraries",
                select=(
                    "id,itinerary_number,itinerary_name,itinerary_status,travel_start_date,travel_end_date,"
                    "gross_amount,gross_profit,primary_contact_id"
                ),
                filters=[
                    ("primary_contact_id", contact_filter),
                    ("travel_start_date", f"lte.{as_of.isoformat()}"),
                    ("travel_end_date", f"gte.{as_of.isoformat()}"),
                    ("itinerary_status", "eq.Traveling"),
                ],
                limit=limit,
                order="travel_start_date.asc",
            )
            return rows
        rows, _ = self.client.select(
            table="itineraries",
            select=(
                "id,itinerary_number,itinerary_name,itinerary_status,travel_start_date,travel_end_date,"
                "gross_amount,gross_profit,primary_contact_external_id"
            ),
            filters=[
                ("primary_contact_external_id", f"eq.{contact_external_id}"),
                ("travel_start_date", f"lte.{as_of.isoformat()}"),
                ("travel_end_date", f"gte.{as_of.isoformat()}"),
                ("itinerary_status", "eq.Traveling"),
            ],
            limit=limit,
            order="travel_start_date.asc",
        )
        return rows

    def list_top_open_itineraries(
        self,
        contact_external_id: str,
        open_status_values: List[str],
        limit: int = 5,
    ) -> List[Dict[str, Any]]:
        status_filter = self._build_in_filter(open_status_values)
        if not status_filter:
            return []
        contact_ids = self._list_contact_ids_by_external_id(contact_external_id)
        if contact_ids:
            contact_filter = self._build_uuid_filter(contact_ids)
            if not contact_filter:
                return []
            rows, _ = self.client.select(
                table="itineraries",
                select=(
                    "id,itinerary_number,itinerary_name,itinerary_status,travel_start_date,travel_end_date,"
                    "gross_amount,gross_profit,primary_contact_id"
                ),
                filters=[
                    ("primary_contact_id", contact_filter),
                    ("itinerary_status", status_filter),
                ],
                limit=limit,
                order="gross_profit.desc.nullslast",
            )
            return rows
        rows, _ = self.client.select(
            table="itineraries",
            select=(
                "id,itinerary_number,itinerary_name,itinerary_status,travel_start_date,travel_end_date,"
                "gross_amount,gross_profit,primary_contact_external_id"
            ),
            filters=[
                ("primary_contact_external_id", f"eq.{contact_external_id}"),
                ("itinerary_status", status_filter),
            ],
            limit=limit,
            order="gross_profit.desc.nullslast",
        )
        return rows

    def list_open_status_values(self) -> List[str]:
        rows, _ = self.client.select(
            table="itinerary_status_reference",
            select="status_value,pipeline_bucket,is_filter_out",
            filters=[("is_filter_out", "eq.false"), ("pipeline_bucket", "in.(open,holding)")],
            limit=MAX_QUERY_ROWS,
            order="status_value.asc",
        )
        return [str(row.get("status_value") or "") for row in rows if row.get("status_value")]

    def _list_contact_ids_by_external_id(self, external_id: str) -> List[str]:
        normalized = external_id.strip()
        if not normalized:
            return []
        rows, _ = self.client.select(
            table="contacts",
            select="id",
            filters=[("external_id", f"eq.{normalized}")],
            limit=10,
        )
        return [str(row.get("id") or "") for row in rows if row.get("id")]

    @staticmethod
    def _build_uuid_filter(values: List[str]) -> Optional[str]:
        sanitized_values = [value.strip() for value in values if value and value.strip()]
        if not sanitized_values:
            return None
        escaped_values = [f"\"{value.replace('\"', '\\\"')}\"" for value in sanitized_values]
        return f"in.({','.join(escaped_values)})"

    @staticmethod
    def _build_in_filter(values: List[str]) -> Optional[str]:
        sanitized_values = [value.strip() for value in values if value and value.strip()]
        if not sanitized_values:
            return None
        escaped_values = [f"\"{value.replace('\"', '\\\"')}\"" for value in sanitized_values]
        return f"in.({','.join(escaped_values)})"
