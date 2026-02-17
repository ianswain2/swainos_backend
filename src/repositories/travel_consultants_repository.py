from __future__ import annotations

from datetime import date
from typing import Any, Dict, List, Optional, Tuple

from src.core.supabase import SupabaseClient

MAX_QUERY_ROWS = 5000


class TravelConsultantsRepository:
    def __init__(self) -> None:
        self.client = SupabaseClient()

    def get_employee(self, employee_id: str) -> Optional[Dict[str, Any]]:
        rows, _ = self.client.select(
            table="employees",
            select="id,external_id,first_name,last_name,email",
            filters=[("id", f"eq.{employee_id}"), ("analysis_disabled", "eq.false")],
            limit=1,
        )
        return rows[0] if rows else None

    def list_existing_employee_ids(self, employee_ids: List[str]) -> set[str]:
        normalized_ids = sorted({employee_id for employee_id in employee_ids if employee_id})
        if not normalized_ids:
            return set()
        existing_ids: set[str] = set()
        chunk_size = 100
        for start in range(0, len(normalized_ids), chunk_size):
            chunk = normalized_ids[start : start + chunk_size]
            in_filter = ",".join(chunk)
            rows, _ = self.client.select(
                table="employees",
                select="id",
                filters=[("id", f"in.({in_filter})"), ("analysis_disabled", "eq.false")],
                limit=len(chunk),
            )
            for row in rows:
                employee_id = row.get("id")
                if employee_id:
                    existing_ids.add(str(employee_id))
        return existing_ids

    def list_leaderboard_monthly(
        self,
        start_date: date,
        end_date: date,
        employee_id: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        filters = self._build_period_filters(start_date, end_date, employee_id)
        rows, _ = self.client.select(
            table="mv_travel_consultant_leaderboard_monthly",
            select=(
                "period_start,period_end,employee_id,employee_external_id,first_name,last_name,email,"
                "itinerary_count,pax_count,booked_revenue_amount,gross_profit_amount,"
                "margin_amount,margin_pct,avg_booking_value_amount"
            ),
            filters=filters,
            limit=MAX_QUERY_ROWS,
            order="period_start.asc",
        )
        return rows

    def list_profile_monthly(
        self,
        start_date: date,
        end_date: date,
        employee_id: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        filters = self._build_period_filters(start_date, end_date, employee_id)
        rows, _ = self.client.select(
            table="mv_travel_consultant_profile_monthly",
            select=(
                "period_start,period_end,employee_id,employee_external_id,first_name,last_name,email,"
                "itinerary_count,pax_count,booked_revenue_amount,net_amount,gross_profit_amount,"
                "margin_amount,margin_pct,avg_number_of_days,avg_number_of_nights"
            ),
            filters=filters,
            limit=MAX_QUERY_ROWS,
            order="period_start.asc",
        )
        return rows

    def list_funnel_monthly(
        self,
        start_date: date,
        end_date: date,
        employee_id: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        filters = self._build_period_filters(start_date, end_date, employee_id)
        rows, _ = self.client.select(
            table="mv_travel_consultant_funnel_monthly",
            select=(
                "period_start,period_end,employee_id,employee_external_id,first_name,last_name,email,"
                "lead_count,closed_won_count,closed_lost_count,booked_revenue_amount,median_speed_to_book_days"
            ),
            filters=filters,
            limit=MAX_QUERY_ROWS,
            order="period_start.asc",
        )
        return rows

    def list_compensation_monthly(
        self,
        start_date: date,
        end_date: date,
        employee_id: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        filters = self._build_period_filters(start_date, end_date, employee_id)
        rows, _ = self.client.select(
            table="mv_travel_consultant_compensation_monthly",
            select=(
                "period_start,period_end,employee_id,employee_external_id,first_name,last_name,email,"
                "salary_annual_amount,salary_monthly_amount,commission_rate,gross_profit_amount,"
                "estimated_commission_amount,estimated_total_pay_amount"
            ),
            filters=filters,
            limit=MAX_QUERY_ROWS,
            order="period_start.asc",
        )
        return rows

    def list_open_status_values(self) -> List[str]:
        rows, _ = self.client.select(
            table="itinerary_status_reference",
            select="status_value,pipeline_bucket,is_filter_out",
            filters=[
                ("is_filter_out", "eq.false"),
                ("pipeline_bucket", "in.(open,holding)"),
            ],
            limit=MAX_QUERY_ROWS,
            order="status_value.asc",
        )
        return [str(row.get("status_value") or "") for row in rows if row.get("status_value")]

    def list_closed_won_status_values(self) -> List[str]:
        rows, _ = self.client.select(
            table="itinerary_status_reference",
            select="status_value,pipeline_bucket,is_filter_out",
            filters=[
                ("is_filter_out", "eq.false"),
                ("pipeline_bucket", "eq.closed_won"),
            ],
            limit=MAX_QUERY_ROWS,
            order="status_value.asc",
        )
        return [str(row.get("status_value") or "") for row in rows if row.get("status_value")]

    def list_current_traveling_itineraries(
        self, employee_id: str, as_of: date, limit: int = 10
    ) -> List[Dict[str, Any]]:
        filters: List[Tuple[str, str]] = [
            ("employee_id", f"eq.{employee_id}"),
            ("travel_start_date", f"lte.{as_of.isoformat()}"),
            ("travel_end_date", f"gte.{as_of.isoformat()}"),
            # "Current traveling" should strictly match itineraries actively in Traveling status.
            ("itinerary_status", "eq.Traveling"),
        ]
        rows, _ = self.client.select(
            table="itineraries",
            select=(
                "id,itinerary_number,itinerary_name,itinerary_status,primary_country,"
                "travel_start_date,travel_end_date,gross_amount,pax_count,close_date,created_at"
            ),
            filters=filters,
            limit=limit,
            order="travel_start_date.asc",
        )
        return rows

    def list_top_open_itineraries(
        self, employee_id: str, open_status_values: List[str], limit: int = 5
    ) -> List[Dict[str, Any]]:
        if not open_status_values:
            return []
        status_filter = self._build_in_filter(open_status_values)
        if not status_filter:
            return []
        filters: List[Tuple[str, str]] = [
            ("employee_id", f"eq.{employee_id}"),
            ("itinerary_status", status_filter),
        ]
        rows, _ = self.client.select(
            table="itineraries",
            select=(
                "id,itinerary_number,itinerary_name,itinerary_status,primary_country,"
                "travel_start_date,travel_end_date,gross_amount,pax_count,close_date,created_at"
            ),
            filters=filters,
            limit=limit,
            order="gross_amount.desc.nullslast",
        )
        return rows[:limit]

    def list_closed_won_itineraries_by_travel_period(
        self,
        employee_id: str,
        start_date: date,
        end_date: date,
        closed_won_status_values: List[str],
    ) -> List[Dict[str, Any]]:
        filters: List[Tuple[str, str]] = [
            ("employee_id", f"eq.{employee_id}"),
            ("travel_end_date", f"gte.{start_date.isoformat()}"),
            ("travel_end_date", f"lte.{end_date.isoformat()}"),
        ]
        status_filter = self._build_in_filter(closed_won_status_values)
        if status_filter:
            filters.append(("itinerary_status", status_filter))
        rows, _ = self.client.select(
            table="itineraries",
            select=(
                "id,itinerary_status,created_at,close_date,travel_start_date,travel_end_date,"
                "gross_profit,pax_count,number_of_nights"
            ),
            filters=filters,
            limit=MAX_QUERY_ROWS,
            order="travel_end_date.asc",
        )
        return rows

    @staticmethod
    def _build_period_filters(
        start_date: date, end_date: date, employee_id: Optional[str]
    ) -> List[Tuple[str, str]]:
        filters: List[Tuple[str, str]] = [
            ("period_start", f"gte.{start_date.isoformat()}"),
            ("period_start", f"lte.{end_date.isoformat()}"),
        ]
        if employee_id:
            filters.append(("employee_id", f"eq.{employee_id}"))
        return filters

    @staticmethod
    def _build_in_filter(values: List[str]) -> Optional[str]:
        sanitized_values = [value.strip() for value in values if value and value.strip()]
        if not sanitized_values:
            return None
        escaped_values = [f"\"{value.replace('\"', '\\\"')}\"" for value in sanitized_values]
        return f"in.({','.join(escaped_values)})"
