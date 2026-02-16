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
            filters=[("id", f"eq.{employee_id}")],
            limit=1,
        )
        return rows[0] if rows else None

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
                "itinerary_count,pax_count,booked_revenue_amount,commission_income_amount,"
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
                "itinerary_count,pax_count,booked_revenue_amount,net_amount,commission_income_amount,"
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
                "salary_annual_amount,salary_monthly_amount,commission_rate,commission_income_amount,"
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
        filters: List[Tuple[str, str]] = [("employee_id", f"eq.{employee_id}")]
        rows, _ = self.client.select(
            table="itineraries",
            select=(
                "id,itinerary_number,itinerary_name,itinerary_status,primary_country,"
                "travel_start_date,travel_end_date,gross_amount,pax_count,close_date,created_at"
            ),
            filters=filters,
            # Pull a bounded candidate set then filter in Python so status values with spaces/slashes
            # don't rely on brittle PostgREST in.(...) encoding semantics.
            limit=max(limit * 20, 50),
            order="gross_amount.desc.nullslast",
        )
        open_status_set = {value.strip() for value in open_status_values if value.strip()}
        filtered_rows = [
            row for row in rows if str(row.get("itinerary_status") or "").strip() in open_status_set
        ]
        return filtered_rows[:limit]

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
        closed_won_status_set = {value.strip() for value in closed_won_status_values if value.strip()}
        return [
            row
            for row in rows
            if str(row.get("itinerary_status") or "").strip() in closed_won_status_set
        ]

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
