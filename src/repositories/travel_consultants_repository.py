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
