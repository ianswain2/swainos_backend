from __future__ import annotations

from datetime import date, datetime, timedelta
from statistics import median
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
        closed_won_status_values = self.list_closed_won_status_values()
        closed_lost_status_values = self.list_closed_lost_status_values()
        period_end_exclusive = end_date + timedelta(days=1)

        base_employee_filter: List[Tuple[str, str]] = [("employees.analysis_disabled", "eq.false")]
        if employee_id:
            base_employee_filter.append(("employee_id", f"eq.{employee_id}"))

        lead_filters = base_employee_filter + [
            ("created_at", f"gte.{start_date.isoformat()}T00:00:00Z"),
            ("created_at", f"lt.{period_end_exclusive.isoformat()}T00:00:00Z"),
        ]
        lead_rows = self._select_all(
            table="itineraries",
            select=(
                "employee_id,created_at,"
                "employees!inner(external_id,first_name,last_name,email,analysis_disabled)"
            ),
            filters=lead_filters,
            order="created_at.asc",
        )

        won_rows: List[Dict[str, Any]] = []
        won_status_filter = self._build_in_filter(closed_won_status_values)
        if won_status_filter:
            won_filters = base_employee_filter + [
                ("close_date", f"gte.{start_date.isoformat()}"),
                ("close_date", f"lt.{period_end_exclusive.isoformat()}"),
                ("itinerary_status", won_status_filter),
            ]
            won_rows = self._select_all(
                table="itineraries",
                select=(
                    "employee_id,created_at,close_date,gross_amount,"
                    "employees!inner(external_id,first_name,last_name,email,analysis_disabled)"
                ),
                filters=won_filters,
                order="close_date.asc",
            )

        lost_rows: List[Dict[str, Any]] = []
        lost_status_filter = self._build_in_filter(closed_lost_status_values)
        if lost_status_filter:
            lost_filters = base_employee_filter + [
                ("close_date", f"gte.{start_date.isoformat()}"),
                ("close_date", f"lt.{period_end_exclusive.isoformat()}"),
                ("itinerary_status", lost_status_filter),
            ]
            lost_rows = self._select_all(
                table="itineraries",
                select=(
                    "employee_id,close_date,"
                    "employees!inner(external_id,first_name,last_name,email,analysis_disabled)"
                ),
                filters=lost_filters,
                order="close_date.asc",
            )

        aggregate: Dict[Tuple[date, str], Dict[str, Any]] = {}

        def ensure_bucket(period_start: date, row: Dict[str, Any]) -> Dict[str, Any]:
            employee_key = str(row.get("employee_id") or "")
            if not employee_key:
                return {}
            key = (period_start, employee_key)
            bucket = aggregate.get(key)
            if bucket is not None:
                return bucket
            employee_info = row.get("employees") or {}
            bucket = {
                "period_start": period_start.isoformat(),
                "period_end": self._month_end(period_start).isoformat(),
                "employee_id": employee_key,
                "employee_external_id": str(employee_info.get("external_id") or ""),
                "first_name": str(employee_info.get("first_name") or ""),
                "last_name": str(employee_info.get("last_name") or ""),
                "email": str(employee_info.get("email") or ""),
                "lead_count": 0,
                "closed_won_count": 0,
                "closed_lost_count": 0,
                "booked_revenue_amount": 0.0,
                "median_speed_to_book_days": None,
                "_speed_samples": [],
            }
            aggregate[key] = bucket
            return bucket

        for row in lead_rows:
            created_period = self._to_month_start(self._parse_datetime_date(row.get("created_at")))
            if created_period is None:
                continue
            bucket = ensure_bucket(created_period, row)
            if not bucket:
                continue
            bucket["lead_count"] = int(bucket["lead_count"]) + 1

        for row in won_rows:
            close_period = self._to_month_start(self._parse_iso_date(row.get("close_date")))
            if close_period is None:
                continue
            bucket = ensure_bucket(close_period, row)
            if not bucket:
                continue
            bucket["closed_won_count"] = int(bucket["closed_won_count"]) + 1
            bucket["booked_revenue_amount"] = float(bucket["booked_revenue_amount"]) + float(
                row.get("gross_amount") or 0.0
            )
            created_date = self._parse_datetime_date(row.get("created_at"))
            close_date = self._parse_iso_date(row.get("close_date"))
            if created_date and close_date:
                speed_days = (close_date - created_date).days
                if speed_days >= 0:
                    bucket["_speed_samples"].append(float(speed_days))

        for row in lost_rows:
            close_period = self._to_month_start(self._parse_iso_date(row.get("close_date")))
            if close_period is None:
                continue
            bucket = ensure_bucket(close_period, row)
            if not bucket:
                continue
            bucket["closed_lost_count"] = int(bucket["closed_lost_count"]) + 1

        result: List[Dict[str, Any]] = []
        for key in sorted(aggregate.keys()):
            bucket = aggregate[key]
            speed_samples = bucket.pop("_speed_samples", [])
            bucket["booked_revenue_amount"] = round(float(bucket["booked_revenue_amount"]), 2)
            bucket["median_speed_to_book_days"] = (
                round(float(median(speed_samples)), 1) if speed_samples else None
            )
            result.append(bucket)
        return result

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

    def list_closed_lost_status_values(self) -> List[str]:
        rows, _ = self.client.select(
            table="itinerary_status_reference",
            select="status_value,pipeline_bucket,is_filter_out",
            filters=[
                ("is_filter_out", "eq.false"),
                ("pipeline_bucket", "eq.closed_lost"),
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
            ("travel_start_date", f"gte.{start_date.isoformat()}"),
            ("travel_start_date", f"lte.{end_date.isoformat()}"),
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
            order="travel_start_date.asc",
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

    def _select_all(
        self,
        table: str,
        select: str,
        filters: Optional[List[Tuple[str, str]]] = None,
        order: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        offset = 0
        while True:
            chunk, _ = self.client.select(
                table=table,
                select=select,
                filters=filters,
                limit=MAX_QUERY_ROWS,
                offset=offset,
                order=order,
            )
            rows.extend(chunk)
            if len(chunk) < MAX_QUERY_ROWS:
                break
            offset += MAX_QUERY_ROWS
        return rows

    @staticmethod
    def _to_month_start(value: Optional[date]) -> Optional[date]:
        if value is None:
            return None
        return date(value.year, value.month, 1)

    @staticmethod
    def _month_end(period_start: date) -> date:
        if period_start.month == 12:
            return date(period_start.year, 12, 31)
        next_month = date(period_start.year, period_start.month + 1, 1)
        return next_month - timedelta(days=1)

    @staticmethod
    def _parse_iso_date(value: object) -> Optional[date]:
        if value in (None, ""):
            return None
        if isinstance(value, date):
            return value
        try:
            return date.fromisoformat(str(value)[:10])
        except ValueError:
            return None

    @staticmethod
    def _parse_datetime_date(value: object) -> Optional[date]:
        if value in (None, ""):
            return None
        if isinstance(value, datetime):
            return value.date()
        if isinstance(value, date):
            return value
        text = str(value)
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            return datetime.fromisoformat(text).date()
        except ValueError:
            return None
