from __future__ import annotations

from datetime import date
from typing import Any, Dict, List

from src.core.supabase import SupabaseClient


class ItineraryRevenueRepository:
    def __init__(self) -> None:
        self.client = SupabaseClient()

    def list_revenue_outlook(
        self, start_date: date, end_date: date, grain: str
    ) -> List[Dict[str, Any]]:
        table = (
            "mv_itinerary_revenue_weekly"
            if grain == "weekly"
            else "mv_itinerary_revenue_monthly"
        )
        rows, _ = self.client.select(
            table=table,
            select=(
                "period_start,period_end,pipeline_bucket,pipeline_category,"
                "itinerary_count,pax_count,gross_amount,commission_income_amount,margin_amount,"
                "commission_amount,trade_commission_amount"
            ),
            filters=[
                ("period_start", f"gte.{start_date.isoformat()}"),
                ("period_start", f"lte.{end_date.isoformat()}"),
            ],
            order="period_start.asc",
        )
        return rows

    def list_deposit_trends(self, start_date: date, end_date: date) -> List[Dict[str, Any]]:
        rows, _ = self.client.select(
            table="mv_itinerary_deposit_monthly",
            select=(
                "period_start,period_end,closed_itinerary_count,closed_gross_amount,"
                "deposit_received_amount,target_deposit_amount,deposit_gap_amount,"
                "deposit_coverage_ratio"
            ),
            filters=[
                ("period_start", f"gte.{start_date.isoformat()}"),
                ("period_start", f"lte.{end_date.isoformat()}"),
            ],
            order="period_start.asc",
        )
        return rows

    def list_consortia_channels(self, start_date: date, end_date: date) -> List[Dict[str, Any]]:
        rows, _ = self.client.select(
            table="mv_itinerary_consortia_monthly",
            select=(
                "consortia,itinerary_count,pax_count,gross_amount,commission_income_amount,margin_amount"
            ),
            filters=[
                ("period_start", f"gte.{start_date.isoformat()}"),
                ("period_start", f"lte.{end_date.isoformat()}"),
            ],
            order="period_start.asc",
        )
        return rows

    def list_trade_agency_channels(self, start_date: date, end_date: date) -> List[Dict[str, Any]]:
        rows, _ = self.client.select(
            table="mv_itinerary_trade_agency_monthly",
            select=(
                "agency_name,itinerary_count,pax_count,gross_amount,commission_income_amount,"
                "net_amount,trade_commission_amount"
            ),
            filters=[
                ("period_start", f"gte.{start_date.isoformat()}"),
                ("period_start", f"lte.{end_date.isoformat()}"),
            ],
            order="period_start.asc",
        )
        return rows

    def list_actuals_yoy(self, start_date: date, end_date: date) -> List[Dict[str, Any]]:
        rows, _ = self.client.select(
            table="mv_itinerary_revenue_monthly",
            select=(
                "period_start,itinerary_count,pax_count,gross_amount,commission_income_amount,margin_amount,"
                "trade_commission_amount,margin_pct,avg_gross_per_itinerary,avg_commission_income_per_itinerary,"
                "avg_gross_per_pax,avg_commission_income_per_pax,avg_number_of_days,avg_number_of_nights"
            ),
            filters=[
                ("period_start", f"gte.{start_date.isoformat()}"),
                ("period_start", f"lte.{end_date.isoformat()}"),
                ("pipeline_bucket", "eq.closed_won"),
            ],
            order="period_start.asc",
        )
        return rows

    def list_actuals_consortia_channels(
        self, start_date: date, end_date: date
    ) -> List[Dict[str, Any]]:
        rows, _ = self.client.select(
            table="mv_itinerary_consortia_actuals_monthly",
            select=(
                "period_start,period_end,consortia,itinerary_count,pax_count,gross_amount,"
                "commission_income_amount,margin_amount"
            ),
            filters=[
                ("period_start", f"gte.{start_date.isoformat()}"),
                ("period_start", f"lte.{end_date.isoformat()}"),
            ],
            order="period_start.asc",
        )
        return rows

    def list_actuals_trade_agency_channels(
        self, start_date: date, end_date: date
    ) -> List[Dict[str, Any]]:
        rows, _ = self.client.select(
            table="mv_itinerary_trade_agency_actuals_monthly",
            select=(
                "agency_name,itinerary_count,pax_count,gross_amount,commission_income_amount,"
                "net_amount,trade_commission_amount"
            ),
            filters=[
                ("period_start", f"gte.{start_date.isoformat()}"),
                ("period_start", f"lte.{end_date.isoformat()}"),
            ],
            order="period_start.asc",
        )
        return rows
