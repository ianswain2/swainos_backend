from __future__ import annotations

from datetime import date
from typing import Dict, List, Optional, Tuple

from src.core.supabase import SupabaseClient
from src.models.revenue_bookings import (
    ApAgingRecord,
    ApMonthlyOutflowRecord,
    ApOpenLiabilityRecord,
    ApPaymentCalendarRecord,
    ApSummaryRecord,
    BookingRecord,
    CustomerPaymentRecord,
    ItineraryLeadFlowRecord,
    ItineraryTrendRecord,
)


class RevenueBookingsRepository:
    def __init__(self) -> None:
        self.client = SupabaseClient()

    def list_bookings(
        self,
        start_date: Optional[date],
        end_date: Optional[date],
        currency_code: Optional[str],
        page: int,
        page_size: int,
        include_count: bool = True,
    ) -> Tuple[List[BookingRecord], int]:
        filters: List[Tuple[str, str]] = []
        filters.append(("is_deleted", "eq.false"))
        if start_date:
            filters.append(("service_start_date", f"gte.{start_date.isoformat()}"))
        if end_date:
            filters.append(("service_start_date", f"lte.{end_date.isoformat()}"))
        if currency_code:
            filters.append(("currency_code", f"eq.{currency_code}"))

        offset = (page - 1) * page_size
        rows, total = self.client.select(
            table="bookings",
            select="*",
            filters=filters,
            limit=page_size,
            offset=offset,
            order="service_start_date.desc",
            count=include_count,
        )
        records = [BookingRecord.model_validate(row) for row in rows]
        if include_count:
            return records, total or 0
        return records, 0

    def get_booking_by_id(self, booking_id: str) -> Optional[BookingRecord]:
        rows, _ = self.client.select(
            table="bookings",
            select="*",
            filters=[("id", f"eq.{booking_id}")],
            limit=1,
        )
        if not rows:
            return None
        return BookingRecord.model_validate(rows[0])

    def list_customer_payments(
        self,
        start_date: date,
        end_date: date,
        currency_code: Optional[str],
        limit: int = 3000,
    ) -> List[CustomerPaymentRecord]:
        filters = [
            ("payment_date", f"gte.{start_date.isoformat()}"),
            ("payment_date", f"lte.{end_date.isoformat()}"),
        ]
        if currency_code:
            filters.append(("currency_code", f"eq.{currency_code}"))
        rows, _ = self.client.select(
            table="customer_payments", select="*", filters=filters, limit=limit
        )
        return [CustomerPaymentRecord.model_validate(row) for row in rows]

    def list_ap_open_liabilities(
        self,
        start_date: date,
        end_date: date,
        currency_code: Optional[str],
        limit: int = 10000,
    ) -> List[ApOpenLiabilityRecord]:
        filters = [
            ("effective_payment_date", f"gte.{start_date.isoformat()}"),
            ("effective_payment_date", f"lte.{end_date.isoformat()}"),
        ]
        if currency_code:
            filters.append(("currency_code", f"eq.{currency_code}"))
        rows, _ = self.client.select(
            table="ap_open_liability_v1",
            select=(
                "supplier_invoice_line_id,supplier_invoice_line_external_id,supplier_invoice_booking_id,"
                "supplier_invoice_booking_external_id,supplier_invoice_id,supplier_id,supplier_name,"
                "itinerary_id,line_label,service_date,due_date,effective_payment_date,currency_code,outstanding_amount"
            ),
            filters=filters,
            limit=limit,
            order="effective_payment_date.asc",
        )
        return [ApOpenLiabilityRecord.model_validate(row) for row in rows]

    def list_ap_summary(self, currency_code: Optional[str]) -> List[ApSummaryRecord]:
        filters: List[Tuple[str, str]] = []
        if currency_code:
            filters.append(("currency_code", f"eq.{currency_code}"))
        rows, _ = self.client.select(
            table="ap_summary_v1",
            select=(
                "currency_code,open_line_count,open_booking_count,open_supplier_count,"
                "total_outstanding_amount,next_due_date"
            ),
            filters=filters,
            order="currency_code.asc",
            limit=50,
        )
        return [ApSummaryRecord.model_validate(row) for row in rows]

    def list_ap_aging(self, currency_code: Optional[str]) -> List[ApAgingRecord]:
        filters: List[Tuple[str, str]] = []
        if currency_code:
            filters.append(("currency_code", f"eq.{currency_code}"))
        rows, _ = self.client.select(
            table="ap_aging_v1",
            select=(
                "currency_code,open_line_count,total_outstanding_amount,current_not_due_amount,"
                "overdue_1_30_amount,overdue_31_60_amount,overdue_61_90_amount,overdue_90_plus_amount"
            ),
            filters=filters,
            order="currency_code.asc",
            limit=50,
        )
        return [ApAgingRecord.model_validate(row) for row in rows]

    def list_ap_payment_calendar(
        self,
        start_date: date,
        end_date: date,
        currency_code: Optional[str],
        limit: int = 10000,
    ) -> List[ApPaymentCalendarRecord]:
        filters = [
            ("payment_date", f"gte.{start_date.isoformat()}"),
            ("payment_date", f"lte.{end_date.isoformat()}"),
        ]
        if currency_code:
            filters.append(("currency_code", f"eq.{currency_code}"))
        rows, _ = self.client.select(
            table="ap_payment_calendar_v1",
            select="payment_date,currency_code,line_count,supplier_count,amount_due",
            filters=filters,
            order="payment_date.asc",
            limit=limit,
        )
        return [ApPaymentCalendarRecord.model_validate(row) for row in rows]

    def list_ap_monthly_outflow(
        self,
        start_date: date,
        end_date: date,
        currency_code: Optional[str],
        limit: int = 10000,
    ) -> List[ApMonthlyOutflowRecord]:
        start_month = start_date.replace(day=1)
        end_month = end_date.replace(day=1)
        filters = [
            ("month_start", f"gte.{start_month.isoformat()}"),
            ("month_start", f"lte.{end_month.isoformat()}"),
        ]
        if currency_code:
            filters.append(("currency_code", f"eq.{currency_code}"))
        rows, _ = self.client.select(
            table="ap_monthly_outflow_v1",
            select="month_start,currency_code,line_count,supplier_count,amount_due",
            filters=filters,
            order="month_start.asc",
            limit=limit,
        )
        return [ApMonthlyOutflowRecord.model_validate(row) for row in rows]

    def list_ap_pressure(self, currency_code: Optional[str]) -> Dict[str, Dict[str, object]]:
        filters: List[Tuple[str, str]] = []
        if currency_code:
            filters.append(("currency_code", f"eq.{currency_code}"))
        rows, _ = self.client.select(
            table="ap_pressure_30_60_90_v1",
            select=(
                "currency_code,due_7d_amount,due_30d_amount,due_60d_amount,due_90d_amount,"
                "invoices_due_30d_count,next_due_date"
            ),
            filters=filters,
            order="currency_code.asc",
            limit=50,
        )
        pressure_by_currency: Dict[str, Dict[str, object]] = {}
        for row in rows:
            currency = str(row.get("currency_code") or "").upper()
            if not currency:
                continue
            pressure_by_currency[currency] = row
        return pressure_by_currency

    def list_itinerary_trends(
        self, start_date: date, end_date: date
    ) -> List[ItineraryTrendRecord]:
        rows, _ = self.client.select(
            table="mv_itinerary_trends",
            select="period_start,created_count,closed_count,travel_start_count,travel_end_count",
            filters=[
                ("period_start", f"gte.{start_date.isoformat()}"),
                ("period_start", f"lte.{end_date.isoformat()}"),
            ],
            order="period_start.asc",
        )
        return [ItineraryTrendRecord.model_validate(row) for row in rows]

    def list_itinerary_lead_flow(
        self, start_date: date, end_date: date
    ) -> List[ItineraryLeadFlowRecord]:
        rows, _ = self.client.select(
            table="mv_itinerary_lead_flow_monthly",
            select="period_start,created_count,closed_won_count,closed_lost_count",
            filters=[
                ("period_start", f"gte.{start_date.isoformat()}"),
                ("period_start", f"lte.{end_date.isoformat()}"),
            ],
            order="period_start.asc",
        )
        return [ItineraryLeadFlowRecord.model_validate(row) for row in rows]
