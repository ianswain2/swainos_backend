from __future__ import annotations

from datetime import date
from typing import List, Optional, Tuple

from src.core.supabase import SupabaseClient
from src.models.revenue_bookings import (
    BookingRecord,
    CustomerPaymentRecord,
    ItineraryLeadFlowRecord,
    ItineraryTrendRecord,
    SupplierInvoiceRecord,
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

    def list_supplier_invoices(
        self,
        start_date: date,
        end_date: date,
        currency_code: Optional[str],
        limit: int = 3000,
    ) -> List[SupplierInvoiceRecord]:
        filters = [
            ("invoice_date", f"gte.{start_date.isoformat()}"),
            ("invoice_date", f"lte.{end_date.isoformat()}"),
        ]
        if currency_code:
            filters.append(("currency_code", f"eq.{currency_code}"))
        rows, _ = self.client.select(
            table="supplier_invoices", select="*", filters=filters, limit=limit
        )
        return [SupplierInvoiceRecord.model_validate(row) for row in rows]

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
