from __future__ import annotations

from datetime import date
from typing import List, Optional, Tuple

from src.analytics.booking_forecast import forecast_bookings
from src.analytics.cash_flow import calculate_cashflow_summary, calculate_cashflow_timeseries
from src.core.errors import NotFoundError
from src.models.revenue_bookings import BookingRecord
from src.repositories.revenue_bookings_repository import RevenueBookingsRepository
from src.schemas.revenue_bookings import (
    BookingDetail,
    BookingForecastPoint,
    BookingSummary,
    CashFlowSummary,
    CashFlowTimeseriesPoint,
    DepositSummary,
    ItineraryTrendPoint,
    ItineraryLeadFlowPoint,
    ItineraryLeadFlowResponse,
    ItineraryLeadFlowSummary,
    ItineraryTrendsResponse,
    ItineraryTrendsSummary,
    PaymentOutSummary,
)
from src.schemas.common import Lineage


class RevenueBookingsService:
    def __init__(self, repository: RevenueBookingsRepository) -> None:
        self.repository = repository

    def list_bookings(
        self,
        start_date: Optional[date],
        end_date: Optional[date],
        currency_code: Optional[str],
        page: int,
        page_size: int,
    ) -> Tuple[List[BookingSummary], int]:
        records, total = self.repository.list_bookings(
            start_date=start_date,
            end_date=end_date,
            currency_code=currency_code,
            page=page,
            page_size=page_size,
        )
        return [self._to_booking_summary(record) for record in records], total

    def get_booking(self, booking_id: str) -> BookingDetail:
        record = self.repository.get_booking_by_id(booking_id)
        if not record:
            raise NotFoundError("Booking not found")
        return self._to_booking_detail(record)

    def get_cashflow_summary(
        self, start_date: date, end_date: date, currency_code: Optional[str]
    ) -> List[CashFlowSummary]:
        payments_in = self.repository.list_customer_payments(
            start_date, end_date, currency_code
        )
        payments_out = self.repository.list_supplier_invoices(
            start_date, end_date, currency_code
        )
        return calculate_cashflow_summary(payments_in, payments_out)

    def get_cashflow_timeseries(
        self, start_date: date, end_date: date, currency_code: Optional[str]
    ) -> List[CashFlowTimeseriesPoint]:
        payments_in = self.repository.list_customer_payments(
            start_date, end_date, currency_code
        )
        payments_out = self.repository.list_supplier_invoices(
            start_date, end_date, currency_code
        )
        return calculate_cashflow_timeseries(payments_in, payments_out)

    def get_deposit_summary(
        self, start_date: date, end_date: date, currency_code: Optional[str]
    ) -> List[DepositSummary]:
        payments_in = self.repository.list_customer_payments(
            start_date, end_date, currency_code
        )
        totals = {}
        for payment in payments_in:
            if not payment.currency_code or payment.amount is None:
                continue
            current = totals.get(payment.currency_code, 0)
            totals[payment.currency_code] = current + payment.amount
        return [
            DepositSummary(
                currency_code=currency,
                total_deposits=amount,
                received_deposits=amount,
                outstanding_deposits=0,
            )
            for currency, amount in totals.items()
        ]

    def get_payments_out_summary(
        self, start_date: date, end_date: date, currency_code: Optional[str]
    ) -> List[PaymentOutSummary]:
        invoices = self.repository.list_supplier_invoices(
            start_date, end_date, currency_code
        )
        totals = {}
        paid_totals = {}
        for invoice in invoices:
            if not invoice.currency_code or invoice.total_amount is None:
                continue
            totals[invoice.currency_code] = totals.get(invoice.currency_code, 0) + invoice.total_amount
            if invoice.paid_amount is not None:
                paid_totals[invoice.currency_code] = paid_totals.get(
                    invoice.currency_code, 0
                ) + invoice.paid_amount
        summaries: List[PaymentOutSummary] = []
        for currency, total in totals.items():
            paid = paid_totals.get(currency, 0)
            summaries.append(
                PaymentOutSummary(
                    currency_code=currency,
                    total_invoices=total,
                    paid_amount=paid,
                    outstanding_amount=total - paid,
                )
            )
        return summaries

    def get_booking_forecasts(
        self, lookback_months: int, horizon_months: int
    ) -> List[BookingForecastPoint]:
        records, _ = self.repository.list_bookings(
            start_date=None,
            end_date=None,
            currency_code=None,
            page=1,
            page_size=1000,
            include_count=False,
        )
        return forecast_bookings(records, lookback_months, horizon_months)

    def get_itinerary_trends(self, start_date: date, end_date: date) -> ItineraryTrendsResponse:
        try:
            records = self.repository.list_itinerary_trends(start_date, end_date)
        except Exception:
            return self._empty_itinerary_trends_response()

        timeline = [
            ItineraryTrendPoint(
                period_start=record.period_start,
                created_count=record.created_count,
                closed_count=record.closed_count,
                travel_start_count=record.travel_start_count,
                travel_end_count=record.travel_end_count,
            )
            for record in records
        ]

        created_total = sum(point.created_count for point in timeline)
        closed_total = sum(point.closed_count for point in timeline)
        travel_start_total = sum(point.travel_start_count for point in timeline)
        travel_end_total = sum(point.travel_end_count for point in timeline)

        summary = ItineraryTrendsSummary(
            created_itineraries=created_total,
            closed_itineraries=closed_total,
            travel_start_itineraries=travel_start_total,
            travel_end_itineraries=travel_end_total,
        )

        return ItineraryTrendsResponse(summary=summary, timeline=timeline)

    def _empty_itinerary_trends_response(self) -> ItineraryTrendsResponse:
        return ItineraryTrendsResponse(
            summary=ItineraryTrendsSummary(
                created_itineraries=0,
                closed_itineraries=0,
                travel_start_itineraries=0,
                travel_end_itineraries=0,
            ),
            timeline=[],
        )

    def get_itinerary_lead_flow(self, start_date: date, end_date: date) -> ItineraryLeadFlowResponse:
        try:
            records = self.repository.list_itinerary_lead_flow(start_date, end_date)
        except Exception:
            return self._empty_itinerary_lead_flow_response()

        timeline: List[ItineraryLeadFlowPoint] = []
        for record in records:
            denominator = record.closed_won_count + record.closed_lost_count
            conversion_rate = (record.closed_won_count / denominator) if denominator else 0.0
            timeline.append(
                ItineraryLeadFlowPoint(
                    period_start=record.period_start,
                    created_count=record.created_count,
                    closed_won_count=record.closed_won_count,
                    closed_lost_count=record.closed_lost_count,
                    conversion_rate=round(conversion_rate, 4),
                )
            )

        created_total = sum(point.created_count for point in timeline)
        closed_won_total = sum(point.closed_won_count for point in timeline)
        closed_lost_total = sum(point.closed_lost_count for point in timeline)
        total_closed = closed_won_total + closed_lost_total
        conversion_rate_total = (closed_won_total / total_closed) if total_closed else 0.0

        summary = ItineraryLeadFlowSummary(
            created_itineraries=created_total,
            closed_won_itineraries=closed_won_total,
            closed_lost_itineraries=closed_lost_total,
            conversion_rate=round(conversion_rate_total, 4),
        )

        return ItineraryLeadFlowResponse(summary=summary, timeline=timeline)

    def _empty_itinerary_lead_flow_response(self) -> ItineraryLeadFlowResponse:
        return ItineraryLeadFlowResponse(
            summary=ItineraryLeadFlowSummary(
                created_itineraries=0,
                closed_won_itineraries=0,
                closed_lost_itineraries=0,
                conversion_rate=0.0,
            ),
            timeline=[],
        )

    def _to_booking_summary(self, record: BookingRecord) -> BookingSummary:
        return BookingSummary(
            id=record.id,
            booking_number=record.booking_number,
            service_start_date=record.service_start_date,
            service_end_date=record.service_end_date,
            gross_amount=record.gross_amount,
            net_amount=record.net_amount,
            currency_code=record.currency_code,
            itinerary_id=record.itinerary_id,
            lineage=Lineage(
                source_system="salesforce_kaptio",
                source_record_id=record.external_id,
                ingested_at=record.synced_at.isoformat() if record.synced_at else None,
            ),
        )

    def _to_booking_detail(self, record: BookingRecord) -> BookingDetail:
        return BookingDetail(
            **self._to_booking_summary(record).model_dump(),
            supplier_id=record.supplier_id,
            booking_type=record.booking_type,
            service_name=record.service_name,
            location_country=record.location_country,
            location_city=record.location_city,
            confirmation_number=record.confirmation_number,
        )


