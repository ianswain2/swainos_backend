from __future__ import annotations

from datetime import date
from typing import List

import pytest
from fastapi.testclient import TestClient

from src.api.dependencies import get_revenue_bookings_service
from src.main import create_app
from src.schemas.revenue_bookings import (
    BookingDetail,
    BookingForecastPoint,
    BookingSummary,
    CashFlowSummary,
    CashFlowTimeseriesPoint,
    DepositSummary,
    PaymentOutSummary,
)
from src.schemas.common import Lineage


class FakeRevenueBookingsService:
    def list_bookings(self, **_: object):
        return (
            [
                BookingSummary(
                    id="booking-1",
                    booking_number="BK-1001",
                    service_start_date=date(2026, 2, 1),
                    service_end_date=date(2026, 2, 5),
                    gross_amount=1000,
                    net_amount=800,
                    currency_code="USD",
                    itinerary_id="itinerary-1",
                    lineage=Lineage(
                        source_system="salesforce_kaptio",
                        source_record_id="sf-1",
                        ingested_at="2026-02-01T00:00:00Z",
                    ),
                )
            ],
            1,
        )

    def get_booking(self, booking_id: str) -> BookingDetail:
        summary, _ = self.list_bookings()
        booking = summary[0]
        return BookingDetail(
            **booking.model_dump(),
            supplier_id="supplier-1",
            booking_type="HOTEL",
            service_name="Test Stay",
            location_country="US",
            location_city="New York",
            confirmation_number="CONF-1",
        )

    def get_cashflow_summary(self, *_: object) -> List[CashFlowSummary]:
        return [
            CashFlowSummary(
                currency_code="USD", cash_in_total=1000, cash_out_total=600, net_cash_total=400
            )
        ]

    def get_cashflow_timeseries(self, *_: object) -> List[CashFlowTimeseriesPoint]:
        return [
            CashFlowTimeseriesPoint(
                period_start=date(2026, 2, 1), cash_in=1000, cash_out=600, net_cash=400
            )
        ]

    def get_deposit_summary(self, *_: object) -> List[DepositSummary]:
        return [
            DepositSummary(
                currency_code="USD",
                total_deposits=1000,
                received_deposits=1000,
                outstanding_deposits=0,
            )
        ]

    def get_payments_out_summary(self, *_: object) -> List[PaymentOutSummary]:
        return [
            PaymentOutSummary(
                currency_code="USD",
                total_invoices=800,
                paid_amount=400,
                outstanding_amount=400,
            )
        ]

    def get_booking_forecasts(self, *_: object) -> List[BookingForecastPoint]:
        return [
            BookingForecastPoint(period_start=date(2026, 3, 1), projected_bookings=5, confidence=0.7)
        ]


@pytest.fixture()
def client() -> TestClient:
    app = create_app()
    app.dependency_overrides[get_revenue_bookings_service] = FakeRevenueBookingsService
    return TestClient(app)
