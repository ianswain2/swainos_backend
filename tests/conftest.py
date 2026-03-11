from __future__ import annotations

from datetime import date

import pytest
from fastapi.testclient import TestClient

from src.api.authz import get_current_user_access
from src.api.dependencies import get_revenue_bookings_service
from src.main import create_app
from src.schemas.auth_access import AuthenticatedUserAccess
from src.schemas.common import Lineage
from src.schemas.revenue_bookings import (
    BookingDetail,
    BookingForecastPoint,
    BookingSummary,
    CashFlowForecastPoint,
    CashFlowForecastResponse,
    CashFlowSummary,
    CashFlowTimeseriesPoint,
    DepositSummary,
    PaymentOutSummary,
)


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

    def get_cashflow_summary(self, *_: object) -> list[CashFlowSummary]:
        return [
            CashFlowSummary(
                currency_code="USD", cash_in_total=1000, cash_out_total=600, net_cash_total=400
            )
        ]

    def get_cashflow_timeseries(self, *_: object) -> list[CashFlowTimeseriesPoint]:
        return [
            CashFlowTimeseriesPoint(
                period_start=date(2026, 2, 1), cash_in=1000, cash_out=600, net_cash=400
            )
        ]

    def get_cashflow_forecast(self, *_: object) -> list[CashFlowForecastResponse]:
        return [
            CashFlowForecastResponse(
                currency_code="USD",
                time_window="3m",
                points=[
                    CashFlowForecastPoint(
                        period_start=date(2026, 3, 1),
                        period_end=date(2026, 3, 31),
                        cash_in=1200,
                        cash_out=700,
                        net_cash=500,
                        projected_ending_cash=2500,
                        coverage_ratio=1.4,
                        at_risk=False,
                    )
                ],
            )
        ]

    def get_deposit_summary(self, *_: object) -> list[DepositSummary]:
        return [
            DepositSummary(
                currency_code="USD",
                total_deposits=1000,
                received_deposits=1000,
                outstanding_deposits=0,
                available_cash_after_liability=1000,
            )
        ]

    def get_payments_out_summary(self, *_: object) -> list[PaymentOutSummary]:
        return [
            PaymentOutSummary(
                currency_code="USD",
                open_line_count=8,
                total_outstanding_amount=400,
                due_30d_amount=200,
            )
        ]

    def get_booking_forecasts(self, *_: object) -> list[BookingForecastPoint]:
        return [
            BookingForecastPoint(
                period_start=date(2026, 3, 1),
                projected_bookings=5,
                confidence=0.7,
            )
        ]


@pytest.fixture()
def client() -> TestClient:
    app = create_app()
    app.dependency_overrides[get_revenue_bookings_service] = FakeRevenueBookingsService
    app.dependency_overrides[get_current_user_access] = lambda: AuthenticatedUserAccess(
        user_id="test-admin-id",
        email="test-admin@example.com",
        role="admin",
        is_admin=True,
        is_active=True,
        permission_keys=[
            "command_center",
            "ai_insights",
            "itinerary_forecast",
            "itinerary_actuals",
            "destination",
            "travel_consultant",
            "travel_agencies",
            "marketing_web_analytics",
            "search_console_insights",
            "cash_flow",
            "debt_service",
            "fx_command",
            "operations",
            "settings_job_controls",
            "settings_run_logs",
            "settings_user_access",
        ],
        can_manage_access=True,
    )
    return TestClient(app)
