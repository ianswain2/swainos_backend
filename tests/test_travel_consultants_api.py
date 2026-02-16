from __future__ import annotations

from datetime import date

import pytest
from fastapi.testclient import TestClient

from src.api.dependencies import get_travel_consultants_service
from src.core.errors import NotFoundError
from src.main import create_app
from src.schemas.travel_consultants import (
    TravelConsultantComparisonContext,
    TravelConsultantCompensationImpact,
    TravelConsultantForecastFilters,
    TravelConsultantForecastPoint,
    TravelConsultantForecastResponse,
    TravelConsultantForecastSection,
    TravelConsultantForecastSummary,
    TravelConsultantFunnelHealth,
    TravelConsultantHighlight,
    TravelConsultantIdentity,
    TravelConsultantInsightCard,
    TravelConsultantKpiCard,
    TravelConsultantLeaderboardFilters,
    TravelConsultantLeaderboardResponse,
    TravelConsultantLeaderboardRow,
    TravelConsultantProfileFilters,
    TravelConsultantProfileResponse,
    TravelConsultantSignal,
    TravelConsultantTrendStory,
    TravelConsultantTrendStoryPoint,
)


class FakeTravelConsultantsService:
    def get_leaderboard(
        self, _: TravelConsultantLeaderboardFilters
    ) -> TravelConsultantLeaderboardResponse:
        return TravelConsultantLeaderboardResponse(
            period_start=date(2026, 2, 1),
            period_end=date(2026, 2, 28),
            period_type="monthly",
            domain="travel",
            sort_by="booked_revenue",
            sort_order="desc",
            rankings=[
                TravelConsultantLeaderboardRow(
                    rank=1,
                    employee_id="employee-1",
                    employee_external_id="005AAA",
                    first_name="Alex",
                    last_name="Taylor",
                    email="alex@example.com",
                    itinerary_count=10,
                    pax_count=24,
                    booked_revenue=120000.0,
                    commission_income=84000.0,
                    margin_amount=36000.0,
                    margin_pct=0.30,
                    lead_count=18,
                    closed_won_count=9,
                    closed_lost_count=4,
                    conversion_rate=0.5,
                    close_rate=0.6923,
                    median_speed_to_book_days=32.0,
                    spend_to_book=None,
                    growth_target_variance_pct=0.08,
                )
            ],
            highlights=[
                TravelConsultantHighlight(
                    key="top_mover",
                    title="Top Mover",
                    description="Alex leads target pace.",
                    trend_direction="up",
                    trend_strength="high",
                )
            ],
        )

    def get_profile(self, employee_id: str, _: TravelConsultantProfileFilters) -> TravelConsultantProfileResponse:
        employee = TravelConsultantIdentity(
            employee_id=employee_id,
            employee_external_id="005AAA",
            first_name="Alex",
            last_name="Taylor",
            email="alex@example.com",
        )
        trend_point = TravelConsultantTrendStoryPoint(
            period_start=date(2026, 1, 1),
            period_end=date(2026, 1, 31),
            month_label="Jan",
            current_value=100000.0,
            baseline_value=90000.0,
            yoy_delta_pct=0.1111,
        )
        trend_story = TravelConsultantTrendStory(
            points=[trend_point],
            current_total=100000.0,
            baseline_total=90000.0,
            yoy_delta_pct=0.1111,
        )
        forecast_point = TravelConsultantForecastPoint(
            period_start=date(2026, 3, 1),
            period_end=date(2026, 3, 31),
            projected_revenue_amount=110000.0,
            target_revenue_amount=112000.0,
            growth_gap_pct=-0.0179,
        )
        forecast_summary = TravelConsultantForecastSummary(
            total_projected_revenue_amount=110000.0,
            total_target_revenue_amount=112000.0,
            total_growth_gap_pct=-0.0179,
        )
        return TravelConsultantProfileResponse(
            employee=employee,
            section_order=[
                "heroKpis",
                "trendStory",
                "funnelHealth",
                "forecastAndTarget",
                "compensationImpact",
                "signals",
                "insightCards",
            ],
            hero_kpis=[
                TravelConsultantKpiCard(
                    key="booked_revenue",
                    display_label="Booked Revenue",
                    description="Closed-won realized travel revenue for selected period.",
                    value=120000.0,
                    trend_direction="up",
                    trend_strength="high",
                    is_lagging_indicator=False,
                )
            ],
            trend_story=trend_story,
            funnel_health=TravelConsultantFunnelHealth(
                lead_count=18,
                closed_won_count=9,
                closed_lost_count=4,
                conversion_rate=0.5,
                close_rate=0.6923,
                median_speed_to_book_days=32.0,
            ),
            forecast_and_target=TravelConsultantForecastSection(
                timeline=[forecast_point],
                summary=forecast_summary,
            ),
            compensation_impact=TravelConsultantCompensationImpact(
                salary_annual_amount=95000.0,
                salary_period_amount=7916.67,
                commission_rate=0.15,
                estimated_commission_amount=12600.0,
                estimated_total_pay_amount=20516.67,
            ),
            signals=[
                TravelConsultantSignal(
                    key="growth_target",
                    display_label="12% Growth Trajectory",
                    description="Current year-over-year pace is 11.1% against 12% target.",
                    trend_direction="down",
                    trend_strength="medium",
                    is_lagging_indicator=True,
                )
            ],
            insight_cards=[
                TravelConsultantInsightCard(
                    title="Performance Snapshot",
                    description="Alex handled 10 itineraries in scope with current signal mix.",
                    trend_direction="up",
                    trend_strength="low",
                )
            ],
            comparison_context=TravelConsultantComparisonContext(
                current_period="2026-01-01..2026-01-31",
                baseline_period="2025-01-01..2025-01-31",
                yoy_mode="same_period",
            ),
        )

    def get_forecast(self, employee_id: str, _: TravelConsultantForecastFilters) -> TravelConsultantForecastResponse:
        return TravelConsultantForecastResponse(
            employee=TravelConsultantIdentity(
                employee_id=employee_id,
                employee_external_id="005AAA",
                first_name="Alex",
                last_name="Taylor",
                email="alex@example.com",
            ),
            timeline=[
                TravelConsultantForecastPoint(
                    period_start=date(2026, 3, 1),
                    period_end=date(2026, 3, 31),
                    projected_revenue_amount=110000.0,
                    target_revenue_amount=112000.0,
                    growth_gap_pct=-0.0179,
                )
            ],
            summary=TravelConsultantForecastSummary(
                total_projected_revenue_amount=110000.0,
                total_target_revenue_amount=112000.0,
                total_growth_gap_pct=-0.0179,
            ),
        )


@pytest.fixture()
def client() -> TestClient:
    app = create_app()
    app.dependency_overrides[get_travel_consultants_service] = FakeTravelConsultantsService
    try:
        yield TestClient(app)
    finally:
        app.dependency_overrides.clear()


def test_travel_consultants_leaderboard(client: TestClient) -> None:
    response = client.get("/api/v1/travel-consultants/leaderboard?period_type=monthly&sort_by=booked_revenue")
    assert response.status_code == 200
    payload = response.json()
    assert payload["data"]["rankings"][0]["employeeExternalId"] == "005AAA"
    assert payload["data"]["rankings"][0]["bookedRevenue"] == 120000.0
    assert payload["meta"]["timeWindow"] == "monthly"


def test_travel_consultants_profile(client: TestClient) -> None:
    response = client.get("/api/v1/travel-consultants/employee-1/profile?yoy_mode=same_period")
    assert response.status_code == 200
    payload = response.json()
    assert payload["data"]["employee"]["employeeId"] == "employee-1"
    assert payload["data"]["sectionOrder"][0] == "heroKpis"
    assert payload["data"]["trendStory"]["yoyDeltaPct"] == 0.1111


def test_travel_consultants_forecast(client: TestClient) -> None:
    response = client.get("/api/v1/travel-consultants/employee-1/forecast?horizon_months=12")
    assert response.status_code == 200
    payload = response.json()
    assert payload["data"]["timeline"][0]["projectedRevenueAmount"] == 110000.0
    assert payload["meta"]["timeWindow"] == "12m"


def test_travel_consultants_validation_error(client: TestClient) -> None:
    response = client.get("/api/v1/travel-consultants/leaderboard?period_type=invalid")
    assert response.status_code == 422
    payload = response.json()
    assert payload["error"]["code"] == "validation_error"


def test_travel_consultants_profile_not_found() -> None:
    class MissingEmployeeTravelConsultantsService(FakeTravelConsultantsService):
        def get_profile(
            self, employee_id: str, _: TravelConsultantProfileFilters
        ) -> TravelConsultantProfileResponse:
            raise NotFoundError(f"Travel consultant {employee_id} not found")

    app = create_app()
    app.dependency_overrides[get_travel_consultants_service] = MissingEmployeeTravelConsultantsService
    try:
        test_client = TestClient(app)
        response = test_client.get("/api/v1/travel-consultants/employee-missing/profile")
        assert response.status_code == 404
        payload = response.json()
        assert payload["error"]["code"] == "not_found"
    finally:
        app.dependency_overrides.clear()
