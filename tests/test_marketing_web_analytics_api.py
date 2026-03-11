from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest
from fastapi.testclient import TestClient

from src.api.authz import get_current_user_access
from src.api.dependencies import get_marketing_web_analytics_service
from src.main import create_app
from src.schemas.auth_access import AuthenticatedUserAccess
from src.schemas.marketing_web_analytics import (
    MarketingAiInsight,
    MarketingEventCatalog,
    MarketingGeoBreakdown,
    MarketingGeoRow,
    MarketingHealth,
    MarketingHealthStatus,
    MarketingOverview,
    MarketingPageActivity,
    MarketingSearchConsoleBreakdownRow,
    MarketingSearchConsoleInsights,
    MarketingSearchConsoleIssue,
    MarketingSearchConsoleOverview,
    MarketingSearchConsolePagePerformance,
    MarketingSearchConsolePageProfile,
    MarketingSearchPerformance,
)


class FakeMarketingWebAnalyticsService:
    def get_overview(self, *, country: str | None = None) -> MarketingOverview:
        _ = country
        return MarketingOverview(
            kpis=[],
            trend=[],
            top_landing_pages=[],
            channels=[],
            events=[],
            search_console_connected=False,
            currency="USD",
            timezone="UTC",
        )

    def get_search_performance(
        self,
        *,
        days_back: int = 30,
        country: str | None = None,
    ) -> MarketingSearchPerformance:
        _ = days_back, country
        return MarketingSearchPerformance(
            top_landing_pages=[],
            channels=[],
            source_mix=[],
            referral_sources=[],
            top_valuable_sources=[],
            internal_site_search_terms=[],
        )

    def get_search_console_insights(
        self,
        *,
        days_back: int = 30,
        country: str | None = None,
    ) -> MarketingSearchConsoleInsights:
        _ = days_back, country
        return MarketingSearchConsoleInsights(
            search_console_connected=False,
            connection_message="Search Console is not connected yet.",
            data_mode="proxy",
            as_of_date=None,
            overview=MarketingSearchConsoleOverview(
                total_clicks=Decimal("0"),
                total_impressions=Decimal("0"),
                average_ctr=Decimal("0"),
                average_position=Decimal("0"),
                freshness_days=None,
            ),
            top_queries=[],
            top_pages=[
                MarketingSearchConsolePagePerformance(
                    page_path="/destinations/botswana",
                    clicks=Decimal("10"),
                    impressions=Decimal("50"),
                    ctr=Decimal("0.2"),
                    average_position=Decimal("5"),
                )
            ],
            country_breakdown=[
                MarketingSearchConsoleBreakdownRow(
                    label="United States",
                    clicks=Decimal("10"),
                    impressions=Decimal("50"),
                    ctr=Decimal("0.2"),
                    average_position=Decimal("5"),
                )
            ],
            device_breakdown=[],
            opportunities=[],
            challenges=[],
            market_benchmarks=[],
            query_intent_buckets=[],
            position_band_summary=[],
            issues=[
                MarketingSearchConsoleIssue(
                    issue_key="search_console_not_connected",
                    label="Search Console not connected",
                    status="critical",
                    detail="Connect Search Console.",
                )
            ],
            organic_landing_pages=[],
            internal_site_search_terms=[],
        )

    def get_search_console_page_profile(
        self,
        *,
        page_path: str,
        days_back: int = 30,
    ) -> MarketingSearchConsolePageProfile:
        _ = page_path, days_back
        return MarketingSearchConsolePageProfile(
            page_path="/destinations/botswana",
            as_of_date=date.today(),
            overview=MarketingSearchConsoleOverview(
                total_clicks=Decimal("10"),
                total_impressions=Decimal("100"),
                average_ctr=Decimal("0.1"),
                average_position=Decimal("4"),
                freshness_days=1,
            ),
            daily_trend=[],
            top_queries=[],
            market_benchmarks=[],
            issues=[],
            recommended_actions=["Improve title tag"],
        )

    def get_ai_insights(self, *, country: str | None = None) -> list[MarketingAiInsight]:
        _ = country
        return [
            MarketingAiInsight(
                insight_id="i-1",
                priority="high",
                category="content",
                focus_area="fix",
                title="Fix page",
                summary="Summary",
                target_label="/landing",
                target_path="/landing",
                owner_hint="marketing",
                primary_metric_label="Engagement Rate",
                impact_score=Decimal("90"),
                confidence_score=Decimal("80"),
                evidence_points=["Sessions: 100"],
                recommended_actions=["Rewrite hero"],
            )
        ]

    def get_page_activity(
        self,
        *,
        page_path_contains: str | None = None,
        limit: int = 100,
        days_back: int = 30,
        country: str | None = None,
    ) -> MarketingPageActivity:
        _ = page_path_contains, limit, days_back, country
        return MarketingPageActivity(
            snapshot_date=date.today(),
            metric_guide="Guide",
            best_pages=[],
            worst_pages=[],
            itinerary_pages=[],
            lookbook_pages=[],
            destination_pages=[],
            all_pages=[],
        )

    def get_geo_breakdown(
        self,
        *,
        days_back: int = 30,
        country: str | None = None,
    ) -> MarketingGeoBreakdown:
        _ = days_back, country
        return MarketingGeoBreakdown(
            snapshot_date=date.today(),
            rows=[],
            top_countries=[
                MarketingGeoRow(
                    snapshot_date=date.today(),
                    country="United States",
                    region=None,
                    city=None,
                    sessions=Decimal("100"),
                    total_users=Decimal("80"),
                    engaged_sessions=Decimal("60"),
                    key_events=Decimal("10"),
                    engagement_rate=Decimal("0.6"),
                    key_event_rate=Decimal("0.1"),
                )
            ],
            demographics=[],
            devices=[],
        )

    def get_event_catalog(self, *, country: str | None = None) -> MarketingEventCatalog:
        _ = country
        return MarketingEventCatalog(snapshot_date=date.today(), events=[])

    def get_health(self) -> MarketingHealth:
        return MarketingHealth(
            statuses=[
                MarketingHealthStatus(
                    key="latestSyncRun",
                    label="Latest Sync Run",
                    status="healthy",
                    detail="ok",
                )
            ],
            last_synced_at=None,
            latest_run_status="success",
        )

    def run_sync(self):
        raise AssertionError("Not used in these tests")


@pytest.fixture()
def client() -> TestClient:
    app = create_app()
    app.dependency_overrides[get_marketing_web_analytics_service] = FakeMarketingWebAnalyticsService
    app.dependency_overrides[get_current_user_access] = lambda: AuthenticatedUserAccess(
        user_id="test-admin-id",
        email="test-admin@example.com",
        role="admin",
        is_admin=True,
        is_active=True,
        permission_keys=["marketing_web_analytics", "search_console_insights"],
        can_manage_access=True,
    )
    return TestClient(app)


def test_marketing_search_time_window_tracks_days_back(client: TestClient) -> None:
    response = client.get("/api/v1/marketing/web-analytics/search?days_back=90")
    assert response.status_code == 200
    payload = response.json()
    assert payload["meta"]["timeWindow"] == "90d"
    assert payload["meta"]["source"] == "ga4"
    assert payload["meta"]["dataStatus"] == "live"


def test_marketing_geo_returns_canonical_country_row(client: TestClient) -> None:
    response = client.get("/api/v1/marketing/web-analytics/geo")
    assert response.status_code == 200
    payload = response.json()
    assert payload["data"]["topCountries"][0]["country"] == "United States"


def test_marketing_geo_meta_reflects_country_scope(client: TestClient) -> None:
    response = client.get("/api/v1/marketing/web-analytics/geo?country=United%20States")
    assert response.status_code == 200
    payload = response.json()
    assert payload["meta"]["marketScope"] == "United States"
    assert payload["meta"]["marketLabel"] == "United States"


def test_marketing_search_console_endpoint_returns_partial_when_disconnected(
    client: TestClient,
) -> None:
    response = client.get("/api/v1/marketing/web-analytics/search-console?days_back=30")
    assert response.status_code == 200
    payload = response.json()
    assert payload["meta"]["source"] == "gsc + supabase"
    assert payload["meta"]["dataStatus"] == "partial"


def test_marketing_search_console_page_profile_endpoint_returns_profile(
    client: TestClient,
) -> None:
    response = client.get(
        "/api/v1/marketing/web-analytics/search-console/page-profile?page_path=/destinations/botswana"
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["data"]["pagePath"] == "/destinations/botswana"
