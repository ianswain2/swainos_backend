from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import Any, Literal

import httpx

from src.core.config import get_settings
from src.core.errors import BadRequestError
from src.integrations.google_analytics_client import GoogleAnalyticsClient
from src.integrations.google_search_console_client import GoogleSearchConsoleClient
from src.repositories.marketing_web_analytics_repository import MarketingWebAnalyticsRepository
from src.schemas.marketing_web_analytics import (
    MarketingAiInsight,
    MarketingChannelPerformance,
    MarketingDemographicRow,
    MarketingDeviceRow,
    MarketingEventCatalog,
    MarketingEventCatalogItem,
    MarketingGeoBreakdown,
    MarketingGeoRow,
    MarketingHealth,
    MarketingHealthStatus,
    MarketingInternalSiteSearchTerm,
    MarketingKpi,
    MarketingLandingPagePerformance,
    MarketingOverview,
    MarketingPageActivity,
    MarketingPageActivityRow,
    MarketingSearchConsoleBreakdownRow,
    MarketingSearchConsoleChallenge,
    MarketingSearchConsoleInsights,
    MarketingSearchConsoleIntentBucket,
    MarketingSearchConsoleIssue,
    MarketingSearchConsoleMarketBenchmark,
    MarketingSearchConsoleOpportunity,
    MarketingSearchConsoleOverview,
    MarketingSearchConsolePagePerformance,
    MarketingSearchConsolePageProfile,
    MarketingSearchConsolePageTrendPoint,
    MarketingSearchConsolePositionBand,
    MarketingSearchPerformance,
    MarketingSearchQuery,
    MarketingSourcePerformance,
    MarketingTimeSeriesPoint,
    MarketingTrackingEvent,
    MarketingWebAnalyticsSyncResult,
)


@dataclass(frozen=True)
class _PeriodSummary:
    sessions: Decimal
    users: Decimal
    engaged_sessions: Decimal
    key_events: Decimal
    engagement_rate: Decimal


def _pct_change(current: Decimal, baseline: Decimal) -> Decimal | None:
    if baseline == 0:
        return None
    return (current - baseline) / baseline


def _parse_snapshot_date(value: str) -> date:
    return datetime.strptime(value, "%Y%m%d").date()


EVENT_DEFINITIONS: dict[
    str, tuple[Literal["conversion", "engagement", "navigation", "system", "other"], str, bool]
] = {
    "page_view": ("navigation", "A page load or page route view.", False),
    "session_start": ("system", "A new GA4 session has started.", False),
    "user_engagement": (
        "engagement",
        "User remained active long enough to count as engaged.",
        False,
    ),
    "scroll": ("engagement", "User reached GA4 scroll depth threshold on a page.", False),
    "first_visit": ("system", "First recorded visit for a new user/browser.", False),
    "view_search_results": ("engagement", "User viewed internal site search results.", False),
    "generate_lead": ("conversion", "User completed a lead action.", True),
    "sign_up": ("conversion", "User completed sign-up / account creation.", True),
    "purchase": ("conversion", "User completed a purchase transaction.", True),
    "submit_form": ("conversion", "User submitted a conversion form.", True),
}


MARKETING_ALL_SCOPE = "all"
MARKETING_US_SCOPE = "United States"
GSC_COUNTRY_EXPRESSIONS = {
    "United States": "usa",
    "Australia": "aus",
    "New Zealand": "nzl",
    "South Africa": "zaf",
}


class MarketingWebAnalyticsService:
    def __init__(
        self,
        repository: MarketingWebAnalyticsRepository,
        ga_client: GoogleAnalyticsClient,
    ) -> None:
        self.repository = repository
        self.ga_client = ga_client
        self._gsc_client: GoogleSearchConsoleClient | None = None
        self.settings = get_settings()
        self.logger = logging.getLogger(__name__)

    def _assert_configuration(self) -> None:
        if not (self.settings.google_service_account_key_json or "").strip():
            raise BadRequestError("Google Analytics integration is not configured")
        if not (self.settings.google_ga4_property_id or "").strip():
            raise BadRequestError("GOOGLE_GA4_PROPERTY_ID is required")

    @staticmethod
    def _normalize_country_scope(country: str | None) -> str | None:
        normalized = (country or "").strip()
        if not normalized or normalized.lower() == MARKETING_ALL_SCOPE:
            return None
        return normalized

    def _country_filter(self, country: str | None) -> dict[str, object] | None:
        normalized = self._normalize_country_scope(country)
        if not normalized:
            return None
        return {
            "filter": {
                "fieldName": "country",
                "stringFilter": {"matchType": "EXACT", "value": normalized},
            }
        }

    def _merge_dimension_filters(
        self,
        *,
        country: str | None,
        additional_filter: dict[str, object] | None = None,
    ) -> dict[str, object] | None:
        country_filter = self._country_filter(country)
        if country_filter and additional_filter:
            return {
                "andGroup": {
                    "expressions": [
                        country_filter,
                        additional_filter,
                    ]
                }
            }
        return country_filter or additional_filter

    def _get_search_console_client(self) -> GoogleSearchConsoleClient:
        if self._gsc_client is None:
            self._gsc_client = GoogleSearchConsoleClient()
        return self._gsc_client

    def _assert_search_console_configuration(self) -> None:
        if not (self.settings.google_service_account_key_json or "").strip():
            raise BadRequestError(
                "Google service account credentials are required for Search Console"
            )
        if not (self.settings.google_gsc_site_url or "").strip():
            raise BadRequestError("GOOGLE_GSC_SITE_URL is required for Search Console")

    @staticmethod
    def _country_scope_label(country: str | None) -> str:
        return country or MARKETING_ALL_SCOPE

    @staticmethod
    def _safe_rate(numerator: Decimal, denominator: Decimal) -> Decimal:
        if denominator <= 0:
            return Decimal("0")
        return numerator / denominator

    @staticmethod
    def _weighted_average_position(
        total_position_weight: Decimal, total_impressions: Decimal
    ) -> Decimal:
        if total_impressions <= 0:
            return Decimal("0")
        return total_position_weight / total_impressions

    @staticmethod
    def _is_branded_query(query: str, brand_terms: list[str]) -> bool:
        normalized = query.strip().lower()
        if not normalized:
            return False
        return any(term in normalized for term in brand_terms)

    def _gsc_brand_terms(self) -> list[str]:
        configured = os.getenv("MARKETING_SEARCH_CONSOLE_BRAND_TERMS", "").strip()
        if configured:
            return [term.strip().lower() for term in configured.split(",") if term.strip()]
        return ["swain", "swain destinations", "swaindestinations"]

    @staticmethod
    def _parse_gsc_api_date(value: str | None) -> date | None:
        if not value:
            return None
        try:
            return date.fromisoformat(value)
        except ValueError:
            return None

    def _fetch_search_console_rows(
        self,
        *,
        days_back: int,
        dimensions: list[str],
        country: str | None = None,
        device: str | None = None,
        row_limit: int = 25000,
    ) -> list[dict[str, Any]]:
        client = self._get_search_console_client()
        today = date.today()
        start_date = (today - timedelta(days=max(days_back - 1, 0))).isoformat()
        end_date = today.isoformat()
        country_filter_expression = (
            GSC_COUNTRY_EXPRESSIONS.get(country, country.lower())
            if country and country != MARKETING_ALL_SCOPE
            else None
        )
        return client.query(
            start_date=start_date,
            end_date=end_date,
            dimensions=dimensions,
            country_filter=country_filter_expression,
            device_filter=device,
            row_limit=row_limit,
        )

    def _sync_search_console_snapshots(
        self,
        *,
        days_back: int,
        country: str | None = None,
    ) -> None:
        scoped_country = self._normalize_country_scope(country)
        scope_label = self._country_scope_label(scoped_country)
        brand_terms = self._gsc_brand_terms()

        daily_rows = self._fetch_search_console_rows(
            days_back=days_back,
            dimensions=["date"],
            country=scoped_country,
        )
        query_rows = self._fetch_search_console_rows(
            days_back=days_back,
            dimensions=["date", "query"],
            country=scoped_country,
        )
        page_rows = self._fetch_search_console_rows(
            days_back=days_back,
            dimensions=["date", "page"],
            country=scoped_country,
        )
        page_query_rows = self._fetch_search_console_rows(
            days_back=days_back,
            dimensions=["date", "page", "query"],
            country=scoped_country,
        )
        country_rows = []
        if scoped_country is None:
            country_rows = self._fetch_search_console_rows(
                days_back=days_back, dimensions=["date", "country"]
            )
        device_rows = self._fetch_search_console_rows(
            days_back=days_back, dimensions=["date", "device"], country=scoped_country
        )

        upsert_daily: list[dict[str, Any]] = []
        for row in daily_rows:
            snapshot_date = self._parse_gsc_api_date(str(row.get("date") or ""))
            if not snapshot_date:
                continue
            impressions = Decimal(row.get("impressions", 0))
            clicks = Decimal(row.get("clicks", 0))
            ctr = self._safe_rate(clicks, impressions)
            upsert_daily.append(
                {
                    "snapshot_date": snapshot_date.isoformat(),
                    "country_scope": scope_label,
                    "device_scope": "all",
                    "clicks": clicks,
                    "impressions": impressions,
                    "ctr": ctr,
                    "average_position": Decimal(row.get("position", 0)),
                }
            )
        self.repository.upsert_search_console_daily(upsert_daily)

        upsert_queries: list[dict[str, Any]] = []
        for row in query_rows:
            snapshot_date = self._parse_gsc_api_date(str(row.get("date") or ""))
            query = str(row.get("query") or "").strip()
            if not snapshot_date or not query:
                continue
            impressions = Decimal(row.get("impressions", 0))
            clicks = Decimal(row.get("clicks", 0))
            upsert_queries.append(
                {
                    "snapshot_date": snapshot_date.isoformat(),
                    "query": query,
                    "country_scope": scope_label,
                    "device_scope": "all",
                    "clicks": clicks,
                    "impressions": impressions,
                    "ctr": self._safe_rate(clicks, impressions),
                    "average_position": Decimal(row.get("position", 0)),
                    "is_branded": self._is_branded_query(query, brand_terms),
                }
            )
        self.repository.upsert_search_console_query_daily(upsert_queries)

        upsert_pages: list[dict[str, Any]] = []
        for row in page_rows:
            snapshot_date = self._parse_gsc_api_date(str(row.get("date") or ""))
            page_path = str(row.get("page") or "").strip()
            if not snapshot_date or not page_path:
                continue
            impressions = Decimal(row.get("impressions", 0))
            clicks = Decimal(row.get("clicks", 0))
            upsert_pages.append(
                {
                    "snapshot_date": snapshot_date.isoformat(),
                    "page_path": page_path,
                    "country_scope": scope_label,
                    "device_scope": "all",
                    "clicks": clicks,
                    "impressions": impressions,
                    "ctr": self._safe_rate(clicks, impressions),
                    "average_position": Decimal(row.get("position", 0)),
                }
            )
        self.repository.upsert_search_console_page_daily(upsert_pages)

        upsert_page_queries: list[dict[str, Any]] = []
        for row in page_query_rows:
            snapshot_date = self._parse_gsc_api_date(str(row.get("date") or ""))
            page_path = str(row.get("page") or "").strip()
            query = str(row.get("query") or "").strip()
            if not snapshot_date or not page_path or not query:
                continue
            impressions = Decimal(row.get("impressions", 0))
            clicks = Decimal(row.get("clicks", 0))
            upsert_page_queries.append(
                {
                    "snapshot_date": snapshot_date.isoformat(),
                    "page_path": page_path,
                    "query": query,
                    "country_scope": scope_label,
                    "device_scope": "all",
                    "clicks": clicks,
                    "impressions": impressions,
                    "ctr": self._safe_rate(clicks, impressions),
                    "average_position": Decimal(row.get("position", 0)),
                }
            )
        self.repository.upsert_search_console_page_query_daily(upsert_page_queries)

        if scoped_country is None:
            upsert_countries: list[dict[str, Any]] = []
            for row in country_rows:
                snapshot_date = self._parse_gsc_api_date(str(row.get("date") or ""))
                country_value = str(row.get("country") or "").strip()
                if not snapshot_date or not country_value:
                    continue
                impressions = Decimal(row.get("impressions", 0))
                clicks = Decimal(row.get("clicks", 0))
                upsert_countries.append(
                    {
                        "snapshot_date": snapshot_date.isoformat(),
                        "country": country_value,
                        "clicks": clicks,
                        "impressions": impressions,
                        "ctr": self._safe_rate(clicks, impressions),
                        "average_position": Decimal(row.get("position", 0)),
                    }
                )
            self.repository.upsert_search_console_country_daily(upsert_countries)

        upsert_devices: list[dict[str, Any]] = []
        for row in device_rows:
            snapshot_date = self._parse_gsc_api_date(str(row.get("date") or ""))
            device_value = str(row.get("device") or "").strip()
            if not snapshot_date or not device_value:
                continue
            impressions = Decimal(row.get("impressions", 0))
            clicks = Decimal(row.get("clicks", 0))
            upsert_devices.append(
                {
                    "snapshot_date": snapshot_date.isoformat(),
                    "device": device_value,
                    "clicks": clicks,
                    "impressions": impressions,
                    "ctr": self._safe_rate(clicks, impressions),
                    "average_position": Decimal(row.get("position", 0)),
                }
            )
        self.repository.upsert_search_console_device_daily(upsert_devices)

    def _fetch_daily_totals(self, days_back: int = 800) -> list[dict[str, object]]:
        rows = self.ga_client.run_report(
            start_date=f"{days_back}daysAgo",
            end_date="today",
            metrics=["sessions", "totalUsers", "engagedSessions", "keyEvents"],
            dimensions=["date"],
            limit=5000,
            order_bys=[
                {
                    "dimension": {"dimensionName": "date", "orderType": "ALPHANUMERIC"},
                    "desc": False,
                }
            ],
        )
        mapped: list[dict[str, object]] = []
        for row in rows:
            raw_date = str(row.get("date", ""))
            if not raw_date:
                continue
            sessions = Decimal(row.get("sessions", 0))
            engaged_sessions = Decimal(row.get("engagedSessions", 0))
            mapped.append(
                {
                    "snapshot_date": _parse_snapshot_date(raw_date).isoformat(),
                    "sessions": sessions,
                    "total_users": Decimal(row.get("totalUsers", 0)),
                    "engaged_sessions": engaged_sessions,
                    "engagement_rate": (
                        engaged_sessions / sessions if sessions > 0 else Decimal("0")
                    ),
                    "key_events": Decimal(row.get("keyEvents", 0)),
                    "source_medium": "all",
                    "default_channel_group": "all",
                }
            )
        return mapped

    def _fetch_channel_totals(self, days_back: int = 800) -> list[dict[str, object]]:
        rows = self.ga_client.run_report(
            start_date=f"{days_back}daysAgo",
            end_date="today",
            metrics=["sessions", "totalUsers", "engagedSessions", "keyEvents"],
            dimensions=["date", "sessionDefaultChannelGroup"],
            limit=250000,
            order_bys=[
                {
                    "dimension": {"dimensionName": "date", "orderType": "ALPHANUMERIC"},
                    "desc": False,
                }
            ],
        )
        mapped: list[dict[str, object]] = []
        for row in rows:
            raw_date = str(row.get("date", ""))
            if not raw_date:
                continue
            sessions = Decimal(row.get("sessions", 0))
            engaged_sessions = Decimal(row.get("engagedSessions", 0))
            mapped.append(
                {
                    "snapshot_date": _parse_snapshot_date(raw_date).isoformat(),
                    "source_medium": "all",
                    "default_channel_group": str(
                        row.get("sessionDefaultChannelGroup") or "Unassigned"
                    ),
                    "sessions": sessions,
                    "total_users": Decimal(row.get("totalUsers", 0)),
                    "engaged_sessions": engaged_sessions,
                    "engagement_rate": (
                        engaged_sessions / sessions if sessions > 0 else Decimal("0")
                    ),
                    "key_events": Decimal(row.get("keyEvents", 0)),
                }
            )
        return mapped

    def _fetch_country_totals(self, days_back: int = 800) -> list[dict[str, object]]:
        rows = self.ga_client.run_report(
            start_date=f"{days_back}daysAgo",
            end_date="today",
            metrics=["sessions", "totalUsers", "engagedSessions", "keyEvents"],
            dimensions=["date", "country"],
            limit=250000,
            order_bys=[
                {
                    "dimension": {"dimensionName": "date", "orderType": "ALPHANUMERIC"},
                    "desc": False,
                }
            ],
        )
        mapped: list[dict[str, object]] = []
        for row in rows:
            raw_date = str(row.get("date", ""))
            if not raw_date:
                continue
            sessions = Decimal(row.get("sessions", 0))
            engaged_sessions = Decimal(row.get("engagedSessions", 0))
            key_events = Decimal(row.get("keyEvents", 0))
            mapped.append(
                {
                    "snapshot_date": _parse_snapshot_date(raw_date).isoformat(),
                    "country": str(row.get("country") or "Unknown"),
                    "sessions": sessions,
                    "total_users": Decimal(row.get("totalUsers", 0)),
                    "engaged_sessions": engaged_sessions,
                    "key_events": key_events,
                    "engagement_rate": (
                        engaged_sessions / sessions if sessions > 0 else Decimal("0")
                    ),
                    "key_event_rate": key_events / sessions if sessions > 0 else Decimal("0"),
                }
            )
        return mapped

    def _fetch_channel_window_totals(
        self,
        *,
        days_back: int = 30,
        limit: int = 8,
        country: str | None = None,
    ) -> list[MarketingChannelPerformance]:
        rows = self.ga_client.run_report(
            start_date=f"{days_back}daysAgo",
            end_date="today",
            metrics=["sessions", "totalUsers", "engagedSessions", "keyEvents"],
            dimensions=["sessionDefaultChannelGroup"],
            limit=max(limit, 1),
            order_bys=[{"metric": {"metricName": "sessions"}, "desc": True}],
            dimension_filter=self._merge_dimension_filters(country=country),
        )
        mapped: list[MarketingChannelPerformance] = []
        for row in rows:
            sessions = Decimal(row.get("sessions", 0))
            engaged_sessions = Decimal(row.get("engagedSessions", 0))
            mapped.append(
                MarketingChannelPerformance(
                    channel_name=str(row.get("sessionDefaultChannelGroup") or "Unassigned"),
                    sessions=sessions,
                    total_users=Decimal(row.get("totalUsers", 0)),
                    engagement_rate=engaged_sessions / sessions if sessions > 0 else Decimal("0"),
                    key_events=Decimal(row.get("keyEvents", 0)),
                )
            )
        return mapped

    def _fetch_country_window_totals(
        self,
        *,
        days_back: int = 30,
        limit: int = 12,
        country: str | None = None,
    ) -> list[MarketingGeoRow]:
        rows = self.ga_client.run_report(
            start_date=f"{days_back}daysAgo",
            end_date="today",
            metrics=["sessions", "totalUsers", "engagedSessions", "keyEvents"],
            dimensions=["country"],
            limit=max(limit, 1),
            order_bys=[{"metric": {"metricName": "sessions"}, "desc": True}],
            dimension_filter=self._merge_dimension_filters(country=country),
        )
        snapshot_date = date.today()
        mapped: list[MarketingGeoRow] = []
        for row in rows:
            sessions = Decimal(row.get("sessions", 0))
            engaged_sessions = Decimal(row.get("engagedSessions", 0))
            key_events = Decimal(row.get("keyEvents", 0))
            mapped.append(
                MarketingGeoRow(
                    snapshot_date=snapshot_date,
                    country=str(row.get("country") or "Unknown"),
                    region=None,
                    city=None,
                    sessions=sessions,
                    total_users=Decimal(row.get("totalUsers", 0)),
                    engaged_sessions=engaged_sessions,
                    key_events=key_events,
                    engagement_rate=(engaged_sessions / sessions if sessions > 0 else Decimal("0")),
                    key_event_rate=key_events / sessions if sessions > 0 else Decimal("0"),
                )
            )
        return mapped

    @staticmethod
    def _parse_source_medium(source_medium: str) -> tuple[str, str]:
        source, medium = source_medium, "unknown"
        if " / " in source_medium:
            source, medium = source_medium.split(" / ", 1)
        return source.strip() or "unknown", medium.strip() or "unknown"

    @staticmethod
    def _source_value_score(
        *,
        sessions: Decimal,
        qualified_session_rate: Decimal,
        key_event_rate: Decimal,
        bounce_rate: Decimal,
    ) -> Decimal:
        sample_confidence = min(sessions / Decimal("50"), Decimal("1"))
        traffic_component = min(sessions / Decimal("2000"), Decimal("1"))
        conversion_component = min(key_event_rate / Decimal("0.05"), Decimal("1"))
        qualified_component = min(qualified_session_rate / Decimal("0.70"), Decimal("1"))
        retained_traffic_component = Decimal("1") - min(bounce_rate / Decimal("0.80"), Decimal("1"))
        return (
            (conversion_component * Decimal("45"))
            + (qualified_component * Decimal("25"))
            + (retained_traffic_component * Decimal("15"))
            + (traffic_component * Decimal("15"))
        ) * sample_confidence

    @staticmethod
    def _quality_label(
        *,
        qualified_session_rate: Decimal,
        key_event_rate: Decimal,
        bounce_rate: Decimal,
    ) -> Literal["qualified", "mixed", "poor"]:
        if (
            qualified_session_rate >= Decimal("0.62")
            and key_event_rate >= Decimal("0.03")
            and bounce_rate <= Decimal("0.45")
        ):
            return "qualified"
        if qualified_session_rate >= Decimal("0.45") and bounce_rate <= Decimal("0.65"):
            return "mixed"
        return "poor"

    def _fetch_source_medium_performance(
        self,
        *,
        days_back: int = 30,
        limit: int = 25,
        country: str | None = None,
    ) -> list[MarketingSourcePerformance]:
        rows = self.ga_client.run_report(
            start_date=f"{days_back}daysAgo",
            end_date="today",
            metrics=["sessions", "totalUsers", "engagedSessions", "keyEvents", "bounceRate"],
            dimensions=["sessionSourceMedium", "sessionDefaultChannelGroup"],
            limit=max(limit, 1),
            order_bys=[{"metric": {"metricName": "sessions"}, "desc": True}],
            dimension_filter=self._merge_dimension_filters(country=country),
        )
        mapped: list[MarketingSourcePerformance] = []
        for row in rows:
            source_medium = str(row.get("sessionSourceMedium") or "unknown / unknown")
            source, medium = self._parse_source_medium(source_medium)
            sessions = Decimal(str(row.get("sessions") or 0))
            engaged_sessions = Decimal(str(row.get("engagedSessions") or 0))
            key_events = Decimal(str(row.get("keyEvents") or 0))
            engagement_rate = engaged_sessions / sessions if sessions > 0 else Decimal("0")
            key_event_rate = key_events / sessions if sessions > 0 else Decimal("0")
            bounce_rate = Decimal(str(row.get("bounceRate") or 0))
            qualified_session_rate = engaged_sessions / sessions if sessions > 0 else Decimal("0")
            mapped.append(
                MarketingSourcePerformance(
                    source_label=source_medium,
                    source=source,
                    medium=medium,
                    channel_name=str(row.get("sessionDefaultChannelGroup") or "Unassigned"),
                    sessions=sessions,
                    total_users=Decimal(str(row.get("totalUsers") or 0)),
                    engaged_sessions=engaged_sessions,
                    key_events=key_events,
                    engagement_rate=engagement_rate,
                    key_event_rate=key_event_rate,
                    bounce_rate=bounce_rate,
                    qualified_session_rate=qualified_session_rate,
                    quality_label=self._quality_label(
                        qualified_session_rate=qualified_session_rate,
                        key_event_rate=key_event_rate,
                        bounce_rate=bounce_rate,
                    ),
                    value_score=self._source_value_score(
                        sessions=sessions,
                        qualified_session_rate=qualified_session_rate,
                        key_event_rate=key_event_rate,
                        bounce_rate=bounce_rate,
                    ),
                )
            )
        return mapped

    def _fetch_top_landing_pages(
        self, *, days_back: int = 30, limit: int = 12, country: str | None = None
    ) -> list[MarketingLandingPagePerformance]:
        rows = self.ga_client.run_report(
            start_date=f"{days_back}daysAgo",
            end_date="today",
            metrics=["sessions", "totalUsers", "engagementRate", "keyEvents"],
            dimensions=["landingPage"],
            limit=max(limit, 1),
            order_bys=[{"metric": {"metricName": "sessions"}, "desc": True}],
            dimension_filter=self._merge_dimension_filters(country=country),
        )
        today = date.today()
        return [
            MarketingLandingPagePerformance(
                snapshot_date=today,
                landing_page=str(row.get("landingPage") or "/"),
                sessions=Decimal(row.get("sessions", 0)),
                total_users=Decimal(row.get("totalUsers", 0)),
                engagement_rate=Decimal(row.get("engagementRate", 0)),
                key_events=Decimal(row.get("keyEvents", 0)),
                avg_session_duration_seconds=None,
            )
            for row in rows
        ]

    def _fetch_top_events(
        self,
        *,
        limit: int = 12,
        days_back: int = 30,
        country: str | None = None,
    ) -> list[MarketingTrackingEvent]:
        rows = self.ga_client.run_report(
            start_date=f"{days_back}daysAgo",
            end_date="today",
            metrics=["eventCount", "totalUsers"],
            dimensions=["eventName"],
            limit=max(limit, 1),
            order_bys=[{"metric": {"metricName": "eventCount"}, "desc": True}],
            dimension_filter=self._merge_dimension_filters(country=country),
        )
        today = date.today()
        return [
            MarketingTrackingEvent(
                snapshot_date=today,
                event_name=str(row.get("eventName") or "unknown_event"),
                event_count=Decimal(row.get("eventCount", 0)),
                total_users=Decimal(row.get("totalUsers", 0)),
                event_value_amount=None,
            )
            for row in rows
        ]

    def _fetch_page_activity_breakdown(
        self, days_back: int = 30, country: str | None = None
    ) -> list[dict[str, object]]:
        rows = self.ga_client.run_report(
            start_date=f"{days_back}daysAgo",
            end_date="today",
            metrics=[
                "screenPageViews",
                "sessions",
                "totalUsers",
                "engagedSessions",
                "keyEvents",
                "averageSessionDuration",
            ],
            dimensions=["pagePath"],
            limit=10000,
            order_bys=[{"metric": {"metricName": "screenPageViews"}, "desc": True}],
            dimension_filter=self._merge_dimension_filters(country=country),
        )
        snapshot_date = date.today()
        mapped: list[dict[str, object]] = []
        for row in rows:
            page_path = str(row.get("pagePath") or "/")
            sessions = Decimal(row.get("sessions", 0))
            key_events = Decimal(row.get("keyEvents", 0))
            mapped.append(
                {
                    "snapshot_date": snapshot_date.isoformat(),
                    "page_path": page_path,
                    "page_title": None,
                    "screen_page_views": Decimal(row.get("screenPageViews", 0)),
                    "sessions": sessions,
                    "total_users": Decimal(row.get("totalUsers", 0)),
                    "engaged_sessions": Decimal(row.get("engagedSessions", 0)),
                    "key_events": key_events,
                    "avg_session_duration_seconds": Decimal(row.get("averageSessionDuration", 0)),
                    "engagement_rate": (
                        Decimal(row.get("engagedSessions", 0)) / sessions
                        if sessions > 0
                        else Decimal("0")
                    ),
                    "key_event_rate": key_events / sessions if sessions > 0 else Decimal("0"),
                }
            )
        return mapped

    def _fetch_geo_breakdown(
        self, days_back: int = 30, country: str | None = None
    ) -> list[dict[str, object]]:
        rows = self.ga_client.run_report(
            start_date=f"{days_back}daysAgo",
            end_date="today",
            metrics=["sessions", "totalUsers", "engagedSessions", "engagementRate", "keyEvents"],
            dimensions=["country", "region", "city"],
            limit=5000,
            order_bys=[{"metric": {"metricName": "sessions"}, "desc": True}],
            dimension_filter=self._merge_dimension_filters(country=country),
        )
        snapshot_date = date.today()
        mapped: list[dict[str, object]] = []
        for row in rows:
            sessions = Decimal(row.get("sessions", 0))
            key_events = Decimal(row.get("keyEvents", 0))
            key_event_rate = key_events / sessions if sessions > 0 else Decimal("0")
            mapped.append(
                {
                    "snapshot_date": snapshot_date.isoformat(),
                    "country": str(row.get("country") or "Unknown"),
                    "region": str(row.get("region") or ""),
                    "city": str(row.get("city") or ""),
                    "sessions": sessions,
                    "total_users": Decimal(row.get("totalUsers", 0)),
                    "engaged_sessions": Decimal(row.get("engagedSessions", 0)),
                    "engagement_rate": Decimal(row.get("engagementRate", 0)),
                    "key_events": key_events,
                    "key_event_rate": key_event_rate,
                }
            )
        return mapped

    def _fetch_demographics_breakdown(
        self, days_back: int = 30, country: str | None = None
    ) -> list[MarketingDemographicRow]:
        rows = self.ga_client.run_report(
            start_date=f"{days_back}daysAgo",
            end_date="today",
            metrics=["sessions", "totalUsers", "engagedSessions", "engagementRate", "keyEvents"],
            dimensions=["userAgeBracket", "userGender"],
            limit=2000,
            order_bys=[{"metric": {"metricName": "sessions"}, "desc": True}],
            dimension_filter=self._merge_dimension_filters(country=country),
        )
        today = date.today()
        mapped: list[MarketingDemographicRow] = []
        for row in rows:
            mapped.append(
                MarketingDemographicRow(
                    snapshot_date=today,
                    age_bracket=str(row.get("userAgeBracket") or "unknown"),
                    gender=str(row.get("userGender") or "unknown"),
                    sessions=Decimal(row.get("sessions", 0)),
                    total_users=Decimal(row.get("totalUsers", 0)),
                    engaged_sessions=Decimal(row.get("engagedSessions", 0)),
                    key_events=Decimal(row.get("keyEvents", 0)),
                    engagement_rate=Decimal(row.get("engagementRate", 0)),
                )
            )
        return mapped

    def _fetch_device_breakdown(
        self, days_back: int = 30, country: str | None = None
    ) -> list[MarketingDeviceRow]:
        rows = self.ga_client.run_report(
            start_date=f"{days_back}daysAgo",
            end_date="today",
            metrics=["sessions", "totalUsers", "engagedSessions", "engagementRate", "keyEvents"],
            dimensions=["deviceCategory"],
            limit=100,
            order_bys=[{"metric": {"metricName": "sessions"}, "desc": True}],
            dimension_filter=self._merge_dimension_filters(country=country),
        )
        today = date.today()
        return [
            MarketingDeviceRow(
                snapshot_date=today,
                device_category=str(row.get("deviceCategory") or "unknown"),
                sessions=Decimal(row.get("sessions", 0)),
                total_users=Decimal(row.get("totalUsers", 0)),
                engaged_sessions=Decimal(row.get("engagedSessions", 0)),
                key_events=Decimal(row.get("keyEvents", 0)),
                engagement_rate=Decimal(row.get("engagementRate", 0)),
            )
            for row in rows
        ]

    def _fetch_internal_site_search_terms(
        self,
        days_back: int = 30,
        limit: int = 20,
        country: str | None = None,
    ) -> list[MarketingInternalSiteSearchTerm]:
        rows = self.ga_client.run_report(
            start_date=f"{days_back}daysAgo",
            end_date="today",
            metrics=["eventCount", "totalUsers"],
            dimensions=["searchTerm"],
            limit=max(limit, 1),
            order_bys=[{"metric": {"metricName": "eventCount"}, "desc": True}],
            dimension_filter=self._merge_dimension_filters(
                country=country,
                additional_filter={
                    "filter": {
                        "fieldName": "eventName",
                        "stringFilter": {"matchType": "EXACT", "value": "view_search_results"},
                    }
                },
            ),
        )
        mapped: list[MarketingInternalSiteSearchTerm] = []
        for row in rows:
            search_term = str(row.get("searchTerm") or "").strip()
            if not search_term or search_term == "(not set)":
                continue
            mapped.append(
                MarketingInternalSiteSearchTerm(
                    search_term=search_term,
                    event_count=Decimal(row.get("eventCount", 0)),
                    total_users=Decimal(row.get("totalUsers", 0)),
                )
            )
        return mapped

    def run_sync(self) -> MarketingWebAnalyticsSyncResult:
        self._assert_configuration()
        now = datetime.now(UTC)
        sync_run = self.repository.create_sync_run(
            {
                "source_system": "ga4",
                "status": "running",
                "started_at": now.isoformat(),
                "data_window_start": (date.today() - timedelta(days=800)).isoformat(),
                "data_window_end": date.today().isoformat(),
            }
        )
        run_id = str(sync_run.get("id") or "")
        try:
            partial_failures: list[str] = []

            def capture_optional_section(
                *,
                section_key: str,
                fetcher: Any,
                fallback_value: list[Any],
            ) -> list[Any]:
                try:
                    return fetcher()
                except Exception:
                    partial_failures.append(section_key)
                    self.logger.exception(
                        "marketing_sync_optional_section_failed",
                        extra={"section_key": section_key, "run_id": run_id},
                    )
                    return fallback_value

            daily_rows = self._fetch_daily_totals(days_back=800)
            channel_rows = self._fetch_channel_totals(days_back=800)
            country_rows = self._fetch_country_totals(days_back=800)
            pages = self._fetch_top_landing_pages(days_back=30, limit=20)
            events = self._fetch_top_events(limit=20)
            page_activity = self._fetch_page_activity_breakdown(days_back=30)
            geo_breakdown = self._fetch_geo_breakdown(days_back=30)
            demographics = capture_optional_section(
                section_key="demographics",
                fetcher=lambda: self._fetch_demographics_breakdown(days_back=30),
                fallback_value=[],
            )
            devices = capture_optional_section(
                section_key="devices",
                fetcher=lambda: self._fetch_device_breakdown(days_back=30),
                fallback_value=[],
            )
            internal_terms = capture_optional_section(
                section_key="internal_site_search",
                fetcher=lambda: self._fetch_internal_site_search_terms(days_back=30, limit=40),
                fallback_value=[],
            )

            page_rows = [
                {
                    "snapshot_date": item.snapshot_date.isoformat(),
                    "landing_page": item.landing_page,
                    "sessions": item.sessions,
                    "total_users": item.total_users,
                    "engagement_rate": item.engagement_rate,
                    "key_events": item.key_events,
                    "avg_session_duration_seconds": item.avg_session_duration_seconds,
                }
                for item in pages
            ]
            event_rows = [
                {
                    "snapshot_date": item.snapshot_date.isoformat(),
                    "event_name": item.event_name,
                    "event_count": item.event_count,
                    "total_users": item.total_users,
                    "event_value_amount": item.event_value_amount,
                }
                for item in events
            ]
            demographic_rows = [
                {
                    "snapshot_date": item.snapshot_date.isoformat(),
                    "age_bracket": item.age_bracket,
                    "gender": item.gender,
                    "sessions": item.sessions,
                    "total_users": item.total_users,
                    "engaged_sessions": item.engaged_sessions,
                    "key_events": item.key_events,
                    "engagement_rate": item.engagement_rate,
                }
                for item in demographics
            ]
            device_rows = [
                {
                    "snapshot_date": item.snapshot_date.isoformat(),
                    "device_category": item.device_category,
                    "sessions": item.sessions,
                    "total_users": item.total_users,
                    "engaged_sessions": item.engaged_sessions,
                    "key_events": item.key_events,
                    "engagement_rate": item.engagement_rate,
                }
                for item in devices
            ]
            internal_search_rows = [
                {
                    "snapshot_date": date.today().isoformat(),
                    "search_term": item.search_term,
                    "event_count": item.event_count,
                    "total_users": item.total_users,
                }
                for item in internal_terms
            ]
            overview_period_rows = self._build_overview_period_rows(as_of_date=date.today())

            self.repository.upsert_daily_snapshots(daily_rows)
            self.repository.upsert_channel_snapshots(channel_rows)
            self.repository.upsert_country_snapshots(country_rows)
            self.repository.upsert_landing_page_snapshots(page_rows)
            self.repository.upsert_event_snapshots(event_rows)
            self.repository.upsert_page_activity_snapshots(page_activity)
            self.repository.upsert_geo_snapshots(geo_breakdown)
            self.repository.upsert_demographic_snapshots(demographic_rows)
            self.repository.upsert_device_snapshots(device_rows)
            self.repository.upsert_internal_search_snapshots(internal_search_rows)
            self.repository.upsert_overview_period_summaries(overview_period_rows)

            records_processed = (
                len(daily_rows)
                + len(channel_rows)
                + len(country_rows)
                + len(page_rows)
                + len(event_rows)
                + len(page_activity)
                + len(geo_breakdown)
                + len(demographic_rows)
                + len(device_rows)
                + len(internal_search_rows)
                + len(overview_period_rows)
            )
            run_status = "partial" if partial_failures else "success"
            run_message = "GA4 marketing analytics snapshots refreshed"
            if partial_failures:
                run_message = (
                    "GA4 marketing analytics snapshots refreshed with partial data gaps in: "
                    f"{', '.join(partial_failures)}"
                )
            self.repository.update_sync_run(
                run_id,
                {
                    "status": run_status,
                    "records_processed": records_processed,
                    "records_created": records_processed,
                    "error_message": "; ".join(partial_failures) if partial_failures else None,
                    "completed_at": datetime.now(UTC).isoformat(),
                },
            )
            return MarketingWebAnalyticsSyncResult(
                run_id=run_id or "n/a",
                status=run_status,
                records_processed=records_processed,
                records_created=records_processed,
                message=run_message,
            )
        except Exception as exc:
            if run_id:
                self.repository.update_sync_run(
                    run_id,
                    {
                        "status": "failed",
                        "error_message": str(exc),
                        "completed_at": datetime.now(UTC).isoformat(),
                    },
                )
            raise

    def _build_kpis(
        self,
        current: _PeriodSummary,
        previous: _PeriodSummary,
        year_ago: _PeriodSummary,
        today_summary: _PeriodSummary,
        yesterday_summary: _PeriodSummary,
    ) -> list[MarketingKpi]:
        return [
            MarketingKpi(
                metric_key="sessions",
                label="Sessions",
                format="integer",
                current_value=current.sessions,
                previous_value=previous.sessions,
                year_ago_value=year_ago.sessions,
                day_over_day_delta_pct=_pct_change(
                    today_summary.sessions, yesterday_summary.sessions
                ),
                month_over_month_delta_pct=_pct_change(current.sessions, previous.sessions),
                year_over_year_delta_pct=_pct_change(current.sessions, year_ago.sessions),
            ),
            MarketingKpi(
                metric_key="totalUsers",
                label="Users",
                format="integer",
                current_value=current.users,
                previous_value=previous.users,
                year_ago_value=year_ago.users,
                day_over_day_delta_pct=_pct_change(today_summary.users, yesterday_summary.users),
                month_over_month_delta_pct=_pct_change(current.users, previous.users),
                year_over_year_delta_pct=_pct_change(current.users, year_ago.users),
            ),
            MarketingKpi(
                metric_key="engagementRate",
                label="Engagement Rate",
                format="percent",
                current_value=current.engagement_rate,
                previous_value=previous.engagement_rate,
                year_ago_value=year_ago.engagement_rate,
                day_over_day_delta_pct=_pct_change(
                    today_summary.engagement_rate, yesterday_summary.engagement_rate
                ),
                month_over_month_delta_pct=_pct_change(
                    current.engagement_rate, previous.engagement_rate
                ),
                year_over_year_delta_pct=_pct_change(
                    current.engagement_rate, year_ago.engagement_rate
                ),
            ),
            MarketingKpi(
                metric_key="keyEvents",
                label="Key Events",
                format="integer",
                current_value=current.key_events,
                previous_value=previous.key_events,
                year_ago_value=year_ago.key_events,
                day_over_day_delta_pct=_pct_change(
                    today_summary.key_events, yesterday_summary.key_events
                ),
                month_over_month_delta_pct=_pct_change(current.key_events, previous.key_events),
                year_over_year_delta_pct=_pct_change(current.key_events, year_ago.key_events),
            ),
            MarketingKpi(
                metric_key="engagedSessions",
                label="Engaged Sessions",
                format="integer",
                current_value=current.engaged_sessions,
                previous_value=previous.engaged_sessions,
                year_ago_value=year_ago.engaged_sessions,
                day_over_day_delta_pct=_pct_change(
                    today_summary.engaged_sessions, yesterday_summary.engaged_sessions
                ),
                month_over_month_delta_pct=_pct_change(
                    current.engaged_sessions, previous.engaged_sessions
                ),
                year_over_year_delta_pct=_pct_change(
                    current.engaged_sessions, year_ago.engaged_sessions
                ),
            ),
        ]

    @staticmethod
    def _empty_summary() -> _PeriodSummary:
        return _PeriodSummary(
            sessions=Decimal("0"),
            users=Decimal("0"),
            engaged_sessions=Decimal("0"),
            key_events=Decimal("0"),
            engagement_rate=Decimal("0"),
        )

    def _fetch_daily_trend_live(
        self,
        *,
        days_back: int = 800,
        country: str | None = None,
    ) -> list[MarketingTimeSeriesPoint]:
        rows = self.ga_client.run_report(
            start_date=f"{days_back}daysAgo",
            end_date="today",
            metrics=["sessions", "totalUsers", "engagedSessions", "keyEvents"],
            dimensions=["date"],
            limit=max(days_back + 5, 100),
            order_bys=[
                {
                    "dimension": {"dimensionName": "date", "orderType": "ALPHANUMERIC"},
                    "desc": False,
                }
            ],
            dimension_filter=self._merge_dimension_filters(country=country),
        )
        points: list[MarketingTimeSeriesPoint] = []
        for row in rows:
            raw_date = str(row.get("date") or "")
            if not raw_date:
                continue
            snapshot_date = _parse_snapshot_date(raw_date)
            sessions = Decimal(str(row.get("sessions") or 0))
            engaged_sessions = Decimal(str(row.get("engagedSessions") or 0))
            points.append(
                MarketingTimeSeriesPoint(
                    snapshot_date=snapshot_date,
                    sessions=sessions,
                    total_users=Decimal(str(row.get("totalUsers") or 0)),
                    engaged_sessions=engaged_sessions,
                    key_events=Decimal(str(row.get("keyEvents") or 0)),
                    engagement_rate=engaged_sessions / sessions if sessions > 0 else Decimal("0"),
                )
            )
        return points

    def _fetch_period_summary_live(
        self,
        *,
        start_date: date,
        end_date: date,
        country: str | None = None,
    ) -> _PeriodSummary:
        rows = self.ga_client.run_report(
            start_date=start_date.isoformat(),
            end_date=end_date.isoformat(),
            metrics=["sessions", "totalUsers", "engagedSessions", "keyEvents"],
            dimensions=[],
            limit=1,
            dimension_filter=self._merge_dimension_filters(country=country),
        )
        if not rows:
            return self._empty_summary()
        row = rows[0]
        sessions = Decimal(str(row.get("sessions") or 0))
        engaged_sessions = Decimal(str(row.get("engagedSessions") or 0))
        return _PeriodSummary(
            sessions=sessions,
            users=Decimal(str(row.get("totalUsers") or 0)),
            engaged_sessions=engaged_sessions,
            key_events=Decimal(str(row.get("keyEvents") or 0)),
            engagement_rate=engaged_sessions / sessions if sessions > 0 else Decimal("0"),
        )

    def _build_overview_period_rows(self, *, as_of_date: date) -> list[dict[str, object]]:
        windows = {
            "current_30d": (as_of_date - timedelta(days=29), as_of_date),
            "previous_30d": (as_of_date - timedelta(days=59), as_of_date - timedelta(days=30)),
            "year_ago_30d": (as_of_date - timedelta(days=394), as_of_date - timedelta(days=365)),
            "today": (as_of_date, as_of_date),
            "yesterday": (as_of_date - timedelta(days=1), as_of_date - timedelta(days=1)),
        }
        rows: list[dict[str, object]] = []
        for summary_key, (start_date, end_date) in windows.items():
            summary = self._fetch_period_summary_live(start_date=start_date, end_date=end_date)
            rows.append(
                {
                    "as_of_date": as_of_date.isoformat(),
                    "summary_key": summary_key,
                    "start_date": start_date.isoformat(),
                    "end_date": end_date.isoformat(),
                    "sessions": summary.sessions,
                    "total_users": summary.users,
                    "engaged_sessions": summary.engaged_sessions,
                    "key_events": summary.key_events,
                    "engagement_rate": summary.engagement_rate,
                }
            )
        return rows

    def _latest_overview_period_summaries(self, *, as_of_date: date) -> dict[str, _PeriodSummary]:
        rows = self.repository.list_latest_overview_period_summaries(limit=20)
        filtered = [row for row in rows if str(row.get("as_of_date")) == as_of_date.isoformat()]
        summaries: dict[str, _PeriodSummary] = {}
        for row in filtered:
            key = str(row.get("summary_key") or "")
            if not key:
                continue
            summaries[key] = _PeriodSummary(
                sessions=Decimal(str(row.get("sessions") or 0)),
                users=Decimal(str(row.get("total_users") or 0)),
                engaged_sessions=Decimal(str(row.get("engaged_sessions") or 0)),
                key_events=Decimal(str(row.get("key_events") or 0)),
                engagement_rate=Decimal(str(row.get("engagement_rate") or 0)),
            )
        return summaries

    def _load_daily_trend(self) -> list[MarketingTimeSeriesPoint]:
        rows = self.repository.list_latest_daily_snapshots(limit=900)
        points = [
            MarketingTimeSeriesPoint(
                snapshot_date=datetime.strptime(str(row.get("snapshot_date")), "%Y-%m-%d").date(),
                sessions=Decimal(str(row.get("sessions") or 0)),
                total_users=Decimal(str(row.get("total_users") or 0)),
                engaged_sessions=Decimal(str(row.get("engaged_sessions") or 0)),
                key_events=Decimal(str(row.get("key_events") or 0)),
                engagement_rate=Decimal(str(row.get("engagement_rate") or 0)),
            )
            for row in rows
        ]
        return list(sorted(points, key=lambda item: item.snapshot_date))

    def _latest_pages(self, limit: int) -> list[MarketingLandingPagePerformance]:
        rows = self.repository.list_latest_landing_pages(limit=max(limit * 3, 30))
        if not rows:
            return []
        latest_date = str(rows[0].get("snapshot_date"))
        filtered = [row for row in rows if str(row.get("snapshot_date")) == latest_date][:limit]
        return [
            MarketingLandingPagePerformance(
                snapshot_date=datetime.strptime(str(row.get("snapshot_date")), "%Y-%m-%d").date(),
                landing_page=str(row.get("landing_page") or "/"),
                sessions=Decimal(str(row.get("sessions") or 0)),
                total_users=Decimal(str(row.get("total_users") or 0)),
                engagement_rate=Decimal(str(row.get("engagement_rate") or 0)),
                key_events=Decimal(str(row.get("key_events") or 0)),
                avg_session_duration_seconds=(
                    Decimal(str(row.get("avg_session_duration_seconds")))
                    if row.get("avg_session_duration_seconds") is not None
                    else None
                ),
            )
            for row in filtered
        ]

    def _latest_events(self, limit: int) -> list[MarketingTrackingEvent]:
        rows = self.repository.list_latest_events(limit=max(limit * 3, 30))
        if not rows:
            return []
        latest_date = str(rows[0].get("snapshot_date"))
        filtered = [row for row in rows if str(row.get("snapshot_date")) == latest_date][:limit]
        return [
            MarketingTrackingEvent(
                snapshot_date=datetime.strptime(str(row.get("snapshot_date")), "%Y-%m-%d").date(),
                event_name=str(row.get("event_name") or "unknown_event"),
                event_count=Decimal(str(row.get("event_count") or 0)),
                total_users=Decimal(str(row.get("total_users") or 0)),
                event_value_amount=(
                    Decimal(str(row.get("event_value_amount")))
                    if row.get("event_value_amount") is not None
                    else None
                ),
            )
            for row in filtered
        ]

    @staticmethod
    def _is_itinerary_page(page_path: str) -> bool:
        lowered = page_path.lower()
        return "itinerary" in lowered or "/trip/" in lowered

    @staticmethod
    def _is_lookbook_page(page_path: str) -> bool:
        lowered = page_path.lower()
        return "lookbook" in lowered or "/about/lookbooks" in lowered

    @staticmethod
    def _is_destination_page(page_path: str) -> bool:
        lowered = page_path.lower()
        return "destination" in lowered or "destinations" in lowered

    @staticmethod
    def _quality_score(
        sessions: Decimal, engagement_rate: Decimal, key_event_rate: Decimal
    ) -> Decimal:
        traffic_component = min(sessions / Decimal("500"), Decimal("1"))
        return (
            (key_event_rate * Decimal("0.55"))
            + (engagement_rate * Decimal("0.3"))
            + (traffic_component * Decimal("0.15"))
        )

    @staticmethod
    def _is_marketing_focus_page(page_path: str) -> bool:
        lowered = page_path.lower()
        excluded_patterns = (
            "/contact/thank-you",
            "/popups/",
            "forget-password",
            "/default",
            "/login",
            "/signin",
            "/register",
        )
        return not any(pattern in lowered for pattern in excluded_patterns)

    def _latest_page_activity_rows(
        self,
        *,
        limit: int = 100,
        page_path_contains: str | None = None,
    ) -> tuple[date | None, list[MarketingPageActivityRow]]:
        rows = self.repository.list_latest_page_activity(
            limit=max(limit * 4, 200), page_path_contains=page_path_contains
        )
        if not rows:
            return None, []
        latest_date = datetime.strptime(str(rows[0].get("snapshot_date")), "%Y-%m-%d").date()
        filtered = [
            row
            for row in rows
            if datetime.strptime(str(row.get("snapshot_date")), "%Y-%m-%d").date() == latest_date
        ][:limit]
        mapped: list[MarketingPageActivityRow] = []
        for row in filtered:
            sessions = Decimal(str(row.get("sessions") or 0))
            engagement_rate = Decimal(str(row.get("engagement_rate") or 0))
            key_event_rate = Decimal(str(row.get("key_event_rate") or 0))
            page_path = str(row.get("page_path") or "/")
            mapped.append(
                MarketingPageActivityRow(
                    snapshot_date=latest_date,
                    page_path=page_path,
                    page_title=str(row.get("page_title")) if row.get("page_title") else None,
                    screen_page_views=Decimal(str(row.get("screen_page_views") or 0)),
                    sessions=sessions,
                    total_users=Decimal(str(row.get("total_users") or 0)),
                    engaged_sessions=Decimal(str(row.get("engaged_sessions") or 0)),
                    key_events=Decimal(str(row.get("key_events") or 0)),
                    engagement_rate=engagement_rate,
                    key_event_rate=key_event_rate,
                    avg_session_duration_seconds=(
                        Decimal(str(row.get("avg_session_duration_seconds")))
                        if row.get("avg_session_duration_seconds") is not None
                        else None
                    ),
                    quality_score=self._quality_score(
                        sessions=sessions,
                        engagement_rate=engagement_rate,
                        key_event_rate=key_event_rate,
                    ),
                    is_itinerary_page=self._is_itinerary_page(page_path),
                )
            )
        return latest_date, mapped

    @staticmethod
    def _map_page_activity_rows(
        rows: list[dict[str, object]],
        *,
        snapshot_date: date,
        page_path_contains: str | None = None,
        limit: int = 100,
    ) -> list[MarketingPageActivityRow]:
        lowered_filter = (page_path_contains or "").strip().lower()
        mapped: list[MarketingPageActivityRow] = []
        for row in rows:
            page_path = str(row.get("page_path") or "/")
            if lowered_filter and lowered_filter not in page_path.lower():
                continue
            sessions = Decimal(str(row.get("sessions") or 0))
            engagement_rate = Decimal(str(row.get("engagement_rate") or 0))
            key_event_rate = Decimal(str(row.get("key_event_rate") or 0))
            mapped.append(
                MarketingPageActivityRow(
                    snapshot_date=snapshot_date,
                    page_path=page_path,
                    page_title=str(row.get("page_title")) if row.get("page_title") else None,
                    screen_page_views=Decimal(str(row.get("screen_page_views") or 0)),
                    sessions=sessions,
                    total_users=Decimal(str(row.get("total_users") or 0)),
                    engaged_sessions=Decimal(str(row.get("engaged_sessions") or 0)),
                    key_events=Decimal(str(row.get("key_events") or 0)),
                    engagement_rate=engagement_rate,
                    key_event_rate=key_event_rate,
                    avg_session_duration_seconds=(
                        Decimal(str(row.get("avg_session_duration_seconds")))
                        if row.get("avg_session_duration_seconds") is not None
                        else None
                    ),
                    quality_score=MarketingWebAnalyticsService._quality_score(
                        sessions=sessions,
                        engagement_rate=engagement_rate,
                        key_event_rate=key_event_rate,
                    ),
                    is_itinerary_page=MarketingWebAnalyticsService._is_itinerary_page(page_path),
                )
            )
        sorted_rows = sorted(mapped, key=lambda item: item.screen_page_views, reverse=True)
        return sorted_rows[:limit]

    @staticmethod
    def _map_geo_rows(
        rows: list[dict[str, object]],
        *,
        snapshot_date: date,
    ) -> list[MarketingGeoRow]:
        return [
            MarketingGeoRow(
                snapshot_date=snapshot_date,
                country=str(row.get("country") or "Unknown"),
                region=str(row.get("region")) if row.get("region") else None,
                city=str(row.get("city")) if row.get("city") else None,
                sessions=Decimal(str(row.get("sessions") or 0)),
                total_users=Decimal(str(row.get("total_users") or 0)),
                engaged_sessions=Decimal(str(row.get("engaged_sessions") or 0)),
                key_events=Decimal(str(row.get("key_events") or 0)),
                engagement_rate=Decimal(str(row.get("engagement_rate") or 0)),
                key_event_rate=Decimal(str(row.get("key_event_rate") or 0)),
            )
            for row in rows
        ]

    def _latest_geo_rows(self, *, limit: int = 200) -> tuple[date | None, list[MarketingGeoRow]]:
        rows = self.repository.list_latest_geo(limit=max(limit * 2, 300))
        if not rows:
            return None, []
        latest_date = datetime.strptime(str(rows[0].get("snapshot_date")), "%Y-%m-%d").date()
        filtered = [
            row
            for row in rows
            if datetime.strptime(str(row.get("snapshot_date")), "%Y-%m-%d").date() == latest_date
        ][:limit]
        mapped = [
            MarketingGeoRow(
                snapshot_date=latest_date,
                country=str(row.get("country") or "Unknown"),
                region=str(row.get("region")) if row.get("region") else None,
                city=str(row.get("city")) if row.get("city") else None,
                sessions=Decimal(str(row.get("sessions") or 0)),
                total_users=Decimal(str(row.get("total_users") or 0)),
                engaged_sessions=Decimal(str(row.get("engaged_sessions") or 0)),
                key_events=Decimal(str(row.get("key_events") or 0)),
                engagement_rate=Decimal(str(row.get("engagement_rate") or 0)),
                key_event_rate=Decimal(str(row.get("key_event_rate") or 0)),
            )
            for row in filtered
        ]
        return latest_date, mapped

    def _latest_demographics(self, *, limit: int = 50) -> list[MarketingDemographicRow]:
        rows = self.repository.list_latest_demographics(limit=max(limit * 2, 80))
        if not rows:
            return []
        latest_date = str(rows[0].get("snapshot_date"))
        filtered = [row for row in rows if str(row.get("snapshot_date")) == latest_date][:limit]
        snapshot_date = datetime.strptime(latest_date, "%Y-%m-%d").date()
        return [
            MarketingDemographicRow(
                snapshot_date=snapshot_date,
                age_bracket=str(row.get("age_bracket") or "unknown"),
                gender=str(row.get("gender") or "unknown"),
                sessions=Decimal(str(row.get("sessions") or 0)),
                total_users=Decimal(str(row.get("total_users") or 0)),
                engaged_sessions=Decimal(str(row.get("engaged_sessions") or 0)),
                key_events=Decimal(str(row.get("key_events") or 0)),
                engagement_rate=Decimal(str(row.get("engagement_rate") or 0)),
            )
            for row in filtered
        ]

    def _latest_devices(self, *, limit: int = 10) -> list[MarketingDeviceRow]:
        rows = self.repository.list_latest_devices(limit=max(limit * 2, 20))
        if not rows:
            return []
        latest_date = str(rows[0].get("snapshot_date"))
        filtered = [row for row in rows if str(row.get("snapshot_date")) == latest_date][:limit]
        snapshot_date = datetime.strptime(latest_date, "%Y-%m-%d").date()
        return [
            MarketingDeviceRow(
                snapshot_date=snapshot_date,
                device_category=str(row.get("device_category") or "unknown"),
                sessions=Decimal(str(row.get("sessions") or 0)),
                total_users=Decimal(str(row.get("total_users") or 0)),
                engaged_sessions=Decimal(str(row.get("engaged_sessions") or 0)),
                key_events=Decimal(str(row.get("key_events") or 0)),
                engagement_rate=Decimal(str(row.get("engagement_rate") or 0)),
            )
            for row in filtered
        ]

    def _latest_internal_search_terms(
        self,
        *,
        limit: int = 20,
    ) -> list[MarketingInternalSiteSearchTerm]:
        rows = self.repository.list_latest_internal_search_terms(limit=max(limit * 2, 40))
        if not rows:
            return []
        latest_date = str(rows[0].get("snapshot_date"))
        filtered = [row for row in rows if str(row.get("snapshot_date")) == latest_date][:limit]
        return [
            MarketingInternalSiteSearchTerm(
                search_term=str(row.get("search_term") or ""),
                event_count=Decimal(str(row.get("event_count") or 0)),
                total_users=Decimal(str(row.get("total_users") or 0)),
            )
            for row in filtered
            if str(row.get("search_term") or "").strip()
        ]

    def get_overview(self, *, country: str | None = None) -> MarketingOverview:
        self._assert_configuration()
        scoped_country = self._normalize_country_scope(country)

        if scoped_country:
            trend = self._fetch_daily_trend_live(days_back=800, country=scoped_country)
            latest_date = trend[-1].snapshot_date if trend else date.today()
            current = self._fetch_period_summary_live(
                start_date=latest_date - timedelta(days=29),
                end_date=latest_date,
                country=scoped_country,
            )
            previous = self._fetch_period_summary_live(
                start_date=latest_date - timedelta(days=59),
                end_date=latest_date - timedelta(days=30),
                country=scoped_country,
            )
            year_ago = self._fetch_period_summary_live(
                start_date=latest_date - timedelta(days=394),
                end_date=latest_date - timedelta(days=365),
                country=scoped_country,
            )
            today_summary = self._fetch_period_summary_live(
                start_date=latest_date,
                end_date=latest_date,
                country=scoped_country,
            )
            yesterday_summary = self._fetch_period_summary_live(
                start_date=latest_date - timedelta(days=1),
                end_date=latest_date - timedelta(days=1),
                country=scoped_country,
            )
            landing_pages = self._fetch_top_landing_pages(
                days_back=30,
                limit=10,
                country=scoped_country,
            )
            channels = self._fetch_channel_window_totals(
                days_back=30, limit=6, country=scoped_country
            )
            events = self._fetch_top_events(limit=10, days_back=30, country=scoped_country)
        else:
            trend = self._load_daily_trend()
            latest_date = trend[-1].snapshot_date if trend else date.today()
            period_summaries = self._latest_overview_period_summaries(as_of_date=latest_date)
            current = period_summaries.get("current_30d", self._empty_summary())
            previous = period_summaries.get("previous_30d", self._empty_summary())
            year_ago = period_summaries.get("year_ago_30d", self._empty_summary())
            today_summary = period_summaries.get("today", self._empty_summary())
            yesterday_summary = period_summaries.get("yesterday", self._empty_summary())
            landing_pages = self._latest_pages(limit=10)
            channels = self._fetch_channel_window_totals(days_back=30, limit=6)
            events = self._latest_events(limit=10)

        return MarketingOverview(
            kpis=self._build_kpis(current, previous, year_ago, today_summary, yesterday_summary),
            trend=trend[-800:],
            top_landing_pages=landing_pages,
            channels=channels,
            events=events,
            search_console_connected=bool((self.settings.google_gsc_site_url or "").strip()),
            currency=self.settings.marketing_default_currency,
            timezone=self.settings.marketing_default_timezone,
        )

    def get_search_performance(
        self,
        *,
        days_back: int = 30,
        country: str | None = None,
    ) -> MarketingSearchPerformance:
        scoped_country = self._normalize_country_scope(country)
        if days_back == 30 and not scoped_country:
            _ = self._load_daily_trend()
            channels = self._fetch_channel_window_totals(days_back=30, limit=8)
            pages = self._latest_pages(limit=15)
            internal_terms = self._latest_internal_search_terms(limit=20)
            source_mix = self._fetch_source_medium_performance(days_back=30, limit=25)
        else:
            self._assert_configuration()
            channels = self._fetch_channel_window_totals(
                days_back=days_back,
                limit=8,
                country=scoped_country,
            )
            pages = self._fetch_top_landing_pages(
                days_back=days_back,
                limit=15,
                country=scoped_country,
            )
            internal_terms = self._fetch_internal_site_search_terms(
                days_back=days_back,
                limit=20,
                country=scoped_country,
            )
            source_mix = self._fetch_source_medium_performance(
                days_back=days_back,
                limit=25,
                country=scoped_country,
            )
        referral_sources = sorted(
            [row for row in source_mix if row.medium.lower() == "referral"],
            key=lambda row: row.sessions,
            reverse=True,
        )[:10]
        valuable_source_candidates = [row for row in source_mix if row.sessions >= 20]
        if not valuable_source_candidates:
            valuable_source_candidates = source_mix
        top_valuable_sources = sorted(
            valuable_source_candidates,
            key=lambda row: (row.value_score, row.key_events, row.sessions),
            reverse=True,
        )[:10]
        return MarketingSearchPerformance(
            top_landing_pages=pages,
            channels=channels,
            source_mix=source_mix,
            referral_sources=referral_sources,
            top_valuable_sources=top_valuable_sources,
            internal_site_search_terms=internal_terms,
        )

    def get_search_console_insights(
        self,
        *,
        days_back: int = 30,
        country: str | None = None,
    ) -> MarketingSearchConsoleInsights:
        _ = country
        scoped_country = MARKETING_US_SCOPE
        scope_label = self._country_scope_label(scoped_country)
        is_gsc_connected = bool((self.settings.google_gsc_site_url or "").strip())
        if not is_gsc_connected:
            return MarketingSearchConsoleInsights(
                search_console_connected=False,
                connection_message=(
                    "Search Console is not connected yet. Connect it to unlock query impressions, "
                    "CTR, and rankings."
                ),
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
                top_pages=[],
                country_breakdown=[],
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
                        detail="Configure GOOGLE_GSC_SITE_URL and service-account permissions.",
                    )
                ],
                organic_landing_pages=[],
                internal_site_search_terms=[],
            )

        self._assert_search_console_configuration()
        today = date.today()
        try:
            latest_snapshot_date = self.repository.latest_search_console_snapshot_date(
                country_scope=scope_label
            )
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code != 404:
                raise
            return MarketingSearchConsoleInsights(
                search_console_connected=True,
                connection_message=(
                    "Search Console is connected, but canonical Search Console tables are "
                    "not present yet."
                ),
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
                top_pages=[],
                country_breakdown=[],
                device_breakdown=[],
                opportunities=[],
                challenges=[],
                market_benchmarks=[],
                query_intent_buckets=[],
                position_band_summary=[],
                issues=[
                    MarketingSearchConsoleIssue(
                        issue_key="search_console_tables_missing",
                        label="Search Console migrations not applied",
                        status="critical",
                        detail=(
                            "Run migration 0087_create_search_console_analytics_tables.sql "
                            "to enable Search Console snapshot reads."
                        ),
                    )
                ],
                organic_landing_pages=[],
                internal_site_search_terms=[],
            )

        rollup = self.repository.get_search_console_us_workspace_rollup(days_back=days_back)
        rollup_as_of = rollup.get("as_of_date")
        as_of_date = date.fromisoformat(str(rollup_as_of)) if rollup_as_of else latest_snapshot_date
        freshness_days_raw = rollup.get("freshness_days")
        freshness_days = (
            int(freshness_days_raw)
            if freshness_days_raw is not None
            else ((today - as_of_date).days if as_of_date else None)
        )

        def _decimal(value: Any) -> Decimal:
            if value is None or value == "":
                return Decimal("0")
            return Decimal(str(value))

        def _classify_opportunity_type(row: dict[str, Any]) -> str:
            opportunity_id = str(row.get("opportunity_id") or "").strip()
            average_position = _decimal(row.get("average_position"))
            if opportunity_id.startswith("low-ctr-query-"):
                return "low_ctr"
            if opportunity_id.startswith("near-breakout-query-"):
                return "near_breakout"
            if average_position > Decimal("12"):
                return "destination_gap"
            return "page_refresh"

        def _classify_challenge_type(row: dict[str, Any]) -> str:
            challenge_id = str(row.get("challenge_id") or "").strip()
            average_position = _decimal(row.get("average_position"))
            ctr = _decimal(row.get("ctr"))
            if challenge_id.startswith("page-query-ctr-gap-"):
                return "page_ctr_gap"
            if average_position > Decimal("15"):
                return "ranking_drop"
            if ctr < Decimal("0.01"):
                return "coverage_gap"
            return "intent_mismatch"

        overview_payload = (
            rollup.get("overview") if isinstance(rollup.get("overview"), dict) else {}
        )
        top_queries_payload = (
            rollup.get("top_queries") if isinstance(rollup.get("top_queries"), list) else []
        )
        top_pages_payload = (
            rollup.get("top_pages") if isinstance(rollup.get("top_pages"), list) else []
        )
        country_breakdown_payload = (
            rollup.get("country_breakdown")
            if isinstance(rollup.get("country_breakdown"), list)
            else []
        )
        device_breakdown_payload = (
            rollup.get("device_breakdown")
            if isinstance(rollup.get("device_breakdown"), list)
            else []
        )
        opportunities_payload = (
            rollup.get("opportunities") if isinstance(rollup.get("opportunities"), list) else []
        )
        challenges_payload = (
            rollup.get("challenges") if isinstance(rollup.get("challenges"), list) else []
        )
        benchmarks_payload = (
            rollup.get("market_benchmarks")
            if isinstance(rollup.get("market_benchmarks"), list)
            else []
        )
        intent_buckets_payload = (
            rollup.get("query_intent_buckets")
            if isinstance(rollup.get("query_intent_buckets"), list)
            else []
        )
        position_bands_payload = (
            rollup.get("position_band_summary")
            if isinstance(rollup.get("position_band_summary"), list)
            else []
        )

        top_queries = [
            MarketingSearchQuery(
                query=str(row.get("query") or "").strip(),
                clicks=_decimal(row.get("clicks")),
                impressions=_decimal(row.get("impressions")),
                ctr=_decimal(row.get("ctr")),
                average_position=_decimal(row.get("average_position")),
                is_branded=bool(row.get("is_branded")),
                intent_bucket=str(row.get("intent_bucket") or "").strip() or None,
                term_type=str(row.get("term_type") or "").strip() or None,
                position_band=str(row.get("position_band") or "").strip() or None,
            )
            for row in top_queries_payload
            if isinstance(row, dict) and str(row.get("query") or "").strip()
        ]
        top_pages = [
            MarketingSearchConsolePagePerformance(
                page_path=str(row.get("page_path") or "").strip(),
                clicks=_decimal(row.get("clicks")),
                impressions=_decimal(row.get("impressions")),
                ctr=_decimal(row.get("ctr")),
                average_position=_decimal(row.get("average_position")),
            )
            for row in top_pages_payload
            if isinstance(row, dict) and str(row.get("page_path") or "").strip()
        ]
        country_breakdown = [
            MarketingSearchConsoleBreakdownRow(
                label=str(row.get("label") or "").strip(),
                clicks=_decimal(row.get("clicks")),
                impressions=_decimal(row.get("impressions")),
                ctr=_decimal(row.get("ctr")),
                average_position=_decimal(row.get("average_position")),
            )
            for row in country_breakdown_payload
            if isinstance(row, dict) and str(row.get("label") or "").strip()
        ]
        device_breakdown = [
            MarketingSearchConsoleBreakdownRow(
                label=str(row.get("label") or "").strip(),
                clicks=_decimal(row.get("clicks")),
                impressions=_decimal(row.get("impressions")),
                ctr=_decimal(row.get("ctr")),
                average_position=_decimal(row.get("average_position")),
            )
            for row in device_breakdown_payload
            if isinstance(row, dict) and str(row.get("label") or "").strip()
        ]
        opportunities = [
            MarketingSearchConsoleOpportunity(
                opportunity_id=str(row.get("opportunity_id") or "").strip(),
                title=str(row.get("title") or "").strip(),
                summary=str(row.get("summary") or "").strip(),
                page_path=str(row.get("page_path") or "").strip() or None,
                query=str(row.get("query") or "").strip() or None,
                clicks=_decimal(row.get("clicks")),
                impressions=_decimal(row.get("impressions")),
                ctr=_decimal(row.get("ctr")),
                average_position=_decimal(row.get("average_position")),
                priority_score=_decimal(row.get("priority_score")),
                recommended_action=str(row.get("recommended_action") or "").strip(),
                opportunity_type=(
                    str(row.get("opportunity_type") or "").strip()
                    or _classify_opportunity_type(row)
                ),
            )
            for row in opportunities_payload
            if isinstance(row, dict) and str(row.get("opportunity_id") or "").strip()
        ]
        challenges = [
            MarketingSearchConsoleChallenge(
                challenge_id=str(row.get("challenge_id") or "").strip(),
                title=str(row.get("title") or "").strip(),
                summary=str(row.get("summary") or "").strip(),
                page_path=str(row.get("page_path") or "").strip() or None,
                query=str(row.get("query") or "").strip() or None,
                clicks=_decimal(row.get("clicks")),
                impressions=_decimal(row.get("impressions")),
                ctr=_decimal(row.get("ctr")),
                average_position=_decimal(row.get("average_position")),
                severity_score=_decimal(row.get("severity_score")),
                recommended_action=str(row.get("recommended_action") or "").strip(),
                challenge_type=(
                    str(row.get("challenge_type") or "").strip() or _classify_challenge_type(row)
                ),
            )
            for row in challenges_payload
            if isinstance(row, dict) and str(row.get("challenge_id") or "").strip()
        ]
        market_benchmarks = [
            MarketingSearchConsoleMarketBenchmark(
                market_label=str(row.get("market_label") or "").strip(),
                clicks=_decimal(row.get("clicks")),
                impressions=_decimal(row.get("impressions")),
                ctr=_decimal(row.get("ctr")),
                average_position=_decimal(row.get("average_position")),
            )
            for row in benchmarks_payload
            if isinstance(row, dict) and str(row.get("market_label") or "").strip()
        ]
        query_intent_buckets = [
            MarketingSearchConsoleIntentBucket(
                bucket_label=str(row.get("bucket_label") or "").strip(),
                query_count=int(row.get("query_count") or 0),
                clicks=_decimal(row.get("clicks")),
                impressions=_decimal(row.get("impressions")),
                average_ctr=_decimal(row.get("average_ctr")),
            )
            for row in intent_buckets_payload
            if isinstance(row, dict) and str(row.get("bucket_label") or "").strip()
        ]
        position_band_summary = [
            MarketingSearchConsolePositionBand(
                band_label=str(row.get("band_label") or "").strip(),
                query_count=int(row.get("query_count") or 0),
                clicks=_decimal(row.get("clicks")),
                impressions=_decimal(row.get("impressions")),
                average_ctr=_decimal(row.get("average_ctr")),
            )
            for row in position_bands_payload
            if isinstance(row, dict) and str(row.get("band_label") or "").strip()
        ]
        query_row_count = int(rollup.get("query_row_count") or 0)

        issues: list[MarketingSearchConsoleIssue] = []
        if query_row_count <= 0:
            issues.append(
                MarketingSearchConsoleIssue(
                    issue_key="empty_query_dataset",
                    label="No query data returned",
                    status="warning",
                    detail="Search Console returned no query rows for the selected window.",
                )
            )
        if freshness_days is not None and freshness_days > 3:
            issues.append(
                MarketingSearchConsoleIssue(
                    issue_key="stale_search_console_snapshot",
                    label="Search Console snapshot is stale",
                    status="warning",
                    detail=f"Latest stored Search Console snapshot is {freshness_days} days old.",
                )
            )
        if not issues:
            issues.append(
                MarketingSearchConsoleIssue(
                    issue_key="search_console_healthy",
                    label="Search Console data is healthy",
                    status="healthy",
                    detail=(
                        "Search Console snapshots are current and query/page datasets "
                        "are populated."
                    ),
                )
            )

        internal_terms = self._fetch_internal_site_search_terms(
            days_back=days_back,
            limit=20,
            country=scoped_country,
        )
        organic_landing_pages = [
            MarketingLandingPagePerformance(
                snapshot_date=as_of_date or today,
                landing_page=page.page_path,
                sessions=page.clicks,
                total_users=page.clicks,
                engagement_rate=page.ctr,
                key_events=Decimal("0"),
                avg_session_duration_seconds=None,
            )
            for page in top_pages[:20]
        ]

        return MarketingSearchConsoleInsights(
            search_console_connected=True,
            connection_message=(
                "Search Console is connected and Supabase snapshots are serving US-first search "
                "insights with benchmark market comparisons."
            ),
            data_mode="snapshot",
            as_of_date=as_of_date,
            overview=MarketingSearchConsoleOverview(
                total_clicks=_decimal(overview_payload.get("total_clicks")),
                total_impressions=_decimal(overview_payload.get("total_impressions")),
                average_ctr=_decimal(overview_payload.get("average_ctr")),
                average_position=_decimal(overview_payload.get("average_position")),
                clicks_delta_pct=(
                    _decimal(overview_payload.get("clicks_delta_pct"))
                    if overview_payload.get("clicks_delta_pct") is not None
                    else None
                ),
                impressions_delta_pct=(
                    _decimal(overview_payload.get("impressions_delta_pct"))
                    if overview_payload.get("impressions_delta_pct") is not None
                    else None
                ),
                ctr_delta_pct=(
                    _decimal(overview_payload.get("ctr_delta_pct"))
                    if overview_payload.get("ctr_delta_pct") is not None
                    else None
                ),
                position_delta=(
                    _decimal(overview_payload.get("position_delta"))
                    if overview_payload.get("position_delta") is not None
                    else None
                ),
                freshness_days=freshness_days,
            ),
            top_queries=top_queries,
            top_pages=top_pages,
            country_breakdown=country_breakdown,
            device_breakdown=device_breakdown,
            opportunities=opportunities,
            challenges=challenges,
            market_benchmarks=market_benchmarks,
            query_intent_buckets=query_intent_buckets,
            position_band_summary=position_band_summary,
            issues=issues,
            organic_landing_pages=organic_landing_pages,
            internal_site_search_terms=internal_terms,
        )

    def get_search_console_page_profile(
        self,
        *,
        page_path: str,
        days_back: int = 30,
    ) -> MarketingSearchConsolePageProfile:
        normalized_page_path = (page_path or "").strip()
        if not normalized_page_path:
            raise BadRequestError("page_path is required")
        is_absolute_url = normalized_page_path.startswith(
            ("http://", "https://")
        )
        if not normalized_page_path.startswith("/") and not is_absolute_url:
            normalized_page_path = f"/{normalized_page_path}"

        def _decimal(value: Any) -> Decimal:
            if value is None or value == "":
                return Decimal("0")
            return Decimal(str(value))

        rollup = self.repository.get_search_console_page_profile_rollup(
            days_back=days_back,
            page_path=normalized_page_path,
        )
        if normalized_page_path.startswith(("http://", "https://")):
            trend_rows = (
                rollup.get("daily_trend")
                if isinstance(rollup.get("daily_trend"), list)
                else []
            )
            query_rows = (
                rollup.get("top_queries")
                if isinstance(rollup.get("top_queries"), list)
                else []
            )
            if not trend_rows and not query_rows:
                alternate_page_path = (
                    normalized_page_path[:-1]
                    if normalized_page_path.endswith("/")
                    else f"{normalized_page_path}/"
                )
                rollup = self.repository.get_search_console_page_profile_rollup(
                    days_back=days_back,
                    page_path=alternate_page_path,
                )
                normalized_page_path = alternate_page_path
        overview_payload = (
            rollup.get("overview") if isinstance(rollup.get("overview"), dict) else {}
        )
        top_queries_payload = (
            rollup.get("top_queries") if isinstance(rollup.get("top_queries"), list) else []
        )
        trend_payload = (
            rollup.get("daily_trend") if isinstance(rollup.get("daily_trend"), list) else []
        )
        benchmarks_payload = (
            rollup.get("market_benchmarks")
            if isinstance(rollup.get("market_benchmarks"), list)
            else []
        )
        as_of_raw = rollup.get("as_of_date")
        as_of_date = date.fromisoformat(str(as_of_raw)) if as_of_raw else None

        top_queries = [
            MarketingSearchQuery(
                query=str(row.get("query") or "").strip(),
                clicks=_decimal(row.get("clicks")),
                impressions=_decimal(row.get("impressions")),
                ctr=_decimal(row.get("ctr")),
                average_position=_decimal(row.get("average_position")),
                is_branded=bool(row.get("is_branded")),
            )
            for row in top_queries_payload
            if isinstance(row, dict) and str(row.get("query") or "").strip()
        ]
        daily_trend = [
            MarketingSearchConsolePageTrendPoint(
                snapshot_date=date.fromisoformat(str(row.get("snapshot_date"))),
                clicks=_decimal(row.get("clicks")),
                impressions=_decimal(row.get("impressions")),
                ctr=_decimal(row.get("ctr")),
                average_position=_decimal(row.get("average_position")),
            )
            for row in trend_payload
            if isinstance(row, dict) and row.get("snapshot_date")
        ]
        market_benchmarks = [
            MarketingSearchConsoleMarketBenchmark(
                market_label=str(row.get("market_label") or "").strip(),
                clicks=_decimal(row.get("clicks")),
                impressions=_decimal(row.get("impressions")),
                ctr=_decimal(row.get("ctr")),
                average_position=_decimal(row.get("average_position")),
            )
            for row in benchmarks_payload
            if isinstance(row, dict) and str(row.get("market_label") or "").strip()
        ]

        issues: list[MarketingSearchConsoleIssue] = []
        if not daily_trend:
            issues.append(
                MarketingSearchConsoleIssue(
                    issue_key="empty_page_profile",
                    label="No page-level trend data",
                    status="warning",
                    detail="No Search Console page data is available for this URL and time window.",
                )
            )
        if not top_queries:
            issues.append(
                MarketingSearchConsoleIssue(
                    issue_key="empty_page_profile_queries",
                    label="No query matches for page",
                    status="warning",
                    detail=(
                        "No associated search queries were found for this URL in the selected "
                        "window."
                    ),
                )
            )
        if not issues:
            issues.append(
                MarketingSearchConsoleIssue(
                    issue_key="page_profile_healthy",
                    label="Page profile data is healthy",
                    status="healthy",
                    detail="Page trend and query coverage are available for this URL.",
                )
            )

        recommended_actions = [
            (
                "Tighten title and meta description to improve click-through on "
                "high-impression queries."
            ),
            "Align page headings with strongest query intent themes from this profile.",
            "Add internal links from related destination pages to strengthen topical authority.",
        ]

        return MarketingSearchConsolePageProfile(
            page_path=normalized_page_path,
            as_of_date=as_of_date,
            overview=MarketingSearchConsoleOverview(
                total_clicks=_decimal(overview_payload.get("total_clicks")),
                total_impressions=_decimal(overview_payload.get("total_impressions")),
                average_ctr=_decimal(overview_payload.get("average_ctr")),
                average_position=_decimal(overview_payload.get("average_position")),
                freshness_days=(date.today() - as_of_date).days if as_of_date else None,
            ),
            daily_trend=daily_trend,
            top_queries=top_queries,
            market_benchmarks=market_benchmarks,
            issues=issues,
            recommended_actions=recommended_actions,
        )

    def get_page_activity(
        self,
        *,
        page_path_contains: str | None = None,
        limit: int = 100,
        days_back: int = 30,
        country: str | None = None,
    ) -> MarketingPageActivity:
        scoped_country = self._normalize_country_scope(country)
        if days_back == 30 and not scoped_country:
            _ = self._load_daily_trend()
            snapshot_date, all_pages = self._latest_page_activity_rows(
                limit=limit,
                page_path_contains=page_path_contains,
            )
        else:
            self._assert_configuration()
            snapshot_date = date.today()
            raw_rows = self._fetch_page_activity_breakdown(
                days_back=days_back, country=scoped_country
            )
            all_pages = self._map_page_activity_rows(
                raw_rows,
                snapshot_date=snapshot_date,
                page_path_contains=page_path_contains,
                limit=limit,
            )
        pages_with_volume = [page for page in all_pages if page.sessions >= 20]
        ranked_pages = [
            page for page in pages_with_volume if self._is_marketing_focus_page(page.page_path)
        ]
        worst_pages = sorted(ranked_pages, key=lambda page: page.quality_score)[:10]
        best_pages = sorted(ranked_pages, key=lambda page: page.quality_score, reverse=True)[:10]
        itinerary_pages = [page for page in all_pages if page.is_itinerary_page][:25]
        lookbook_pages = [page for page in all_pages if self._is_lookbook_page(page.page_path)][:25]
        destination_pages = [
            page for page in all_pages if self._is_destination_page(page.page_path)
        ][:25]
        if page_path_contains:
            lookbook_pages = []
            destination_pages = []
        return MarketingPageActivity(
            snapshot_date=snapshot_date,
            metric_guide=(
                "Quality score blends key event rate, engagement rate, "
                "and traffic scale to rank pages."
            ),
            best_pages=best_pages,
            worst_pages=worst_pages,
            itinerary_pages=itinerary_pages,
            lookbook_pages=lookbook_pages,
            destination_pages=destination_pages,
            all_pages=all_pages[:50],
        )

    def get_geo_breakdown(
        self,
        *,
        days_back: int = 30,
        country: str | None = None,
    ) -> MarketingGeoBreakdown:
        scoped_country = self._normalize_country_scope(country)
        if days_back == 30 and not scoped_country:
            _ = self._load_daily_trend()
            snapshot_date, rows = self._latest_geo_rows(limit=300)
            top_countries = self._fetch_country_window_totals(days_back=30, limit=12)
            demographics = self._latest_demographics(limit=50)
            devices = self._latest_devices(limit=10)
        else:
            self._assert_configuration()
            snapshot_date = date.today()
            raw_rows = self._fetch_geo_breakdown(days_back=days_back, country=scoped_country)
            rows = self._map_geo_rows(raw_rows, snapshot_date=snapshot_date)
            top_countries = self._fetch_country_window_totals(
                days_back=days_back,
                limit=12,
                country=scoped_country,
            )
            demographics = self._fetch_demographics_breakdown(
                days_back=days_back, country=scoped_country
            )
            devices = self._fetch_device_breakdown(days_back=days_back, country=scoped_country)
        return MarketingGeoBreakdown(
            snapshot_date=snapshot_date,
            rows=rows,
            top_countries=top_countries,
            demographics=demographics,
            devices=devices,
        )

    def get_event_catalog(self, *, country: str | None = None) -> MarketingEventCatalog:
        scoped_country = self._normalize_country_scope(country)
        if scoped_country:
            self._assert_configuration()
            events = self._fetch_top_events(limit=50, days_back=30, country=scoped_country)
        else:
            _ = self._load_daily_trend()
            events = self._latest_events(limit=50)
        snapshot_date = events[0].snapshot_date if events else None
        catalog_items: list[MarketingEventCatalogItem] = []
        for event in events:
            event_name = event.event_name
            category, description, is_conversion_event = EVENT_DEFINITIONS.get(
                event_name,
                (
                    "other",
                    "Custom or uncategorized event; review implementation in GA/GTM.",
                    False,
                ),
            )
            catalog_items.append(
                MarketingEventCatalogItem(
                    event_name=event_name,
                    event_count=event.event_count,
                    total_users=event.total_users,
                    event_value_amount=event.event_value_amount,
                    category=category,
                    description=description,
                    is_conversion_event=is_conversion_event,
                )
            )
        return MarketingEventCatalog(snapshot_date=snapshot_date, events=catalog_items)

    def get_ai_insights(self, *, country: str | None = None) -> list[MarketingAiInsight]:
        scoped_country = self._normalize_country_scope(country)
        overview = self.get_overview(country=scoped_country)
        page_activity = self.get_page_activity(days_back=30, country=scoped_country)
        geo = self.get_geo_breakdown(days_back=30, country=scoped_country)
        search = self.get_search_performance(days_back=30, country=scoped_country)
        insights: list[MarketingAiInsight] = []

        low_engagement_pages = sorted(
            [page for page in overview.top_landing_pages if page.engagement_rate < Decimal("0.45")],
            key=lambda item: item.sessions,
            reverse=True,
        )
        if low_engagement_pages:
            page = low_engagement_pages[0]
            insights.append(
                MarketingAiInsight(
                    insight_id="improve-low-engagement-page",
                    priority="high",
                    category="content",
                    focus_area="fix",
                    title="Fix a high-traffic landing page before buying more traffic",
                    summary=(
                        f"{page.landing_page} is bringing in volume, but user intent is not being "
                        "converted into engagement."
                    ),
                    target_label=page.landing_page,
                    target_path=page.landing_page,
                    owner_hint="marketing",
                    primary_metric_label="Engagement Rate",
                    impact_score=Decimal("94"),
                    confidence_score=Decimal("89"),
                    evidence_points=[
                        f"Sessions: {int(page.sessions)}",
                        f"Engagement rate: {round(float(page.engagement_rate) * 100, 1)}%",
                        f"Key events: {int(page.key_events)}",
                    ],
                    recommended_actions=[
                        "Rewrite the headline, hero value proposition, and primary CTA "
                        "to match the acquisition intent landing here.",
                        "Reduce above-the-fold distraction and give one obvious next step.",
                        "Treat this page as a live experiment candidate for one-week copy "
                        "and CTA testing.",
                    ],
                )
            )

        weak_channel = next(
            (
                channel
                for channel in sorted(
                    overview.channels,
                    key=lambda item: item.sessions,
                    reverse=True,
                )
                if channel.sessions >= 100
                and channel.engagement_rate < Decimal("0.05")
                and channel.key_events == 0
            ),
            None,
        )
        if weak_channel:
            insights.append(
                MarketingAiInsight(
                    insight_id="repair-low-quality-acquisition-channel",
                    priority="high",
                    category="acquisition",
                    focus_area="cut",
                    title="Trim or rebuild a high-volume channel that is wasting attention",
                    summary=(
                        f"{weak_channel.channel_name} is driving traffic, but it is not producing "
                        "meaningful engagement or conversion signals."
                    ),
                    target_label=weak_channel.channel_name,
                    target_path=None,
                    owner_hint="marketing",
                    primary_metric_label="Channel Engagement Rate",
                    impact_score=Decimal("91"),
                    confidence_score=Decimal("92"),
                    evidence_points=[
                        f"Sessions: {int(weak_channel.sessions)}",
                        f"Engagement rate: {round(float(weak_channel.engagement_rate) * 100, 1)}%",
                        f"Key events: {int(weak_channel.key_events)}",
                    ],
                    recommended_actions=[
                        "Audit campaign targeting, placements, audience quality, and "
                        "landing-page match for this channel.",
                        "Pause the weakest traffic slices until creative and intent "
                        "alignment are repaired.",
                        "Move spend toward pages and campaigns already showing better "
                        "engagement quality.",
                    ],
                )
            )

        mobile_device = next(
            (device for device in geo.devices if device.device_category.lower() == "mobile"),
            None,
        )
        desktop_device = next(
            (device for device in geo.devices if device.device_category.lower() == "desktop"),
            None,
        )
        total_device_sessions = sum((device.sessions for device in geo.devices), Decimal("0"))
        if (
            mobile_device
            and desktop_device
            and total_device_sessions > 0
            and mobile_device.sessions / total_device_sessions >= Decimal("0.6")
            and desktop_device.engagement_rate - mobile_device.engagement_rate >= Decimal("0.05")
        ):
            insights.append(
                MarketingAiInsight(
                    insight_id="mobile-experience-priority",
                    priority="high",
                    category="device",
                    focus_area="optimize",
                    title="Mobile is the dominant experience, but it is underperforming desktop",
                    summary=(
                        "Most sessions are coming through mobile, so even moderate mobile friction "
                        "is suppressing total marketing return."
                    ),
                    target_label="Mobile experience",
                    target_path=None,
                    owner_hint="web",
                    primary_metric_label="Mobile Engagement Rate",
                    impact_score=Decimal("90"),
                    confidence_score=Decimal("86"),
                    evidence_points=[
                        f"Mobile sessions: {int(mobile_device.sessions)}",
                        "Mobile engagement: "
                        f"{round(float(mobile_device.engagement_rate) * 100, 1)}%",
                        "Desktop engagement: "
                        f"{round(float(desktop_device.engagement_rate) * 100, 1)}%",
                    ],
                    recommended_actions=[
                        "Audit mobile landing pages for speed, CTA prominence, and form friction.",
                        "Check whether high-volume mobile pages bury the primary next step.",
                        "Prioritize mobile-specific hero, copy-density, "
                        "and button hierarchy tests.",
                    ],
                )
            )

        localization_market = next(
            (
                row
                for row in sorted(
                    geo.top_countries,
                    key=lambda item: item.sessions * item.engagement_rate,
                    reverse=True,
                )
                if row.country not in {"United States", "(not set)"}
                and row.sessions >= 100
                and row.engagement_rate >= Decimal("0.15")
            ),
            None,
        )
        if localization_market:
            insights.append(
                MarketingAiInsight(
                    insight_id="localize-proven-market",
                    priority="medium",
                    category="geography",
                    focus_area="localize",
                    title="Localize messaging for a market already showing qualified interest",
                    summary=(
                        f"{localization_market.country} is showing enough engagement "
                        "quality to justify "
                        "market-specific creative or destination emphasis."
                    ),
                    target_label=localization_market.country,
                    target_path=None,
                    owner_hint="marketing",
                    primary_metric_label="Country Engagement Rate",
                    impact_score=Decimal("77"),
                    confidence_score=Decimal("75"),
                    evidence_points=[
                        f"Sessions: {int(localization_market.sessions)}",
                        "Engagement rate: "
                        f"{round(float(localization_market.engagement_rate) * 100, 1)}%",
                    ],
                    recommended_actions=[
                        "Create one localized campaign/message set tailored to this market.",
                        "Feature the destinations or offers that map to the "
                        "strongest demand signals.",
                        "Use this market as a test bed before broader international rollout.",
                    ],
                )
            )

        search_tokens = {
            token
            for page in search.top_landing_pages
            for token in str(page.landing_page).lower().replace("-", " ").split("/")
            if len(token) >= 4
        }
        site_search_gap = next(
            (
                term
                for term in search.internal_site_search_terms
                if term.total_users >= 5
                and not any(token in search_tokens for token in term.search_term.lower().split())
            ),
            None,
        )
        if site_search_gap:
            insights.append(
                MarketingAiInsight(
                    insight_id="build-content-for-internal-demand",
                    priority="medium",
                    category="intent",
                    focus_area="scale",
                    title="Build content around what visitors are explicitly searching for",
                    summary=(
                        f"Visitors are repeatedly searching for '{site_search_gap.search_term}', "
                        "which suggests demand is ahead of content or navigation support."
                    ),
                    target_label=site_search_gap.search_term,
                    target_path=None,
                    owner_hint="marketing",
                    primary_metric_label="Internal Search Demand",
                    impact_score=Decimal("74"),
                    confidence_score=Decimal("81"),
                    evidence_points=[
                        f"Search events: {int(site_search_gap.event_count)}",
                        f"Users: {int(site_search_gap.total_users)}",
                    ],
                    recommended_actions=[
                        "Create or strengthen a page, module, or CTA path "
                        "aligned to this search intent.",
                        "Expose this topic higher in navigation or landing-page internal links.",
                        "Track whether adding direct content reduces repeat internal search loops.",
                    ],
                )
            )

        destination_winner = next(
            (
                page
                for page in sorted(
                    page_activity.destination_pages,
                    key=lambda item: item.quality_score * item.sessions,
                    reverse=True,
                )
                if page.sessions >= 75 and page.quality_score >= Decimal("0.2")
            ),
            None,
        )
        if destination_winner:
            insights.append(
                MarketingAiInsight(
                    insight_id="scale-high-signal-destination",
                    priority="medium",
                    category="content",
                    focus_area="scale",
                    title="Scale a destination page that is already proving demand quality",
                    summary=(
                        f"{destination_winner.page_path} is showing "
                        "stronger-than-average signal quality "
                        "and is a good candidate for campaign amplification."
                    ),
                    target_label=destination_winner.page_path,
                    target_path=destination_winner.page_path,
                    owner_hint="sales",
                    primary_metric_label="Destination Quality Score",
                    impact_score=Decimal("79"),
                    confidence_score=Decimal("78"),
                    evidence_points=[
                        f"Sessions: {int(destination_winner.sessions)}",
                        "Quality score: "
                        f"{round(float(destination_winner.quality_score) * 100, 1)}%",
                        "Engagement rate: "
                        f"{round(float(destination_winner.engagement_rate) * 100, 1)}%",
                    ],
                    recommended_actions=[
                        "Feature this destination in paid and lifecycle campaigns.",
                        "Build itinerary, offer, or advisor CTA pathways "
                        "off this destination page.",
                        "Use this page's framing as a benchmark for lower-performing "
                        "destination pages.",
                    ],
                )
            )

        removable_page = next(
            (
                page
                for page in sorted(
                    page_activity.worst_pages,
                    key=lambda item: item.sessions,
                    reverse=True,
                )
                if page.sessions >= 50
                and page.engagement_rate <= Decimal("0.08")
                and page.key_event_rate == 0
            ),
            None,
        )
        if removable_page:
            insights.append(
                MarketingAiInsight(
                    insight_id="deprioritize-low-value-page",
                    priority="medium",
                    category="content",
                    focus_area="cut",
                    title=(
                        "Deprioritize or consolidate a page "
                        "that is absorbing traffic without payoff"
                    ),
                    summary=(
                        f"{removable_page.page_path} is taking attention "
                        "but not creating measurable "
                        "commercial value."
                    ),
                    target_label=removable_page.page_path,
                    target_path=removable_page.page_path,
                    owner_hint="web",
                    primary_metric_label="Page Quality Score",
                    impact_score=Decimal("72"),
                    confidence_score=Decimal("83"),
                    evidence_points=[
                        f"Sessions: {int(removable_page.sessions)}",
                        "Engagement rate: "
                        f"{round(float(removable_page.engagement_rate) * 100, 1)}%",
                        "Key event rate: "
                        f"{round(float(removable_page.key_event_rate) * 100, 1)}%",
                    ],
                    recommended_actions=[
                        "Decide whether this page should be upgraded, redirected, "
                        "or removed from promotion.",
                        "Stop sending paid or internal promotional traffic here "
                        "until its role is clarified.",
                        "If the page must remain, give it one clear CTA and simpler page intent.",
                    ],
                )
            )

        important_events = {event.event_name: event for event in overview.events}
        if "generate_lead" not in important_events and "sign_up" not in important_events:
            insights.append(
                MarketingAiInsight(
                    insight_id="track-primary-conversion-events",
                    priority="high",
                    category="measurement",
                    focus_area="instrument",
                    title=(
                        "Tighten conversion instrumentation before "
                        "trusting optimization decisions"
                    ),
                    summary=(
                        "Primary lead-conversion events are still not "
                        "clearly visible, which limits "
                        "the reliability of every downstream marketing decision."
                    ),
                    target_label="Primary conversion tracking",
                    target_path=None,
                    owner_hint="analytics",
                    primary_metric_label="Primary Conversion Events",
                    impact_score=Decimal("96"),
                    confidence_score=Decimal("95"),
                    evidence_points=[
                        "Key conversion events did not appear in the top event list.",
                        "Strategic funnel analysis is constrained without clean instrumentation.",
                    ],
                    recommended_actions=[
                        "Define one canonical primary conversion event for qualified leads.",
                        "Define one secondary intent event for strong-but-not-final "
                        "buying signals.",
                        "Align GA4 event naming with sales handoff stages and CRM expectations.",
                    ],
                )
            )

        if not insights:
            insights.append(
                MarketingAiInsight(
                    insight_id="baseline-stable",
                    priority="low",
                    category="content",
                    focus_area="optimize",
                    title="Baseline is stable; use this period for disciplined experiments",
                    summary=(
                        "No major structural risks are visible right now, so this is a good time "
                        "to run focused tests instead of broad changes."
                    ),
                    target_label="Website baseline",
                    target_path=None,
                    owner_hint="marketing",
                    primary_metric_label="Overall Stability",
                    impact_score=Decimal("48"),
                    confidence_score=Decimal("67"),
                    evidence_points=[
                        "No major structural signal risk detected in current GA4 snapshot."
                    ],
                    recommended_actions=[
                        "Test one value proposition update on a top landing page.",
                        "Add conversion-step diagnostics to validate funnel friction points.",
                    ],
                )
            )

        priority_rank = {"high": 0, "medium": 1, "low": 2}
        return sorted(
            insights,
            key=lambda item: (priority_rank[item.priority], -item.impact_score),
        )[:8]

    def get_health(self) -> MarketingHealth:
        configured = bool(
            (self.settings.google_service_account_key_json or "").strip()
            and (self.settings.google_ga4_property_id or "").strip()
        )
        gsc_connected = bool((self.settings.google_gsc_site_url or "").strip())
        latest_run = self.repository.latest_sync_run()
        last_synced_at = latest_run.get("completed_at") if latest_run else None
        run_status = latest_run.get("status") if latest_run else "not_started"

        status_rows = [
            MarketingHealthStatus(
                key="ga4Configuration",
                label="GA4 Configuration",
                status="connected" if configured else "missing",
                detail=(
                    "GA4 credentials and property ID are configured."
                    if configured
                    else "Set Google GA4 env vars to enable ingestion."
                ),
            ),
            MarketingHealthStatus(
                key="searchConsoleConnection",
                label="Search Console Connection",
                status="connected" if gsc_connected else "pending",
                detail=(
                    "Search Console property is configured."
                    if gsc_connected
                    else "Search Console is deferred; connect later for keyword query analytics."
                ),
            ),
            MarketingHealthStatus(
                key="latestSyncRun",
                label="Latest Sync Run",
                status=(
                    "healthy"
                    if run_status == "success"
                    else ("pending" if run_status == "running" else "warning")
                ),
                detail=f"Latest run status: {run_status}.",
            ),
        ]
        return MarketingHealth(
            statuses=status_rows,
            last_synced_at=last_synced_at,
            latest_run_status=str(run_status),
        )
