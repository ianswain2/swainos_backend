from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import Literal

from src.core.config import get_settings
from src.core.errors import BadRequestError
from src.integrations.google_analytics_client import GoogleAnalyticsClient
from src.repositories.marketing_web_analytics_repository import MarketingWebAnalyticsRepository
from src.schemas.marketing_web_analytics import (
    MarketingAiInsight,
    MarketingChannelPerformance,
    MarketingEventCatalog,
    MarketingEventCatalogItem,
    MarketingGeoBreakdown,
    MarketingGeoRow,
    MarketingHealth,
    MarketingHealthStatus,
    MarketingKpi,
    MarketingLandingPagePerformance,
    MarketingOverview,
    MarketingPageActivity,
    MarketingPageActivityRow,
    MarketingSearchPerformance,
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


class MarketingWebAnalyticsService:
    def __init__(
        self,
        repository: MarketingWebAnalyticsRepository,
        ga_client: GoogleAnalyticsClient,
    ) -> None:
        self.repository = repository
        self.ga_client = ga_client
        self.settings = get_settings()

    def _assert_configuration(self) -> None:
        if not (self.settings.google_service_account_key_json or "").strip():
            raise BadRequestError("Google Analytics integration is not configured")
        if not (self.settings.google_ga4_property_id or "").strip():
            raise BadRequestError("GOOGLE_GA4_PROPERTY_ID is required")

    def _fetch_daily_channel_breakdown(self, days_back: int = 400) -> list[dict[str, object]]:
        rows = self.ga_client.run_report(
            start_date=f"{days_back}daysAgo",
            end_date="today",
            metrics=["sessions", "totalUsers", "engagedSessions", "keyEvents", "engagementRate"],
            dimensions=["date", "sessionSourceMedium", "sessionDefaultChannelGroup"],
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
            snapshot_date = _parse_snapshot_date(raw_date)
            mapped.append(
                {
                    "snapshot_date": snapshot_date,
                    "source_medium": str(row.get("sessionSourceMedium") or "unknown / unknown"),
                    "default_channel_group": str(
                        row.get("sessionDefaultChannelGroup") or "Unassigned"
                    ),
                    "sessions": Decimal(row.get("sessions", 0)),
                    "total_users": Decimal(row.get("totalUsers", 0)),
                    "engaged_sessions": Decimal(row.get("engagedSessions", 0)),
                    "key_events": Decimal(row.get("keyEvents", 0)),
                    "engagement_rate": Decimal(row.get("engagementRate", 0)),
                }
            )
        return mapped

    def _fetch_top_landing_pages(self, limit: int = 12) -> list[MarketingLandingPagePerformance]:
        rows = self.ga_client.run_report(
            start_date="30daysAgo",
            end_date="today",
            metrics=["sessions", "totalUsers", "engagementRate", "keyEvents"],
            dimensions=["landingPage"],
            limit=max(limit, 1),
            order_bys=[{"metric": {"metricName": "sessions"}, "desc": True}],
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

    def _fetch_top_events(self, limit: int = 12) -> list[MarketingTrackingEvent]:
        rows = self.ga_client.run_report(
            start_date="30daysAgo",
            end_date="today",
            metrics=["eventCount", "totalUsers"],
            dimensions=["eventName"],
            limit=max(limit, 1),
            order_bys=[{"metric": {"metricName": "eventCount"}, "desc": True}],
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

    def _fetch_page_activity_breakdown(self, days_back: int = 30) -> list[dict[str, object]]:
        rows = self.ga_client.run_report(
            start_date=f"{days_back}daysAgo",
            end_date="today",
            metrics=[
                "screenPageViews",
                "sessions",
                "totalUsers",
                "engagedSessions",
                "engagementRate",
                "keyEvents",
                "averageSessionDuration",
            ],
            dimensions=["pagePath", "pageTitle"],
            limit=10000,
            order_bys=[{"metric": {"metricName": "screenPageViews"}, "desc": True}],
        )
        snapshot_date = date.today()
        by_page_path: dict[str, dict[str, object]] = {}
        for row in rows:
            page_path = str(row.get("pagePath") or "/")
            sessions = Decimal(row.get("sessions", 0))
            key_events = Decimal(row.get("keyEvents", 0))
            current = by_page_path.get(page_path)
            if current is None:
                by_page_path[page_path] = {
                    "snapshot_date": snapshot_date.isoformat(),
                    "page_path": page_path,
                    "page_title": str(row.get("pageTitle")) if row.get("pageTitle") else None,
                    "screen_page_views": Decimal(row.get("screenPageViews", 0)),
                    "sessions": sessions,
                    "total_users": Decimal(row.get("totalUsers", 0)),
                    "engaged_sessions": Decimal(row.get("engagedSessions", 0)),
                    "key_events": key_events,
                    "avg_session_duration_seconds": Decimal(row.get("averageSessionDuration", 0)),
                }
                continue

            current["screen_page_views"] = Decimal(current["screen_page_views"]) + Decimal(
                row.get("screenPageViews", 0)
            )
            current["sessions"] = Decimal(current["sessions"]) + sessions
            # totalUsers is non-additive across segmented rows; summing can overcount.
            current["total_users"] = max(
                Decimal(current["total_users"]),
                Decimal(row.get("totalUsers", 0)),
            )
            current["engaged_sessions"] = Decimal(current["engaged_sessions"]) + Decimal(
                row.get("engagedSessions", 0)
            )
            current["key_events"] = Decimal(current["key_events"]) + key_events
            current["avg_session_duration_seconds"] = max(
                Decimal(current["avg_session_duration_seconds"]),
                Decimal(row.get("averageSessionDuration", 0)),
            )
            if not current.get("page_title") and row.get("pageTitle"):
                current["page_title"] = str(row.get("pageTitle"))

        mapped: list[dict[str, object]] = []
        for row in by_page_path.values():
            sessions = Decimal(row["sessions"])
            engaged_sessions = Decimal(row["engaged_sessions"])
            key_events = Decimal(row["key_events"])
            mapped.append(
                {
                    **row,
                    "engagement_rate": (
                        engaged_sessions / sessions if sessions > 0 else Decimal("0")
                    ),
                    "key_event_rate": key_events / sessions if sessions > 0 else Decimal("0"),
                }
            )
        return mapped

    def _fetch_geo_breakdown(self, days_back: int = 30) -> list[dict[str, object]]:
        rows = self.ga_client.run_report(
            start_date=f"{days_back}daysAgo",
            end_date="today",
            metrics=["sessions", "totalUsers", "engagedSessions", "engagementRate", "keyEvents"],
            dimensions=["country", "region", "city"],
            limit=5000,
            order_bys=[{"metric": {"metricName": "sessions"}, "desc": True}],
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
            channel_breakdown = self._fetch_daily_channel_breakdown(days_back=800)
            pages = self._fetch_top_landing_pages(limit=20)
            events = self._fetch_top_events(limit=20)
            page_activity = self._fetch_page_activity_breakdown(days_back=30)
            geo_breakdown = self._fetch_geo_breakdown(days_back=30)
            by_date: dict[date, dict[str, Decimal]] = {}
            for row in channel_breakdown:
                snapshot_date = row["snapshot_date"]  # type: ignore[index]
                if snapshot_date not in by_date:
                    by_date[snapshot_date] = {
                        "sessions": Decimal("0"),
                        "total_users": Decimal("0"),
                        "engaged_sessions": Decimal("0"),
                        "key_events": Decimal("0"),
                    }
                day_bucket = by_date[snapshot_date]
                day_bucket["sessions"] += row["sessions"]  # type: ignore[index]
                day_bucket["total_users"] += row["total_users"]  # type: ignore[index]
                day_bucket["engaged_sessions"] += row["engaged_sessions"]  # type: ignore[index]
                day_bucket["key_events"] += row["key_events"]  # type: ignore[index]

            daily_rows = [
                {
                    "snapshot_date": snapshot_date.isoformat(),
                    "sessions": values["sessions"],
                    "total_users": values["total_users"],
                    "engaged_sessions": values["engaged_sessions"],
                    "engagement_rate": (
                        values["engaged_sessions"] / values["sessions"]
                        if values["sessions"] > 0
                        else Decimal("0")
                    ),
                    "key_events": values["key_events"],
                    "source_medium": "all",
                    "default_channel_group": "all",
                }
                for snapshot_date, values in sorted(by_date.items(), key=lambda item: item[0])
            ]
            channel_rows = [
                {
                    "snapshot_date": row["snapshot_date"].isoformat(),  # type: ignore[index]
                    "source_medium": row["source_medium"],
                    "default_channel_group": row["default_channel_group"],
                    "sessions": row["sessions"],
                    "total_users": row["total_users"],
                    "engaged_sessions": row["engaged_sessions"],
                    "engagement_rate": row["engagement_rate"],
                    "key_events": row["key_events"],
                }
                for row in channel_breakdown
            ]
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

            self.repository.upsert_daily_snapshots(daily_rows)
            self.repository.upsert_channel_snapshots(channel_rows)
            self.repository.upsert_landing_page_snapshots(page_rows)
            self.repository.upsert_event_snapshots(event_rows)
            self.repository.upsert_page_activity_snapshots(page_activity)
            self.repository.upsert_geo_snapshots(geo_breakdown)

            records_processed = (
                len(daily_rows)
                + len(channel_rows)
                + len(page_rows)
                + len(event_rows)
                + len(page_activity)
                + len(geo_breakdown)
            )
            self.repository.update_sync_run(
                run_id,
                {
                    "status": "success",
                    "records_processed": records_processed,
                    "records_created": records_processed,
                    "completed_at": datetime.now(UTC).isoformat(),
                },
            )
            return MarketingWebAnalyticsSyncResult(
                run_id=run_id or "n/a",
                status="success",
                records_processed=records_processed,
                records_created=records_processed,
                message="GA4 marketing analytics snapshots refreshed",
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

    @staticmethod
    def _summary_from_trend(
        trend: list[MarketingTimeSeriesPoint],
        start_date: date,
        end_date: date,
    ) -> _PeriodSummary:
        scoped = [point for point in trend if start_date <= point.snapshot_date <= end_date]
        if not scoped:
            return MarketingWebAnalyticsService._empty_summary()
        sessions = sum((point.sessions for point in scoped), Decimal("0"))
        users = sum((point.total_users for point in scoped), Decimal("0"))
        engaged_sessions = sum((point.engaged_sessions for point in scoped), Decimal("0"))
        key_events = sum((point.key_events for point in scoped), Decimal("0"))
        engagement_rate = engaged_sessions / sessions if sessions > 0 else Decimal("0")
        return _PeriodSummary(
            sessions=sessions,
            users=users,
            engaged_sessions=engaged_sessions,
            key_events=key_events,
            engagement_rate=engagement_rate,
        )

    def _load_daily_trend(self, run_sync: bool) -> list[MarketingTimeSeriesPoint]:
        rows = self.repository.list_latest_daily_snapshots(limit=900)
        if run_sync and (not rows or str(rows[0].get("snapshot_date")) < date.today().isoformat()):
            self.run_sync()
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

    def _latest_channels(self, limit: int) -> list[MarketingChannelPerformance]:
        rows = self.repository.list_latest_channels(limit=max(limit * 4, 40))
        if not rows:
            return []
        latest_date = str(rows[0].get("snapshot_date"))
        filtered = [row for row in rows if str(row.get("snapshot_date")) == latest_date][:limit]
        return [
            MarketingChannelPerformance(
                channel_name=str(row.get("default_channel_group") or "Unassigned"),
                sessions=Decimal(str(row.get("sessions") or 0)),
                total_users=Decimal(str(row.get("total_users") or 0)),
                engagement_rate=Decimal(str(row.get("engagement_rate") or 0)),
                key_events=Decimal(str(row.get("key_events") or 0)),
            )
            for row in filtered
        ]

    @staticmethod
    def _is_itinerary_page(page_path: str) -> bool:
        lowered = page_path.lower()
        return "itinerary" in lowered or "/trip/" in lowered

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

    def get_overview(self, *, run_sync: bool = True) -> MarketingOverview:
        self._assert_configuration()
        trend = self._load_daily_trend(run_sync=run_sync)
        latest_date = trend[-1].snapshot_date if trend else date.today()
        current = self._summary_from_trend(trend, latest_date - timedelta(days=29), latest_date)
        previous = self._summary_from_trend(
            trend,
            latest_date - timedelta(days=59),
            latest_date - timedelta(days=30),
        )
        year_ago = self._summary_from_trend(
            trend,
            latest_date - timedelta(days=394),
            latest_date - timedelta(days=365),
        )
        today_summary = self._summary_from_trend(trend, latest_date, latest_date)
        yesterday_summary = self._summary_from_trend(
            trend, latest_date - timedelta(days=1), latest_date - timedelta(days=1)
        )

        landing_pages = self._latest_pages(limit=10)
        channels = self._latest_channels(limit=6)
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

    def get_search_performance(self) -> MarketingSearchPerformance:
        _ = self._load_daily_trend(run_sync=False)
        channels = self._latest_channels(limit=8)
        pages = self._latest_pages(limit=15)
        is_gsc_connected = bool((self.settings.google_gsc_site_url or "").strip())
        return MarketingSearchPerformance(
            search_console_connected=is_gsc_connected,
            connection_message=(
                "Search Console is not connected yet. Connect it "
                "to unlock query-level SEO insights."
                if not is_gsc_connected
                else "Search Console is connected."
            ),
            top_landing_pages=pages,
            channels=channels,
            top_queries=[],
        )

    def get_page_activity(
        self, *, page_path_contains: str | None = None, limit: int = 100
    ) -> MarketingPageActivity:
        _ = self._load_daily_trend(run_sync=True)
        snapshot_date, all_pages = self._latest_page_activity_rows(
            limit=limit,
            page_path_contains=page_path_contains,
        )
        pages_with_volume = [page for page in all_pages if page.sessions >= 20]
        best_pages = sorted(pages_with_volume, key=lambda page: page.quality_score, reverse=True)[
            :10
        ]
        worst_pages = sorted(pages_with_volume, key=lambda page: page.quality_score)[:10]
        itinerary_pages = [page for page in all_pages if page.is_itinerary_page][:25]
        return MarketingPageActivity(
            snapshot_date=snapshot_date,
            metric_guide=(
                "Quality score blends key event rate, engagement rate, "
                "and traffic scale to rank pages."
            ),
            best_pages=best_pages,
            worst_pages=worst_pages,
            itinerary_pages=itinerary_pages,
            all_pages=all_pages[:50],
        )

    def get_geo_breakdown(self) -> MarketingGeoBreakdown:
        _ = self._load_daily_trend(run_sync=True)
        snapshot_date, rows = self._latest_geo_rows(limit=300)
        top_countries = sorted(rows, key=lambda row: row.sessions, reverse=True)[:12]
        return MarketingGeoBreakdown(
            snapshot_date=snapshot_date,
            rows=rows,
            top_countries=top_countries,
        )

    def get_event_catalog(self) -> MarketingEventCatalog:
        _ = self._load_daily_trend(run_sync=True)
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

    def get_ai_insights(self) -> list[MarketingAiInsight]:
        overview = self.get_overview(run_sync=False)
        insights: list[MarketingAiInsight] = []

        low_engagement_pages = sorted(
            [p for p in overview.top_landing_pages if p.engagement_rate < Decimal("0.45")],
            key=lambda item: item.sessions,
            reverse=True,
        )
        if low_engagement_pages:
            page = low_engagement_pages[0]
            insights.append(
                MarketingAiInsight(
                    insight_id="improve-low-engagement-page",
                    priority="high",
                    title="Improve a high-traffic landing page with low engagement",
                    summary=(
                        f"{page.landing_page} is attracting traffic but engagement is trailing. "
                        "This is likely a messaging or page intent mismatch."
                    ),
                    evidence_points=[
                        f"Sessions: {int(page.sessions)}",
                        f"Engagement rate: {round(float(page.engagement_rate) * 100, 1)}%",
                    ],
                    recommended_actions=[
                        "Refresh above-the-fold value proposition and CTA hierarchy.",
                        "Align headline and hero copy to the intent of top entry channels.",
                        "Run two headline+CTA variants for one business week "
                        "and compare key events.",
                    ],
                )
            )

        top_channel = overview.channels[0] if overview.channels else None
        if top_channel:
            insights.append(
                MarketingAiInsight(
                    insight_id="double-down-top-channel",
                    priority="medium",
                    title="Double down on highest-yield acquisition channel",
                    summary=(
                        f"{top_channel.channel_name} is currently your largest traffic driver. "
                        "Treat it as the control channel for short-cycle growth experiments."
                    ),
                    evidence_points=[
                        f"Sessions: {int(top_channel.sessions)}",
                        f"Key events: {int(top_channel.key_events)}",
                        f"Engagement rate: {round(float(top_channel.engagement_rate) * 100, 1)}%",
                    ],
                    recommended_actions=[
                        "Create a dedicated campaign/topic cluster "
                        "for the top two converting landing pages.",
                        "Mirror winning messaging in email and "
                        "outbound sequences for sales alignment.",
                    ],
                )
            )

        important_events = {event.event_name: event for event in overview.events}
        if "generate_lead" not in important_events and "sign_up" not in important_events:
            insights.append(
                MarketingAiInsight(
                    insight_id="track-primary-conversion-events",
                    priority="high",
                    title="Tighten conversion instrumentation",
                    summary=(
                        "Primary lead-conversion events are not "
                        "clearly visible in top tracked events."
                    ),
                    evidence_points=[
                        "Key conversion events did not appear in top event list.",
                        "Strategic funnel analysis is constrained "
                        "without clean event instrumentation.",
                    ],
                    recommended_actions=[
                        "Define one canonical primary conversion event "
                        "for marketing-qualified leads.",
                        "Define one canonical secondary conversion event "
                        "for high-intent engagement.",
                        "Reconcile GA event naming with sales handoff stages.",
                    ],
                )
            )

        if not insights:
            insights.append(
                MarketingAiInsight(
                    insight_id="baseline-stable",
                    priority="low",
                    title="Baseline is stable; prioritize focused experiments",
                    summary=(
                        "Top-level website behavior appears stable. "
                        "Use short experiment loops to find lift."
                    ),
                    evidence_points=[
                        "No major structural signal risk detected in current GA4 snapshot."
                    ],
                    recommended_actions=[
                        "Test one value proposition update on top landing page.",
                        "Add conversion-step diagnostics to validate funnel friction points.",
                    ],
                )
            )
        return insights

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
