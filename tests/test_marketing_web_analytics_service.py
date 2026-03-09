from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal
from typing import Any

from marketing_web_analytics_testkit import (
    FakeMarketingGaClient,
    FakeMarketingRepository,
    StubSyncMarketingService,
)

from src.services.marketing_web_analytics_service import MarketingWebAnalyticsService


def test_run_sync_is_overwrite_safe_for_same_day_facts() -> None:
    repository = FakeMarketingRepository()
    service = StubSyncMarketingService(
        repository=repository,
        ga_client=FakeMarketingGaClient(),
    )
    service.run_sync()
    service.channel_sessions = Decimal("175")
    service.run_sync()

    today = date.today().isoformat()
    assert len(repository.channel_rows) == 1
    assert repository.channel_rows[(today, "Organic Search")]["sessions"] == Decimal("175")
    assert len(repository.overview_rows) == 1


def test_overview_uses_snapshot_period_users_not_summed_daily_users() -> None:
    repository = FakeMarketingRepository()
    ga_client = FakeMarketingGaClient()
    service = MarketingWebAnalyticsService(repository=repository, ga_client=ga_client)
    latest = date.today()
    previous_day = latest - timedelta(days=1)

    repository.upsert_daily_snapshots(
        [
            {
                "snapshot_date": previous_day.isoformat(),
                "sessions": Decimal("100"),
                "total_users": Decimal("90"),
                "engaged_sessions": Decimal("70"),
                "engagement_rate": Decimal("0.7"),
                "key_events": Decimal("8"),
                "source_medium": "all",
                "default_channel_group": "all",
            },
            {
                "snapshot_date": latest.isoformat(),
                "sessions": Decimal("120"),
                "total_users": Decimal("110"),
                "engaged_sessions": Decimal("72"),
                "engagement_rate": Decimal("0.6"),
                "key_events": Decimal("9"),
                "source_medium": "all",
                "default_channel_group": "all",
            },
        ]
    )
    repository.upsert_overview_period_summaries(
        [
            {
                "as_of_date": latest.isoformat(),
                "summary_key": "current_30d",
                "start_date": latest.isoformat(),
                "end_date": latest.isoformat(),
                "sessions": Decimal("220"),
                "total_users": Decimal("150"),
                "engaged_sessions": Decimal("142"),
                "key_events": Decimal("17"),
                "engagement_rate": Decimal("0.64"),
            },
            {
                "as_of_date": latest.isoformat(),
                "summary_key": "previous_30d",
                "start_date": latest.isoformat(),
                "end_date": latest.isoformat(),
                "sessions": Decimal("0"),
                "total_users": Decimal("0"),
                "engaged_sessions": Decimal("0"),
                "key_events": Decimal("0"),
                "engagement_rate": Decimal("0"),
            },
            {
                "as_of_date": latest.isoformat(),
                "summary_key": "year_ago_30d",
                "start_date": latest.isoformat(),
                "end_date": latest.isoformat(),
                "sessions": Decimal("0"),
                "total_users": Decimal("0"),
                "engaged_sessions": Decimal("0"),
                "key_events": Decimal("0"),
                "engagement_rate": Decimal("0"),
            },
            {
                "as_of_date": latest.isoformat(),
                "summary_key": "today",
                "start_date": latest.isoformat(),
                "end_date": latest.isoformat(),
                "sessions": Decimal("120"),
                "total_users": Decimal("110"),
                "engaged_sessions": Decimal("72"),
                "key_events": Decimal("9"),
                "engagement_rate": Decimal("0.6"),
            },
            {
                "as_of_date": latest.isoformat(),
                "summary_key": "yesterday",
                "start_date": previous_day.isoformat(),
                "end_date": previous_day.isoformat(),
                "sessions": Decimal("100"),
                "total_users": Decimal("90"),
                "engaged_sessions": Decimal("70"),
                "key_events": Decimal("8"),
                "engagement_rate": Decimal("0.7"),
            },
        ]
    )

    overview = service.get_overview(run_sync=False)
    users_kpi = next(kpi for kpi in overview.kpis if kpi.metric_key == "totalUsers")
    assert users_kpi.current_value == Decimal("150")


def test_geo_top_countries_uses_exact_window_totals() -> None:
    repository = FakeMarketingRepository()
    service = MarketingWebAnalyticsService(
        repository=repository,
        ga_client=FakeMarketingGaClient(),
    )
    today = date.today().isoformat()

    repository.upsert_geo_snapshots(
        [
            {
                "snapshot_date": today,
                "country": "Canada",
                "region": "ON",
                "city": "Toronto",
                "sessions": Decimal("90"),
                "total_users": Decimal("70"),
                "engaged_sessions": Decimal("40"),
                "key_events": Decimal("4"),
                "engagement_rate": Decimal("0.44"),
                "key_event_rate": Decimal("0.04"),
            }
        ]
    )
    repository.upsert_daily_snapshots(
        [
            {
                "snapshot_date": today,
                "sessions": Decimal("1"),
                "total_users": Decimal("1"),
                "engaged_sessions": Decimal("1"),
                "engagement_rate": Decimal("1"),
                "key_events": Decimal("1"),
                "source_medium": "all",
                "default_channel_group": "all",
            }
        ]
    )

    result = service.get_geo_breakdown(days_back=30)
    assert result.top_countries[0].country == "Canada"
    assert result.top_countries[0].total_users == Decimal("130")


def test_search_performance_30d_reads_internal_search_from_snapshot() -> None:
    repository = FakeMarketingRepository()
    service = MarketingWebAnalyticsService(
        repository=repository,
        ga_client=FakeMarketingGaClient(),
    )
    today = date.today().isoformat()

    repository.upsert_internal_search_snapshots(
        [
            {
                "snapshot_date": today,
                "search_term": "australia luxury",
                "event_count": Decimal("12"),
                "total_users": Decimal("8"),
            }
        ]
    )
    repository.upsert_daily_snapshots(
        [
            {
                "snapshot_date": today,
                "sessions": Decimal("1"),
                "total_users": Decimal("1"),
                "engaged_sessions": Decimal("1"),
                "engagement_rate": Decimal("1"),
                "key_events": Decimal("1"),
                "source_medium": "all",
                "default_channel_group": "all",
            }
        ]
    )

    result = service.get_search_performance(days_back=30)
    assert result.internal_site_search_terms[0].search_term == "australia luxury"
    assert result.internal_site_search_terms[0].event_count == Decimal("12")


def test_search_performance_country_scope_reads_live_ga4_terms() -> None:
    class CountryScopedGaClient(FakeMarketingGaClient):
        def run_report(self, **kwargs: Any) -> list[dict[str, Any]]:
            dimensions = kwargs.get("dimensions") or []
            if dimensions == ["searchTerm"]:
                return [
                    {
                        "searchTerm": "honeymoon samoa",
                        "eventCount": Decimal("9"),
                        "totalUsers": Decimal("5"),
                    }
                ]
            return super().run_report(**kwargs)

    class CountryScopedService(MarketingWebAnalyticsService):
        def _assert_configuration(self) -> None:
            return None

    repository = FakeMarketingRepository()
    service = CountryScopedService(
        repository=repository,
        ga_client=CountryScopedGaClient(),
    )
    today = date.today().isoformat()
    repository.upsert_internal_search_snapshots(
        [
            {
                "snapshot_date": today,
                "search_term": "snapshot-only",
                "event_count": Decimal("99"),
                "total_users": Decimal("40"),
            }
        ]
    )

    result = service.get_search_performance(days_back=30, country="United States")
    assert result.internal_site_search_terms[0].search_term == "honeymoon samoa"
    assert result.internal_site_search_terms[0].event_count == Decimal("9")


def test_search_performance_includes_referral_and_source_value_mix() -> None:
    class SourceMixGaClient(FakeMarketingGaClient):
        def run_report(self, **kwargs: Any) -> list[dict[str, Any]]:
            dimensions = kwargs.get("dimensions") or []
            if dimensions == ["sessionSourceMedium", "sessionDefaultChannelGroup"]:
                return [
                    {
                        "sessionSourceMedium": "google / organic",
                        "sessionDefaultChannelGroup": "Organic Search",
                        "sessions": Decimal("300"),
                        "totalUsers": Decimal("210"),
                        "engagedSessions": Decimal("180"),
                        "keyEvents": Decimal("24"),
                        "bounceRate": Decimal("0.40"),
                    },
                    {
                        "sessionSourceMedium": "tripadvisor.com / referral",
                        "sessionDefaultChannelGroup": "Referral",
                        "sessions": Decimal("120"),
                        "totalUsers": Decimal("90"),
                        "engagedSessions": Decimal("72"),
                        "keyEvents": Decimal("14"),
                        "bounceRate": Decimal("0.34"),
                    },
                ]
            return super().run_report(**kwargs)

    class SourceMixService(MarketingWebAnalyticsService):
        def _assert_configuration(self) -> None:
            return None

    service = SourceMixService(repository=FakeMarketingRepository(), ga_client=SourceMixGaClient())
    result = service.get_search_performance(days_back=90, country="United States")
    assert result.source_mix[0].source == "google"
    assert result.source_mix[0].bounce_rate == Decimal("0.40")
    assert result.source_mix[0].quality_label in {"qualified", "mixed", "poor"}
    assert result.referral_sources[0].source == "tripadvisor.com"
    assert result.top_valuable_sources[0].value_score >= result.top_valuable_sources[1].value_score
