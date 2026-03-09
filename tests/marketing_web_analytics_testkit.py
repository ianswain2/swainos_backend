from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import Any

from src.services.marketing_web_analytics_service import MarketingWebAnalyticsService


class FakeMarketingRepository:
    def __init__(self) -> None:
        self.daily_rows: dict[tuple[str], dict[str, Any]] = {}
        self.channel_rows: dict[tuple[str, str], dict[str, Any]] = {}
        self.country_rows: dict[tuple[str, str], dict[str, Any]] = {}
        self.page_rows: dict[tuple[str, str], dict[str, Any]] = {}
        self.event_rows: dict[tuple[str, str], dict[str, Any]] = {}
        self.page_activity_rows: dict[tuple[str, str], dict[str, Any]] = {}
        self.geo_rows: dict[tuple[str, str, str, str], dict[str, Any]] = {}
        self.demographic_rows: dict[tuple[str, str, str], dict[str, Any]] = {}
        self.device_rows: dict[tuple[str, str], dict[str, Any]] = {}
        self.internal_search_rows: dict[tuple[str, str], dict[str, Any]] = {}
        self.overview_rows: dict[tuple[str, str], dict[str, Any]] = {}
        self.sync_runs: list[dict[str, Any]] = []

    def _upsert(
        self,
        store: dict[tuple[Any, ...], dict[str, Any]],
        key: tuple[Any, ...],
        row: dict[str, Any],
    ) -> None:
        store[key] = row

    def create_sync_run(self, payload: dict[str, Any]) -> dict[str, Any]:
        run = {"id": "run-1", **payload}
        self.sync_runs.append(run)
        return run

    def update_sync_run(self, run_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        _ = run_id
        self.sync_runs[-1].update(payload)
        return self.sync_runs[-1]

    def upsert_daily_snapshots(self, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        for row in rows:
            self._upsert(self.daily_rows, (row["snapshot_date"],), row)
        return rows

    def upsert_channel_snapshots(self, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        for row in rows:
            key = (row["snapshot_date"], row["default_channel_group"])
            self._upsert(self.channel_rows, key, row)
        return rows

    def upsert_country_snapshots(self, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        for row in rows:
            self._upsert(self.country_rows, (row["snapshot_date"], row["country"]), row)
        return rows

    def upsert_landing_page_snapshots(self, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        for row in rows:
            self._upsert(self.page_rows, (row["snapshot_date"], row["landing_page"]), row)
        return rows

    def upsert_event_snapshots(self, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        for row in rows:
            self._upsert(self.event_rows, (row["snapshot_date"], row["event_name"]), row)
        return rows

    def upsert_page_activity_snapshots(self, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        for row in rows:
            self._upsert(self.page_activity_rows, (row["snapshot_date"], row["page_path"]), row)
        return rows

    def upsert_geo_snapshots(self, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        for row in rows:
            key = (row["snapshot_date"], row["country"], row["region"], row["city"])
            self._upsert(self.geo_rows, key, row)
        return rows

    def upsert_demographic_snapshots(self, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        for row in rows:
            key = (row["snapshot_date"], row["age_bracket"], row["gender"])
            self._upsert(self.demographic_rows, key, row)
        return rows

    def upsert_device_snapshots(self, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        for row in rows:
            self._upsert(self.device_rows, (row["snapshot_date"], row["device_category"]), row)
        return rows

    def upsert_internal_search_snapshots(self, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        for row in rows:
            self._upsert(self.internal_search_rows, (row["snapshot_date"], row["search_term"]), row)
        return rows

    def upsert_overview_period_summaries(self, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        for row in rows:
            self._upsert(self.overview_rows, (row["as_of_date"], row["summary_key"]), row)
        return rows

    def list_latest_daily_snapshots(self, limit: int = 900) -> list[dict[str, Any]]:
        _ = limit
        return sorted(self.daily_rows.values(), key=lambda row: row["snapshot_date"], reverse=True)

    def list_latest_landing_pages(self, limit: int = 30) -> list[dict[str, Any]]:
        _ = limit
        return sorted(self.page_rows.values(), key=lambda row: row["sessions"], reverse=True)

    def list_latest_events(self, limit: int = 30) -> list[dict[str, Any]]:
        _ = limit
        return sorted(self.event_rows.values(), key=lambda row: row["event_count"], reverse=True)

    def list_latest_geo(self, limit: int = 300) -> list[dict[str, Any]]:
        _ = limit
        return sorted(self.geo_rows.values(), key=lambda row: row["sessions"], reverse=True)

    def list_latest_demographics(self, limit: int = 50) -> list[dict[str, Any]]:
        _ = limit
        return sorted(self.demographic_rows.values(), key=lambda row: row["sessions"], reverse=True)

    def list_latest_devices(self, limit: int = 10) -> list[dict[str, Any]]:
        _ = limit
        return sorted(self.device_rows.values(), key=lambda row: row["sessions"], reverse=True)

    def list_latest_internal_search_terms(self, limit: int = 30) -> list[dict[str, Any]]:
        _ = limit
        return sorted(
            self.internal_search_rows.values(),
            key=lambda row: row["event_count"],
            reverse=True,
        )

    def list_latest_overview_period_summaries(self, limit: int = 12) -> list[dict[str, Any]]:
        _ = limit
        return sorted(
            self.overview_rows.values(),
            key=lambda row: (row["as_of_date"], row["summary_key"]),
            reverse=True,
        )

    def list_latest_page_activity(
        self,
        limit: int = 200,
        page_path_contains: str | None = None,
    ) -> list[dict[str, Any]]:
        _ = limit, page_path_contains
        return sorted(
            self.page_activity_rows.values(),
            key=lambda row: row["sessions"],
            reverse=True,
        )


class FakeMarketingGaClient:
    def __init__(self) -> None:
        self.window_totals: dict[tuple[str, str], dict[str, Decimal]] = {}

    def run_report(self, **kwargs: Any) -> list[dict[str, Any]]:
        dimensions = kwargs.get("dimensions") or []
        start_date = str(kwargs.get("start_date"))
        end_date = str(kwargs.get("end_date"))
        if dimensions == []:
            return [self.window_totals[(start_date, end_date)]]
        if dimensions == ["sessionDefaultChannelGroup"]:
            return [
                {
                    "sessionDefaultChannelGroup": "Organic Search",
                    "sessions": Decimal("250"),
                    "totalUsers": Decimal("170"),
                    "engagedSessions": Decimal("120"),
                    "keyEvents": Decimal("20"),
                }
            ]
        if dimensions == ["country"]:
            return [
                {
                    "country": "Canada",
                    "sessions": Decimal("200"),
                    "totalUsers": Decimal("130"),
                    "engagedSessions": Decimal("98"),
                    "keyEvents": Decimal("13"),
                }
            ]
        return []


class StubSyncMarketingService(MarketingWebAnalyticsService):
    def __init__(
        self,
        repository: FakeMarketingRepository,
        ga_client: FakeMarketingGaClient,
    ) -> None:
        super().__init__(repository=repository, ga_client=ga_client)
        self.channel_sessions = Decimal("100")

    def _assert_configuration(self) -> None:
        return None

    def _fetch_daily_totals(self, days_back: int = 800) -> list[dict[str, object]]:
        _ = days_back
        return [
            {
                "snapshot_date": date.today().isoformat(),
                "sessions": Decimal("500"),
                "total_users": Decimal("300"),
                "engaged_sessions": Decimal("220"),
                "engagement_rate": Decimal("0.44"),
                "key_events": Decimal("42"),
                "source_medium": "all",
                "default_channel_group": "all",
            }
        ]

    def _fetch_channel_totals(self, days_back: int = 800) -> list[dict[str, object]]:
        _ = days_back
        return [
            {
                "snapshot_date": date.today().isoformat(),
                "source_medium": "all",
                "default_channel_group": "Organic Search",
                "sessions": self.channel_sessions,
                "total_users": Decimal("80"),
                "engaged_sessions": Decimal("50"),
                "engagement_rate": Decimal("0.5"),
                "key_events": Decimal("5"),
            }
        ]

    def _fetch_country_totals(self, days_back: int = 800) -> list[dict[str, object]]:
        _ = days_back
        return [
            {
                "snapshot_date": date.today().isoformat(),
                "country": "United States",
                "sessions": Decimal("200"),
                "total_users": Decimal("140"),
                "engaged_sessions": Decimal("90"),
                "key_events": Decimal("11"),
                "engagement_rate": Decimal("0.45"),
                "key_event_rate": Decimal("0.055"),
            }
        ]

    def _fetch_top_landing_pages(
        self,
        *,
        days_back: int = 30,
        limit: int = 12,
        country: str | None = None,
    ):
        _ = days_back, limit, country
        return []

    def _fetch_top_events(
        self,
        *,
        limit: int = 12,
        days_back: int = 30,
        country: str | None = None,
    ):
        _ = limit, days_back, country
        return []

    def _fetch_page_activity_breakdown(
        self,
        days_back: int = 30,
        country: str | None = None,
    ) -> list[dict[str, object]]:
        _ = days_back, country
        return []

    def _fetch_geo_breakdown(
        self,
        days_back: int = 30,
        country: str | None = None,
    ) -> list[dict[str, object]]:
        _ = days_back, country
        return []

    def _fetch_demographics_breakdown(self, days_back: int = 30, country: str | None = None):
        _ = days_back, country
        return []

    def _fetch_device_breakdown(self, days_back: int = 30, country: str | None = None):
        _ = days_back, country
        return []

    def _fetch_internal_site_search_terms(
        self,
        days_back: int = 30,
        limit: int = 20,
        country: str | None = None,
    ):
        _ = days_back, limit, country
        return []

    def _build_overview_period_rows(self, *, as_of_date: date) -> list[dict[str, object]]:
        return [
            {
                "as_of_date": as_of_date.isoformat(),
                "summary_key": "current_30d",
                "start_date": as_of_date.isoformat(),
                "end_date": as_of_date.isoformat(),
                "sessions": Decimal("1"),
                "total_users": Decimal("1"),
                "engaged_sessions": Decimal("1"),
                "key_events": Decimal("1"),
                "engagement_rate": Decimal("1"),
            }
        ]
