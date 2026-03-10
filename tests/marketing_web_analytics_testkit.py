from __future__ import annotations

from datetime import date, timedelta
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
        self.search_console_daily_rows: dict[tuple[str, str, str], dict[str, Any]] = {}
        self.search_console_query_rows: dict[tuple[str, str, str, str], dict[str, Any]] = {}
        self.search_console_page_rows: dict[tuple[str, str, str, str], dict[str, Any]] = {}
        self.search_console_page_query_rows: dict[
            tuple[str, str, str, str, str], dict[str, Any]
        ] = {}
        self.search_console_country_rows: dict[tuple[str, str], dict[str, Any]] = {}
        self.search_console_device_rows: dict[tuple[str, str], dict[str, Any]] = {}
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

    def upsert_search_console_daily(self, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        for row in rows:
            key = (row["snapshot_date"], row["country_scope"], row["device_scope"])
            self._upsert(self.search_console_daily_rows, key, row)
        return rows

    def upsert_search_console_query_daily(self, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        for row in rows:
            key = (
                row["snapshot_date"],
                row["query"],
                row["country_scope"],
                row["device_scope"],
            )
            self._upsert(self.search_console_query_rows, key, row)
        return rows

    def upsert_search_console_page_daily(self, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        for row in rows:
            key = (
                row["snapshot_date"],
                row["page_path"],
                row["country_scope"],
                row["device_scope"],
            )
            self._upsert(self.search_console_page_rows, key, row)
        return rows

    def upsert_search_console_page_query_daily(
        self, rows: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        for row in rows:
            key = (
                row["snapshot_date"],
                row["page_path"],
                row["query"],
                row["country_scope"],
                row["device_scope"],
            )
            self._upsert(self.search_console_page_query_rows, key, row)
        return rows

    def upsert_search_console_country_daily(
        self, rows: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        for row in rows:
            key = (row["snapshot_date"], row["country"])
            self._upsert(self.search_console_country_rows, key, row)
        return rows

    def upsert_search_console_device_daily(
        self, rows: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        for row in rows:
            key = (row["snapshot_date"], row["device"])
            self._upsert(self.search_console_device_rows, key, row)
        return rows

    def latest_search_console_snapshot_date(
        self,
        *,
        country_scope: str = "all",
        device_scope: str = "all",
    ) -> date | None:
        rows = [
            row
            for row in self.search_console_daily_rows.values()
            if row["country_scope"] == country_scope and row["device_scope"] == device_scope
        ]
        if not rows:
            return None
        latest = max(rows, key=lambda row: row["snapshot_date"])
        return date.fromisoformat(str(latest["snapshot_date"]))

    def get_search_console_insights_rollup(
        self,
        *,
        days_back: int,
        country_scope: str = "all",
        device_scope: str = "all",
    ) -> dict[str, Any]:
        latest = self.latest_search_console_snapshot_date(
            country_scope=country_scope,
            device_scope=device_scope,
        )
        if latest is None:
            return {
                "as_of_date": None,
                "freshness_days": None,
                "query_row_count": 0,
                "overview": {
                    "total_clicks": 0,
                    "total_impressions": 0,
                    "average_ctr": 0,
                    "average_position": 0,
                    "clicks_delta_pct": None,
                    "impressions_delta_pct": None,
                    "ctr_delta_pct": None,
                    "position_delta": None,
                },
                "top_queries": [],
                "top_pages": [],
                "country_breakdown": [],
                "device_breakdown": [],
                "opportunities": [],
                "challenges": [],
            }

        normalized_days = max(days_back, 1)
        current_start = latest - timedelta(days=normalized_days - 1)
        previous_start = current_start - timedelta(days=normalized_days)
        previous_end = current_start - timedelta(days=1)

        scoped_daily = [
            row
            for row in self.search_console_daily_rows.values()
            if row["country_scope"] == country_scope and row["device_scope"] == device_scope
        ]
        current_daily = [
            row
            for row in scoped_daily
            if current_start <= date.fromisoformat(str(row["snapshot_date"])) <= latest
        ]
        previous_daily = [
            row
            for row in scoped_daily
            if previous_start <= date.fromisoformat(str(row["snapshot_date"])) <= previous_end
        ]

        def _rollup(rows: list[dict[str, Any]]) -> tuple[Decimal, Decimal, Decimal]:
            clicks = sum((Decimal(str(row.get("clicks") or 0)) for row in rows), Decimal("0"))
            impressions = sum(
                (Decimal(str(row.get("impressions") or 0)) for row in rows),
                Decimal("0"),
            )
            position_weight = sum(
                (
                    Decimal(str(row.get("average_position") or 0))
                    * Decimal(str(row.get("impressions") or 0))
                    for row in rows
                ),
                Decimal("0"),
            )
            return clicks, impressions, position_weight

        current_clicks, current_impressions, current_position_weight = _rollup(current_daily)
        previous_clicks, previous_impressions, previous_position_weight = _rollup(previous_daily)
        current_ctr = (
            current_clicks / current_impressions if current_impressions > 0 else Decimal("0")
        )
        previous_ctr = (
            previous_clicks / previous_impressions if previous_impressions > 0 else Decimal("0")
        )
        current_position = (
            current_position_weight / current_impressions
            if current_impressions > 0
            else Decimal("0")
        )
        previous_position = (
            previous_position_weight / previous_impressions
            if previous_impressions > 0
            else Decimal("0")
        )

        query_rollup: dict[str, dict[str, Any]] = {}
        for row in self.search_console_query_rows.values():
            if row["country_scope"] != country_scope or row["device_scope"] != device_scope:
                continue
            snapshot_date = date.fromisoformat(str(row["snapshot_date"]))
            if snapshot_date < current_start or snapshot_date > latest:
                continue
            query = str(row["query"])
            bucket = query_rollup.setdefault(
                query,
                {
                    "query": query,
                    "clicks": Decimal("0"),
                    "impressions": Decimal("0"),
                    "position_weight": Decimal("0"),
                    "is_branded": bool(row.get("is_branded")),
                },
            )
            clicks = Decimal(str(row.get("clicks") or 0))
            impressions = Decimal(str(row.get("impressions") or 0))
            bucket["clicks"] += clicks
            bucket["impressions"] += impressions
            bucket["position_weight"] += (
                Decimal(str(row.get("average_position") or 0)) * impressions
            )

        top_queries = sorted(
            query_rollup.values(),
            key=lambda row: row["clicks"],
            reverse=True,
        )[:25]
        top_queries_payload = [
            {
                "query": row["query"],
                "clicks": row["clicks"],
                "impressions": row["impressions"],
                "ctr": (
                    row["clicks"] / row["impressions"] if row["impressions"] > 0 else Decimal("0")
                ),
                "average_position": (
                    row["position_weight"] / row["impressions"]
                    if row["impressions"] > 0
                    else Decimal("0")
                ),
                "is_branded": bool(row["is_branded"]),
            }
            for row in top_queries
        ]

        page_rollup: dict[str, dict[str, Decimal | str]] = {}
        for row in self.search_console_page_rows.values():
            if row["country_scope"] != country_scope or row["device_scope"] != device_scope:
                continue
            snapshot_date = date.fromisoformat(str(row["snapshot_date"]))
            if snapshot_date < current_start or snapshot_date > latest:
                continue
            page_path = str(row["page_path"])
            bucket = page_rollup.setdefault(
                page_path,
                {
                    "page_path": page_path,
                    "clicks": Decimal("0"),
                    "impressions": Decimal("0"),
                    "position_weight": Decimal("0"),
                },
            )
            clicks = Decimal(str(row.get("clicks") or 0))
            impressions = Decimal(str(row.get("impressions") or 0))
            bucket["clicks"] = Decimal(str(bucket["clicks"])) + clicks
            bucket["impressions"] = Decimal(str(bucket["impressions"])) + impressions
            bucket["position_weight"] = Decimal(str(bucket["position_weight"])) + (
                Decimal(str(row.get("average_position") or 0)) * impressions
            )

        top_pages = sorted(
            page_rollup.values(), key=lambda row: Decimal(str(row["clicks"])), reverse=True
        )[:25]
        top_pages_payload = [
            {
                "page_path": str(row["page_path"]),
                "clicks": Decimal(str(row["clicks"])),
                "impressions": Decimal(str(row["impressions"])),
                "ctr": (
                    Decimal(str(row["clicks"])) / Decimal(str(row["impressions"]))
                    if Decimal(str(row["impressions"])) > 0
                    else Decimal("0")
                ),
                "average_position": (
                    Decimal(str(row["position_weight"])) / Decimal(str(row["impressions"]))
                    if Decimal(str(row["impressions"])) > 0
                    else Decimal("0")
                ),
            }
            for row in top_pages
        ]

        country_breakdown = []
        if country_scope == "all":
            for row in self.search_console_country_rows.values():
                snapshot_date = date.fromisoformat(str(row["snapshot_date"]))
                if snapshot_date < current_start or snapshot_date > latest:
                    continue
                impressions = Decimal(str(row.get("impressions") or 0))
                clicks = Decimal(str(row.get("clicks") or 0))
                country_breakdown.append(
                    {
                        "label": str(row["country"]),
                        "clicks": clicks,
                        "impressions": impressions,
                        "ctr": clicks / impressions if impressions > 0 else Decimal("0"),
                        "average_position": Decimal(str(row.get("average_position") or 0)),
                    }
                )
            country_breakdown = sorted(
                country_breakdown,
                key=lambda row: Decimal(str(row["clicks"])),
                reverse=True,
            )[:12]

        device_breakdown = []
        if country_scope == "all":
            for row in self.search_console_device_rows.values():
                snapshot_date = date.fromisoformat(str(row["snapshot_date"]))
                if snapshot_date < current_start or snapshot_date > latest:
                    continue
                impressions = Decimal(str(row.get("impressions") or 0))
                clicks = Decimal(str(row.get("clicks") or 0))
                device_breakdown.append(
                    {
                        "label": str(row["device"]),
                        "clicks": clicks,
                        "impressions": impressions,
                        "ctr": clicks / impressions if impressions > 0 else Decimal("0"),
                        "average_position": Decimal(str(row.get("average_position") or 0)),
                    }
                )
            device_breakdown = sorted(
                device_breakdown,
                key=lambda row: Decimal(str(row["clicks"])),
                reverse=True,
            )[:8]

        return {
            "as_of_date": latest.isoformat(),
            "freshness_days": (date.today() - latest).days,
            "query_row_count": len(top_queries_payload),
            "overview": {
                "total_clicks": current_clicks,
                "total_impressions": current_impressions,
                "average_ctr": current_ctr,
                "average_position": current_position,
                "clicks_delta_pct": (
                    (current_clicks - previous_clicks) / previous_clicks
                    if previous_clicks > 0
                    else None
                ),
                "impressions_delta_pct": (
                    (current_impressions - previous_impressions) / previous_impressions
                    if previous_impressions > 0
                    else None
                ),
                "ctr_delta_pct": (
                    (current_ctr - previous_ctr) / previous_ctr if previous_ctr > 0 else None
                ),
                "position_delta": (
                    current_position - previous_position if previous_impressions > 0 else None
                ),
            },
            "top_queries": top_queries_payload,
            "top_pages": top_pages_payload,
            "country_breakdown": country_breakdown,
            "device_breakdown": device_breakdown,
            "opportunities": [],
            "challenges": [],
        }

    def get_search_console_us_workspace_rollup(self, *, days_back: int) -> dict[str, Any]:
        base = self.get_search_console_insights_rollup(
            days_back=days_back,
            country_scope="United States",
            device_scope="all",
        )
        if base.get("as_of_date") is None:
            return {
                **base,
                "market_benchmarks": [],
                "query_intent_buckets": [],
                "position_band_summary": [],
            }

        benchmarks: list[dict[str, Any]] = []
        for label in ("United States", "Australia", "New Zealand", "South Africa"):
            clicks = Decimal("0")
            impressions = Decimal("0")
            position_weight = Decimal("0")
            for row in self.search_console_country_rows.values():
                if str(row.get("country")) != label:
                    continue
                clicks += Decimal(str(row.get("clicks") or 0))
                row_impressions = Decimal(str(row.get("impressions") or 0))
                impressions += row_impressions
                position_weight += Decimal(str(row.get("average_position") or 0)) * row_impressions
            benchmarks.append(
                {
                    "market_label": label,
                    "clicks": clicks,
                    "impressions": impressions,
                    "ctr": clicks / impressions if impressions > 0 else Decimal("0"),
                    "average_position": (
                        position_weight / impressions if impressions > 0 else Decimal("0")
                    ),
                }
            )

        return {
            **base,
            "top_queries": [
                {
                    **row,
                    "intent_bucket": "core_travel_intent",
                    "term_type": "short_tail",
                    "position_band": "4-10",
                }
                for row in list(base.get("top_queries") or [])
            ],
            "market_benchmarks": benchmarks,
            "query_intent_buckets": [
                {
                    "bucket_label": "core_travel_intent",
                    "query_count": len(base.get("top_queries") or []),
                    "clicks": sum(
                        (
                            Decimal(str(row.get("clicks") or 0))
                            for row in list(base.get("top_queries") or [])
                        ),
                        Decimal("0"),
                    ),
                    "impressions": sum(
                        (
                            Decimal(str(row.get("impressions") or 0))
                            for row in list(base.get("top_queries") or [])
                        ),
                        Decimal("0"),
                    ),
                    "average_ctr": Decimal("0.1"),
                }
            ],
            "position_band_summary": [
                {
                    "band_label": "4-10",
                    "query_count": len(base.get("top_queries") or []),
                    "clicks": sum(
                        (
                            Decimal(str(row.get("clicks") or 0))
                            for row in list(base.get("top_queries") or [])
                        ),
                        Decimal("0"),
                    ),
                    "impressions": sum(
                        (
                            Decimal(str(row.get("impressions") or 0))
                            for row in list(base.get("top_queries") or [])
                        ),
                        Decimal("0"),
                    ),
                    "average_ctr": Decimal("0.1"),
                }
            ],
        }

    def get_search_console_page_profile_rollup(
        self,
        *,
        days_back: int,
        page_path: str,
    ) -> dict[str, Any]:
        base = self.get_search_console_insights_rollup(
            days_back=days_back,
            country_scope="United States",
            device_scope="all",
        )
        page_rows = [
            row
            for row in self.search_console_page_rows.values()
            if str(row.get("page_path")) == page_path
            and str(row.get("country_scope")) == "United States"
        ]
        query_rows = [
            row
            for row in self.search_console_page_query_rows.values()
            if str(row.get("page_path")) == page_path
            and str(row.get("country_scope")) == "United States"
        ]
        total_clicks = sum(
            (Decimal(str(row.get("clicks") or 0)) for row in page_rows),
            Decimal("0"),
        )
        total_impressions = sum(
            (Decimal(str(row.get("impressions") or 0)) for row in page_rows),
            Decimal("0"),
        )
        position_weight = sum(
            (
                Decimal(str(row.get("average_position") or 0))
                * Decimal(str(row.get("impressions") or 0))
                for row in page_rows
            ),
            Decimal("0"),
        )
        return {
            "page_path": page_path,
            "as_of_date": base.get("as_of_date"),
            "overview": {
                "total_clicks": total_clicks,
                "total_impressions": total_impressions,
                "average_ctr": (
                    total_clicks / total_impressions if total_impressions > 0 else Decimal("0")
                ),
                "average_position": (
                    position_weight / total_impressions if total_impressions > 0 else Decimal("0")
                ),
            },
            "daily_trend": [
                {
                    "snapshot_date": row.get("snapshot_date"),
                    "clicks": row.get("clicks"),
                    "impressions": row.get("impressions"),
                    "ctr": row.get("ctr"),
                    "average_position": row.get("average_position"),
                }
                for row in page_rows
            ],
            "top_queries": [
                {
                    "query": row.get("query"),
                    "clicks": row.get("clicks"),
                    "impressions": row.get("impressions"),
                    "ctr": row.get("ctr"),
                    "average_position": row.get("average_position"),
                    "is_branded": False,
                }
                for row in query_rows[:20]
            ],
        }

    def list_search_console_daily(
        self,
        *,
        start_date: date,
        country_scope: str = "all",
        device_scope: str = "all",
    ) -> list[dict[str, Any]]:
        return [
            row
            for row in self.search_console_daily_rows.values()
            if date.fromisoformat(str(row["snapshot_date"])) >= start_date
            and row["country_scope"] == country_scope
            and row["device_scope"] == device_scope
        ]

    def list_search_console_query_daily(
        self,
        *,
        start_date: date,
        country_scope: str = "all",
        device_scope: str = "all",
    ) -> list[dict[str, Any]]:
        return [
            row
            for row in self.search_console_query_rows.values()
            if date.fromisoformat(str(row["snapshot_date"])) >= start_date
            and row["country_scope"] == country_scope
            and row["device_scope"] == device_scope
        ]

    def list_search_console_page_daily(
        self,
        *,
        start_date: date,
        country_scope: str = "all",
        device_scope: str = "all",
    ) -> list[dict[str, Any]]:
        return [
            row
            for row in self.search_console_page_rows.values()
            if date.fromisoformat(str(row["snapshot_date"])) >= start_date
            and row["country_scope"] == country_scope
            and row["device_scope"] == device_scope
        ]

    def list_search_console_page_query_daily(
        self,
        *,
        start_date: date,
        country_scope: str = "all",
        device_scope: str = "all",
    ) -> list[dict[str, Any]]:
        return [
            row
            for row in self.search_console_page_query_rows.values()
            if date.fromisoformat(str(row["snapshot_date"])) >= start_date
            and row["country_scope"] == country_scope
            and row["device_scope"] == device_scope
        ]

    def list_search_console_country_daily(self, *, start_date: date) -> list[dict[str, Any]]:
        return [
            row
            for row in self.search_console_country_rows.values()
            if date.fromisoformat(str(row["snapshot_date"])) >= start_date
        ]

    def list_search_console_device_daily(self, *, start_date: date) -> list[dict[str, Any]]:
        return [
            row
            for row in self.search_console_device_rows.values()
            if date.fromisoformat(str(row["snapshot_date"])) >= start_date
        ]


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
