from __future__ import annotations

from datetime import date, timedelta
from typing import Any

from src.core.supabase import SupabaseClient


class MarketingWebAnalyticsRepository:
    def __init__(self) -> None:
        self.client = SupabaseClient()

    def create_sync_run(self, payload: dict[str, Any]) -> dict[str, Any]:
        rows = self.client.insert(table="marketing_web_analytics_sync_runs", payload=payload)
        return rows[0] if rows else {}

    def update_sync_run(self, run_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        rows = self.client.update(
            table="marketing_web_analytics_sync_runs",
            payload=payload,
            filters=[("id", f"eq.{run_id}")],
        )
        return rows[0] if rows else {}

    def upsert_daily_snapshots(self, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        if not rows:
            return []
        return self.client.insert(
            table="marketing_web_analytics_daily",
            payload=rows,
            upsert=True,
            on_conflict="snapshot_date",
        )

    def upsert_channel_snapshots(self, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        if not rows:
            return []
        return self.client.insert(
            table="marketing_web_analytics_channels_daily",
            payload=rows,
            upsert=True,
            on_conflict="snapshot_date,default_channel_group",
        )

    def upsert_country_snapshots(self, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        if not rows:
            return []
        return self._chunked_upsert(
            table="marketing_web_analytics_country_daily",
            rows=rows,
            on_conflict="snapshot_date,country",
            chunk_size=500,
        )

    def upsert_landing_page_snapshots(self, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        if not rows:
            return []
        return self.client.insert(
            table="marketing_web_analytics_landing_pages_daily",
            payload=rows,
            upsert=True,
            on_conflict="snapshot_date,landing_page",
        )

    def upsert_event_snapshots(self, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        if not rows:
            return []
        return self.client.insert(
            table="marketing_web_analytics_events_daily",
            payload=rows,
            upsert=True,
            on_conflict="snapshot_date,event_name",
        )

    def upsert_page_activity_snapshots(self, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        if not rows:
            return []
        return self._chunked_upsert(
            table="marketing_web_analytics_page_activity_daily",
            rows=rows,
            on_conflict="snapshot_date,page_path",
            chunk_size=500,
        )

    def upsert_geo_snapshots(self, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        if not rows:
            return []
        return self._chunked_upsert(
            table="marketing_web_analytics_geo_daily",
            rows=rows,
            on_conflict="snapshot_date,country,region,city",
            chunk_size=500,
        )

    def upsert_demographic_snapshots(self, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        if not rows:
            return []
        return self._chunked_upsert(
            table="marketing_web_analytics_demographics_daily",
            rows=rows,
            on_conflict="snapshot_date,age_bracket,gender",
            chunk_size=500,
        )

    def upsert_device_snapshots(self, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        if not rows:
            return []
        return self._chunked_upsert(
            table="marketing_web_analytics_devices_daily",
            rows=rows,
            on_conflict="snapshot_date,device_category",
            chunk_size=200,
        )

    def upsert_internal_search_snapshots(self, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        if not rows:
            return []
        return self._chunked_upsert(
            table="marketing_web_analytics_internal_search_daily",
            rows=rows,
            on_conflict="snapshot_date,search_term",
            chunk_size=500,
        )

    def upsert_overview_period_summaries(self, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        if not rows:
            return []
        return self.client.insert(
            table="marketing_web_analytics_overview_period_summaries",
            payload=rows,
            upsert=True,
            on_conflict="as_of_date,summary_key",
        )

    def upsert_search_console_daily(self, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        if not rows:
            return []
        return self._chunked_upsert(
            table="marketing_search_console_daily",
            rows=rows,
            on_conflict="snapshot_date,country_scope,device_scope",
            chunk_size=500,
        )

    def upsert_search_console_query_daily(self, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        if not rows:
            return []
        return self._chunked_upsert(
            table="marketing_search_console_query_daily",
            rows=rows,
            on_conflict="snapshot_date,query,country_scope,device_scope",
            chunk_size=1000,
        )

    def upsert_search_console_page_daily(self, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        if not rows:
            return []
        return self._chunked_upsert(
            table="marketing_search_console_page_daily",
            rows=rows,
            on_conflict="snapshot_date,page_path,country_scope,device_scope",
            chunk_size=1000,
        )

    def upsert_search_console_page_query_daily(
        self, rows: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        if not rows:
            return []
        return self._chunked_upsert(
            table="marketing_search_console_page_query_daily",
            rows=rows,
            on_conflict="snapshot_date,page_path,query,country_scope,device_scope",
            chunk_size=1000,
        )

    def upsert_search_console_country_daily(
        self, rows: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        if not rows:
            return []
        return self._chunked_upsert(
            table="marketing_search_console_country_daily",
            rows=rows,
            on_conflict="snapshot_date,country",
            chunk_size=500,
        )

    def upsert_search_console_device_daily(
        self, rows: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        if not rows:
            return []
        return self._chunked_upsert(
            table="marketing_search_console_device_daily",
            rows=rows,
            on_conflict="snapshot_date,device",
            chunk_size=500,
        )

    def list_daily_snapshots(self, days_back: int = 120) -> list[dict[str, Any]]:
        start_date = (date.today() - timedelta(days=max(days_back, 1))).isoformat()
        rows, _ = self.client.select(
            table="marketing_web_analytics_daily",
            select=(
                "snapshot_date,sessions,total_users,engaged_sessions,engagement_rate,key_events,"
                "source_medium,default_channel_group,created_at,updated_at"
            ),
            filters=[("snapshot_date", f"gte.{start_date}")],
            order="snapshot_date.asc",
            limit=max(days_back + 5, 30),
        )
        return rows

    def list_latest_daily_snapshots(self, limit: int = 60) -> list[dict[str, Any]]:
        rows, _ = self.client.select(
            table="marketing_web_analytics_daily",
            select=(
                "snapshot_date,sessions,total_users,engaged_sessions,engagement_rate,key_events,"
                "source_medium,default_channel_group,created_at,updated_at"
            ),
            filters=[("source_medium", "eq.all"), ("default_channel_group", "eq.all")],
            order="snapshot_date.desc",
            limit=max(limit, 1),
        )
        return rows

    def list_latest_landing_pages(self, limit: int = 25) -> list[dict[str, Any]]:
        rows, _ = self.client.select(
            table="marketing_web_analytics_landing_pages_daily",
            select=(
                "snapshot_date,landing_page,sessions,total_users,engagement_rate,key_events,"
                "avg_session_duration_seconds"
            ),
            order="snapshot_date.desc,sessions.desc",
            limit=max(limit, 1),
        )
        return rows

    def list_latest_events(self, limit: int = 25) -> list[dict[str, Any]]:
        rows, _ = self.client.select(
            table="marketing_web_analytics_events_daily",
            select="snapshot_date,event_name,event_count,total_users,event_value_amount",
            order="snapshot_date.desc,event_count.desc",
            limit=max(limit, 1),
        )
        return rows

    def list_latest_page_activity(
        self,
        limit: int = 100,
        page_path_contains: str | None = None,
    ) -> list[dict[str, Any]]:
        filters: list[tuple[str, str]] = []
        if page_path_contains:
            filters.append(("page_path", f"ilike.*{page_path_contains}*"))
        rows, _ = self.client.select(
            table="marketing_web_analytics_page_activity_daily",
            select=(
                "snapshot_date,page_path,page_title,screen_page_views,sessions,total_users,"
                "engaged_sessions,key_events,engagement_rate,key_event_rate,avg_session_duration_seconds"
            ),
            filters=filters,
            order="snapshot_date.desc,sessions.desc",
            limit=max(limit, 1),
        )
        return rows

    def list_latest_geo(self, limit: int = 200) -> list[dict[str, Any]]:
        rows, _ = self.client.select(
            table="marketing_web_analytics_geo_daily",
            select=(
                "snapshot_date,country,region,city,sessions,total_users,engaged_sessions,key_events,"
                "engagement_rate,key_event_rate"
            ),
            order="snapshot_date.desc,sessions.desc",
            limit=max(limit, 1),
        )
        return rows

    def list_latest_demographics(self, limit: int = 50) -> list[dict[str, Any]]:
        rows, _ = self.client.select(
            table="marketing_web_analytics_demographics_daily",
            select=(
                "snapshot_date,age_bracket,gender,sessions,total_users,engaged_sessions,key_events,"
                "engagement_rate"
            ),
            order="snapshot_date.desc,sessions.desc",
            limit=max(limit, 1),
        )
        return rows

    def list_latest_devices(self, limit: int = 10) -> list[dict[str, Any]]:
        rows, _ = self.client.select(
            table="marketing_web_analytics_devices_daily",
            select=(
                "snapshot_date,device_category,sessions,total_users,engaged_sessions,key_events,"
                "engagement_rate"
            ),
            order="snapshot_date.desc,sessions.desc",
            limit=max(limit, 1),
        )
        return rows

    def list_latest_internal_search_terms(self, limit: int = 30) -> list[dict[str, Any]]:
        rows, _ = self.client.select(
            table="marketing_web_analytics_internal_search_daily",
            select="snapshot_date,search_term,event_count,total_users",
            order="snapshot_date.desc,event_count.desc",
            limit=max(limit, 1),
        )
        return rows

    def list_latest_overview_period_summaries(self, limit: int = 12) -> list[dict[str, Any]]:
        rows, _ = self.client.select(
            table="marketing_web_analytics_overview_period_summaries",
            select=(
                "as_of_date,summary_key,start_date,end_date,sessions,total_users,engaged_sessions,"
                "key_events,engagement_rate"
            ),
            order="as_of_date.desc,summary_key.asc",
            limit=max(limit, 1),
        )
        return rows

    def latest_sync_run(self) -> dict[str, Any] | None:
        rows, _ = self.client.select(
            table="marketing_web_analytics_sync_runs",
            select=(
                "id,status,started_at,completed_at,records_processed,records_created,error_message,"
                "source_system,data_window_start,data_window_end"
            ),
            order="started_at.desc",
            limit=1,
        )
        return rows[0] if rows else None

    def latest_search_console_snapshot_date(
        self,
        *,
        country_scope: str = "all",
        device_scope: str = "all",
    ) -> date | None:
        filters = [
            ("country_scope", f"eq.{country_scope}"),
            ("device_scope", f"eq.{device_scope}"),
        ]
        rows, _ = self.client.select(
            table="marketing_search_console_daily",
            select="snapshot_date",
            filters=filters,
            order="snapshot_date.desc",
            limit=1,
        )
        if not rows:
            return None
        snapshot_date = rows[0].get("snapshot_date")
        if not snapshot_date:
            return None
        return date.fromisoformat(str(snapshot_date))

    def get_search_console_insights_rollup(
        self,
        *,
        days_back: int,
        country_scope: str = "all",
        device_scope: str = "all",
    ) -> dict[str, Any]:
        payload = {
            "p_days_back": days_back,
            "p_country_scope": country_scope,
            "p_device_scope": device_scope,
        }
        result = self.client.rpc("marketing_search_console_insights_rollup_v1", payload=payload)
        return result if isinstance(result, dict) else {}

    def get_search_console_us_workspace_rollup(self, *, days_back: int) -> dict[str, Any]:
        payload = {"p_days_back": days_back}
        result = self.client.rpc("marketing_search_console_us_workspace_v1", payload=payload)
        return result if isinstance(result, dict) else {}

    def get_search_console_page_profile_rollup(
        self,
        *,
        days_back: int,
        page_path: str,
    ) -> dict[str, Any]:
        payload = {"p_days_back": days_back, "p_page_path": page_path}
        result = self.client.rpc("marketing_search_console_page_profile_v1", payload=payload)
        return result if isinstance(result, dict) else {}

    def list_search_console_daily(
        self,
        *,
        start_date: date,
        country_scope: str = "all",
        device_scope: str = "all",
    ) -> list[dict[str, Any]]:
        rows, _ = self.client.select(
            table="marketing_search_console_daily",
            select="snapshot_date,country_scope,device_scope,clicks,impressions,ctr,average_position",
            filters=[
                ("snapshot_date", f"gte.{start_date.isoformat()}"),
                ("country_scope", f"eq.{country_scope}"),
                ("device_scope", f"eq.{device_scope}"),
            ],
            order="snapshot_date.asc",
            limit=5000,
        )
        return rows

    def list_search_console_query_daily(
        self,
        *,
        start_date: date,
        country_scope: str = "all",
        device_scope: str = "all",
    ) -> list[dict[str, Any]]:
        rows, _ = self.client.select(
            table="marketing_search_console_query_daily",
            select=(
                "snapshot_date,query,country_scope,device_scope,clicks,impressions,ctr,"
                "average_position,is_branded"
            ),
            filters=[
                ("snapshot_date", f"gte.{start_date.isoformat()}"),
                ("country_scope", f"eq.{country_scope}"),
                ("device_scope", f"eq.{device_scope}"),
            ],
            order="clicks.desc",
            limit=25000,
        )
        return rows

    def list_search_console_page_daily(
        self,
        *,
        start_date: date,
        country_scope: str = "all",
        device_scope: str = "all",
    ) -> list[dict[str, Any]]:
        rows, _ = self.client.select(
            table="marketing_search_console_page_daily",
            select="snapshot_date,page_path,country_scope,device_scope,clicks,impressions,ctr,average_position",
            filters=[
                ("snapshot_date", f"gte.{start_date.isoformat()}"),
                ("country_scope", f"eq.{country_scope}"),
                ("device_scope", f"eq.{device_scope}"),
            ],
            order="clicks.desc",
            limit=25000,
        )
        return rows

    def list_search_console_page_query_daily(
        self,
        *,
        start_date: date,
        country_scope: str = "all",
        device_scope: str = "all",
    ) -> list[dict[str, Any]]:
        rows, _ = self.client.select(
            table="marketing_search_console_page_query_daily",
            select="snapshot_date,page_path,query,country_scope,device_scope,clicks,impressions,ctr,average_position",
            filters=[
                ("snapshot_date", f"gte.{start_date.isoformat()}"),
                ("country_scope", f"eq.{country_scope}"),
                ("device_scope", f"eq.{device_scope}"),
            ],
            order="impressions.desc",
            limit=25000,
        )
        return rows

    def list_search_console_country_daily(self, *, start_date: date) -> list[dict[str, Any]]:
        rows, _ = self.client.select(
            table="marketing_search_console_country_daily",
            select="snapshot_date,country,clicks,impressions,ctr,average_position",
            filters=[("snapshot_date", f"gte.{start_date.isoformat()}")],
            order="clicks.desc",
            limit=5000,
        )
        return rows

    def list_search_console_device_daily(self, *, start_date: date) -> list[dict[str, Any]]:
        rows, _ = self.client.select(
            table="marketing_search_console_device_daily",
            select="snapshot_date,device,clicks,impressions,ctr,average_position",
            filters=[("snapshot_date", f"gte.{start_date.isoformat()}")],
            order="clicks.desc",
            limit=5000,
        )
        return rows

    def _chunked_upsert(
        self,
        *,
        table: str,
        rows: list[dict[str, Any]],
        on_conflict: str,
        chunk_size: int,
    ) -> list[dict[str, Any]]:
        merged: list[dict[str, Any]] = []
        normalized_chunk_size = max(chunk_size, 1)
        for start in range(0, len(rows), normalized_chunk_size):
            chunk = rows[start : start + normalized_chunk_size]
            chunk_result = self.client.insert(
                table=table,
                payload=chunk,
                upsert=True,
                on_conflict=on_conflict,
            )
            merged.extend(chunk_result)
        return merged
