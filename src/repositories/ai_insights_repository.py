from __future__ import annotations

import json
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Tuple

from src.core.supabase import SupabaseClient
from src.schemas.ai_insights import (
    AiInsightsFeedFilters,
    AiInsightsHistoryFilters,
    AiRecommendationFilters,
)

MAX_CONTEXT_ROWS = 500


class AiInsightsRepository:
    def __init__(self) -> None:
        self.client = SupabaseClient()

    def list_insight_events(
        self,
        filters: AiInsightsFeedFilters,
    ) -> tuple[List[Dict[str, Any]], int]:
        offset = (filters.page - 1) * filters.page_size
        rows, total_count = self.client.select(
            table="ai_insight_events",
            select=(
                "id,insight_type,domain,severity,status,entity_type,entity_id,title,summary,"
                "recommended_action,priority,confidence,evidence,generated_at,model_name,model_tier,"
                "tokens_used,latency_ms,run_id,created_at,updated_at"
            ),
            filters=self._build_insight_filters(
                domain=filters.domain,
                insight_type=filters.insight_type,
                severity=filters.severity,
                status=filters.status,
                entity_type=filters.entity_type,
                entity_id=filters.entity_id,
            ),
            limit=filters.page_size,
            offset=offset,
            order="created_at.desc",
            count="exact" if filters.include_totals else "planned",
        )
        return rows, total_count or 0

    def list_insight_history(
        self,
        filters: AiInsightsHistoryFilters,
    ) -> tuple[List[Dict[str, Any]], int]:
        offset = (filters.page - 1) * filters.page_size
        rows, total_count = self.client.select(
            table="ai_insight_events",
            select=(
                "id,insight_type,domain,severity,status,entity_type,entity_id,title,summary,"
                "recommended_action,priority,confidence,evidence,generated_at,model_name,model_tier,"
                "tokens_used,latency_ms,run_id,created_at,updated_at"
            ),
            filters=self._build_history_filters(filters),
            limit=filters.page_size,
            offset=offset,
            order="created_at.desc",
            count="exact" if filters.include_totals else "planned",
        )
        return rows, total_count or 0

    def list_recommendations(
        self,
        filters: AiRecommendationFilters,
    ) -> tuple[List[Dict[str, Any]], int]:
        offset = (filters.page - 1) * filters.page_size
        rows, total_count = self.client.select(
            table="ai_recommendation_queue",
            select=(
                "id,insight_event_id,domain,status,entity_type,entity_id,title,summary,recommended_action,"
                "priority,confidence,owner_user_id,due_date,resolution_note,evidence,generated_at,model_name,"
                "model_tier,tokens_used,latency_ms,run_id,completed_at,created_at,updated_at"
            ),
            filters=self._build_recommendation_filters(filters),
            limit=filters.page_size,
            offset=offset,
            order="priority.asc,created_at.desc",
            count="exact" if filters.include_totals else "planned",
        )
        return rows, total_count or 0

    def get_latest_briefing(self, briefing_date: Optional[date] = None) -> Optional[Dict[str, Any]]:
        filters: List[Tuple[str, str]] = []
        if briefing_date:
            filters.append(("briefing_date", f"eq.{briefing_date.isoformat()}"))
        rows, _ = self.client.select(
            table="ai_briefings_daily",
            select=(
                "id,briefing_date,title,summary,highlights,top_actions,confidence,evidence,generated_at,"
                "model_name,model_tier,tokens_used,latency_ms,run_id,updated_at"
            ),
            filters=filters,
            limit=1,
            order="briefing_date.desc",
        )
        return rows[0] if rows else None

    def list_entity_insights(
        self, entity_type: str, entity_id: str, limit: int = 25
    ) -> List[Dict[str, Any]]:
        rows, _ = self.client.select(
            table="ai_insight_events",
            select=(
                "id,insight_type,domain,severity,status,entity_type,entity_id,title,summary,"
                "recommended_action,priority,confidence,evidence,generated_at,model_name,model_tier,"
                "tokens_used,latency_ms,run_id,created_at,updated_at"
            ),
            filters=[
                ("entity_type", f"eq.{entity_type}"),
                ("entity_id", f"eq.{entity_id}"),
            ],
            limit=limit,
            order="created_at.desc",
        )
        return rows

    def update_recommendation(
        self,
        recommendation_id: str,
        payload: Dict[str, Any],
    ) -> Optional[Dict[str, Any]]:
        rows = self.client.update(
            table="ai_recommendation_queue",
            payload=payload,
            filters=[("id", f"eq.{recommendation_id}")],
        )
        return rows[0] if rows else None

    def get_recommendation_by_id(self, recommendation_id: str) -> Optional[Dict[str, Any]]:
        rows, _ = self.client.select(
            table="ai_recommendation_queue",
            select=(
                "id,insight_event_id,domain,status,entity_type,entity_id,title,summary,recommended_action,"
                "priority,confidence,owner_user_id,due_date,resolution_note,evidence,generated_at,model_name,"
                "model_tier,tokens_used,latency_ms,run_id,completed_at,created_at,updated_at"
            ),
            filters=[("id", f"eq.{recommendation_id}")],
            limit=1,
        )
        return rows[0] if rows else None

    def insert_insight_events(self, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        if not rows:
            return []
        return self.client.insert(table="ai_insight_events", payload=rows)

    def insert_recommendations(self, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        if not rows:
            return []
        return self.client.insert(table="ai_recommendation_queue", payload=rows)

    def upsert_daily_briefing(self, row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        rows = self.client.insert(
            table="ai_briefings_daily",
            payload=row,
            upsert=True,
            on_conflict="briefing_date",
        )
        return rows[0] if rows else None

    def list_command_center_context(self) -> List[Dict[str, Any]]:
        rows, _ = self.client.select(
            table="ai_context_command_center_v1",
            select="*",
            limit=1,
            order="as_of_date.desc",
        )
        return rows

    def list_travel_consultant_context(self, limit: int) -> List[Dict[str, Any]]:
        rows, _ = self.client.select(
            table="ai_context_travel_consultant_v1",
            select="*",
            limit=min(limit, MAX_CONTEXT_ROWS),
            order="as_of_period_start.desc",
        )
        return rows

    def list_consultant_benchmarks_context(self) -> List[Dict[str, Any]]:
        rows, _ = self.client.select(
            table="ai_context_consultant_benchmarks_v1",
            select="*",
            limit=50,
            order="period_type.asc",
        )
        return rows

    def list_company_metrics_context(self) -> List[Dict[str, Any]]:
        rows, _ = self.client.select(
            table="ai_context_company_metrics_v1",
            select="*",
            limit=50,
            order="period_type.asc",
        )
        return rows

    def list_existing_employee_ids(self, employee_ids: List[str]) -> set[str]:
        normalized_ids = sorted({employee_id for employee_id in employee_ids if employee_id})
        if not normalized_ids:
            return set()
        existing_ids: set[str] = set()
        chunk_size = 100
        for start in range(0, len(normalized_ids), chunk_size):
            chunk = normalized_ids[start : start + chunk_size]
            in_filter = ",".join(chunk)
            rows, _ = self.client.select(
                table="employees",
                select="id",
                filters=[("id", f"in.({in_filter})")],
                limit=len(chunk),
            )
            for row in rows:
                employee_id = row.get("id")
                if employee_id:
                    existing_ids.add(str(employee_id))
        return existing_ids

    def list_itinerary_health_context(self, limit: int) -> List[Dict[str, Any]]:
        rows, _ = self.client.select(
            table="ai_context_itinerary_health_v1",
            select="*",
            limit=min(limit, MAX_CONTEXT_ROWS),
            order="period_start.desc",
        )
        return rows

    @staticmethod
    def parse_evidence(raw_value: Any) -> Dict[str, Any]:
        if raw_value is None:
            return {}
        if isinstance(raw_value, dict):
            return raw_value
        if isinstance(raw_value, str):
            if not raw_value.strip():
                return {}
            try:
                decoded = json.loads(raw_value)
                return decoded if isinstance(decoded, dict) else {}
            except json.JSONDecodeError:
                return {}
        return {}

    @staticmethod
    def _build_insight_filters(
        domain: Optional[str],
        insight_type: Optional[str],
        severity: Optional[str],
        status: Optional[str],
        entity_type: Optional[str],
        entity_id: Optional[str],
    ) -> List[Tuple[str, str]]:
        filters: List[Tuple[str, str]] = []
        if domain:
            filters.append(("domain", f"eq.{domain}"))
        if insight_type:
            filters.append(("insight_type", f"eq.{insight_type}"))
        if severity:
            filters.append(("severity", f"eq.{severity}"))
        if status:
            filters.append(("status", f"eq.{status}"))
        if entity_type:
            filters.append(("entity_type", f"eq.{entity_type}"))
        if entity_id:
            filters.append(("entity_id", f"eq.{entity_id}"))
        return filters

    def _build_history_filters(self, filters: AiInsightsHistoryFilters) -> List[Tuple[str, str]]:
        query_filters = self._build_insight_filters(
            domain=filters.domain,
            insight_type=filters.insight_type,
            severity=None,
            status=filters.status,
            entity_type=None,
            entity_id=None,
        )
        if filters.date_from:
            query_filters.append(("created_at", f"gte.{filters.date_from.isoformat()}T00:00:00"))
        if filters.date_to:
            query_filters.append(("created_at", f"lte.{filters.date_to.isoformat()}T23:59:59"))
        return query_filters

    @staticmethod
    def _build_recommendation_filters(
        filters: AiRecommendationFilters,
    ) -> List[Tuple[str, str]]:
        query_filters: List[Tuple[str, str]] = []
        if filters.domain:
            query_filters.append(("domain", f"eq.{filters.domain}"))
        if filters.status:
            query_filters.append(("status", f"eq.{filters.status}"))
        if filters.owner_user_id:
            query_filters.append(("owner_user_id", f"eq.{filters.owner_user_id}"))
        if filters.entity_type:
            query_filters.append(("entity_type", f"eq.{filters.entity_type}"))
        if filters.entity_id:
            query_filters.append(("entity_id", f"eq.{filters.entity_id}"))
        if filters.priority_min is not None:
            query_filters.append(("priority", f"gte.{filters.priority_min}"))
        if filters.priority_max is not None:
            query_filters.append(("priority", f"lte.{filters.priority_max}"))
        return query_filters

    @staticmethod
    def utc_now() -> datetime:
        return datetime.utcnow()

