from __future__ import annotations

from datetime import date, datetime
from typing import Any, Dict, List

from src.core.errors import BadRequestError, NotFoundError
from src.repositories.ai_insights_repository import AiInsightsRepository
from src.schemas.ai_insights import (
    AiBriefingDaily,
    AiEntityInsightsResponse,
    AiInsightEvidence,
    AiInsightEvidenceMetric,
    AiInsightEvent,
    AiInsightFeedResponse,
    AiInsightHistoryResponse,
    AiInsightsFeedFilters,
    AiInsightsHistoryFilters,
    AiRecommendationFilters,
    AiRecommendationItem,
    AiRecommendationQueueResponse,
    AiRecommendationUpdateRequest,
)
from src.shared.response import build_pagination

TRANSITION_MAP: Dict[str, set[str]] = {
    "new": {"acknowledged", "dismissed"},
    "acknowledged": {"in_progress", "dismissed"},
    "in_progress": {"resolved", "dismissed"},
    "resolved": set(),
    "dismissed": set(),
}


class AiInsightsService:
    def __init__(self, repository: AiInsightsRepository) -> None:
        self.repository = repository

    def get_briefing(self, briefing_date: date | None = None) -> AiBriefingDaily:
        row = self.repository.get_latest_briefing(briefing_date=briefing_date)
        if not row:
            raise NotFoundError("No AI briefing available")
        return self._to_briefing(row)

    def get_feed(self, filters: AiInsightsFeedFilters) -> tuple[AiInsightFeedResponse, Any]:
        rows, total_count = self.repository.list_insight_events(filters)
        items = [self._to_insight_event(row) for row in rows]
        pagination = build_pagination(filters.page, filters.page_size, total_count)
        return AiInsightFeedResponse(items=items), pagination

    def get_history(self, filters: AiInsightsHistoryFilters) -> tuple[AiInsightHistoryResponse, Any]:
        rows, total_count = self.repository.list_insight_history(filters)
        items = [self._to_insight_event(row) for row in rows]
        pagination = build_pagination(filters.page, filters.page_size, total_count)
        return AiInsightHistoryResponse(items=items), pagination

    def get_recommendations(
        self, filters: AiRecommendationFilters
    ) -> tuple[AiRecommendationQueueResponse, Any]:
        rows, total_count = self.repository.list_recommendations(filters)
        items = [self._to_recommendation(row) for row in rows]
        pagination = build_pagination(filters.page, filters.page_size, total_count)
        return AiRecommendationQueueResponse(items=items), pagination

    def update_recommendation(
        self, recommendation_id: str, request: AiRecommendationUpdateRequest
    ) -> AiRecommendationItem:
        existing = self.repository.get_recommendation_by_id(recommendation_id)
        if not existing:
            raise NotFoundError("Recommendation not found")

        current_status = str(existing.get("status") or "new")
        if request.status != current_status and request.status not in TRANSITION_MAP.get(current_status, set()):
            raise BadRequestError(
                f"Invalid recommendation status transition: {current_status} -> {request.status}"
            )

        payload: Dict[str, Any] = {
            "status": request.status,
            "updated_at": datetime.utcnow().isoformat(),
        }
        if request.owner_user_id is not None:
            payload["owner_user_id"] = request.owner_user_id
        if request.resolution_note is not None:
            payload["resolution_note"] = request.resolution_note
        if request.status in {"resolved", "dismissed"}:
            payload["completed_at"] = datetime.utcnow().isoformat()
        updated = self.repository.update_recommendation(recommendation_id, payload)
        if not updated:
            raise NotFoundError("Recommendation not found")
        return self._to_recommendation(updated)

    def get_entity_insights(self, entity_type: str, entity_id: str) -> AiEntityInsightsResponse:
        rows = self.repository.list_entity_insights(entity_type=entity_type, entity_id=entity_id)
        items = [self._to_insight_event(row) for row in rows]
        return AiEntityInsightsResponse(entity_type=entity_type, entity_id=entity_id, items=items)

    def run_manual_generation(self, trigger: str = "manual") -> Dict[str, Any]:
        from src.services.ai_orchestration_service import AiOrchestrationService
        from src.services.openai_insights_service import OpenAiInsightsService

        orchestration_service = AiOrchestrationService(
            repository=self.repository,
            openai_service=OpenAiInsightsService(),
        )
        return orchestration_service.generate_insights(trigger=trigger)

    def _to_insight_event(self, row: Dict[str, Any]) -> AiInsightEvent:
        evidence = self._parse_evidence(row.get("evidence"))
        return AiInsightEvent(
            id=str(row.get("id")),
            insight_type=str(row.get("insight_type")),
            domain=str(row.get("domain")),
            severity=str(row.get("severity")),
            status=str(row.get("status")),
            entity_type=row.get("entity_type"),
            entity_id=row.get("entity_id"),
            title=str(row.get("title") or ""),
            summary=str(row.get("summary") or ""),
            recommended_action=row.get("recommended_action"),
            priority=int(row.get("priority") or 3),
            confidence=float(row.get("confidence") or 0),
            evidence=evidence,
            generated_at=self._parse_datetime(row.get("generated_at")),
            model_name=row.get("model_name"),
            model_tier=row.get("model_tier"),
            tokens_used=self._optional_int(row.get("tokens_used")),
            latency_ms=self._optional_int(row.get("latency_ms")),
            run_id=row.get("run_id"),
            created_at=self._parse_datetime(row.get("created_at")),
            updated_at=self._parse_datetime(row.get("updated_at")),
        )

    def _to_recommendation(self, row: Dict[str, Any]) -> AiRecommendationItem:
        evidence = self._parse_evidence(row.get("evidence"))
        return AiRecommendationItem(
            id=str(row.get("id")),
            insight_event_id=str(row.get("insight_event_id")) if row.get("insight_event_id") else None,
            domain=str(row.get("domain")),
            status=str(row.get("status")),
            entity_type=row.get("entity_type"),
            entity_id=row.get("entity_id"),
            title=str(row.get("title") or ""),
            summary=str(row.get("summary") or ""),
            recommended_action=str(row.get("recommended_action") or ""),
            priority=int(row.get("priority") or 3),
            confidence=float(row.get("confidence") or 0),
            owner_user_id=str(row.get("owner_user_id")) if row.get("owner_user_id") else None,
            due_date=self._parse_date(row.get("due_date")),
            resolution_note=row.get("resolution_note"),
            evidence=evidence,
            generated_at=self._parse_datetime(row.get("generated_at")),
            completed_at=self._parse_datetime_nullable(row.get("completed_at")),
            updated_at=self._parse_datetime(row.get("updated_at")),
        )

    def _to_briefing(self, row: Dict[str, Any]) -> AiBriefingDaily:
        evidence = self._parse_evidence(row.get("evidence"))
        highlights_raw = row.get("highlights") if isinstance(row.get("highlights"), list) else []
        top_actions_raw = row.get("top_actions") if isinstance(row.get("top_actions"), list) else []
        highlights = [self._to_briefing_list_item(item, item_kind="highlight") for item in highlights_raw]
        top_actions = [self._to_briefing_list_item(item, item_kind="action") for item in top_actions_raw]
        highlights = [item for item in highlights if item][:6]
        top_actions = [item for item in top_actions if item][:6]
        return AiBriefingDaily(
            id=str(row.get("id")),
            briefing_date=self._parse_date(row.get("briefing_date")) or date.today(),
            title=str(row.get("title") or ""),
            summary=str(row.get("summary") or ""),
            highlights=highlights,
            top_actions=top_actions,
            confidence=float(row.get("confidence") or 0),
            evidence=evidence,
            generated_at=self._parse_datetime(row.get("generated_at")),
            model_name=row.get("model_name"),
            model_tier=row.get("model_tier"),
            tokens_used=self._optional_int(row.get("tokens_used")),
            latency_ms=self._optional_int(row.get("latency_ms")),
            run_id=row.get("run_id"),
            updated_at=self._parse_datetime(row.get("updated_at")),
        )

    def _parse_evidence(self, raw_value: Any) -> AiInsightEvidence:
        evidence_dict = self.repository.parse_evidence(raw_value)
        metric_rows = evidence_dict.get("metrics", [])
        metrics: List[AiInsightEvidenceMetric] = []
        if isinstance(metric_rows, list):
            for row in metric_rows:
                if not isinstance(row, dict):
                    continue
                metrics.append(
                    AiInsightEvidenceMetric(
                        key=str(row.get("key") or ""),
                        label=str(row.get("label") or ""),
                        current_value=float(row.get("currentValue") or row.get("current_value") or 0),
                        baseline_value=self._optional_float(
                            row.get("baselineValue") if "baselineValue" in row else row.get("baseline_value")
                        ),
                        delta_pct=self._optional_float(
                            row.get("deltaPct") if "deltaPct" in row else row.get("delta_pct")
                        ),
                        unit=row.get("unit"),
                    )
                )
        source_view_names = evidence_dict.get("sourceViewNames")
        if not isinstance(source_view_names, list):
            source_view_names = evidence_dict.get("source_view_names", [])
        return AiInsightEvidence(
            summary=evidence_dict.get("summary"),
            metrics=metrics,
            source_view_names=[str(item) for item in source_view_names] if isinstance(source_view_names, list) else [],
            reference_period=(
                evidence_dict.get("referencePeriod")
                if "referencePeriod" in evidence_dict
                else evidence_dict.get("reference_period")
            ),
        )

    @staticmethod
    def _parse_datetime(raw_value: Any) -> datetime:
        if isinstance(raw_value, datetime):
            return raw_value
        if isinstance(raw_value, str):
            normalized = raw_value.replace("Z", "+00:00")
            return datetime.fromisoformat(normalized)
        return datetime.utcnow()

    @staticmethod
    def _parse_datetime_nullable(raw_value: Any) -> datetime | None:
        if raw_value is None:
            return None
        return AiInsightsService._parse_datetime(raw_value)

    @staticmethod
    def _parse_date(raw_value: Any) -> date | None:
        if raw_value is None:
            return None
        if isinstance(raw_value, date):
            return raw_value
        if isinstance(raw_value, str):
            return date.fromisoformat(raw_value[:10])
        return None

    @staticmethod
    def _optional_float(value: Any) -> float | None:
        if value is None:
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _optional_int(value: Any) -> int | None:
        if value is None:
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _to_briefing_list_item(raw_item: Any, *, item_kind: str) -> str:
        if isinstance(raw_item, str):
            compact = " ".join(raw_item.replace("\n", " ").split()).strip()
            return AiInsightsService._clip_briefing_text(compact, 170) if compact else ""
        if isinstance(raw_item, dict):
            note = str(raw_item.get("note") or "").strip()
            metric = str(raw_item.get("metric") or "").strip()
            value = raw_item.get("value")
            formatted_value = AiInsightsService._format_briefing_metric_value(value, metric)
            action = str(raw_item.get("action") or "").strip()
            next_steps = raw_item.get("nextSteps")
            why = str(raw_item.get("why") or "").strip()

            if item_kind == "highlight":
                if note and metric:
                    if formatted_value:
                        return AiInsightsService._clip_briefing_text(
                            f"{note} ({metric}: {formatted_value})", 170
                        )
                    return AiInsightsService._clip_briefing_text(f"{note} ({metric})", 170)
                if note:
                    return AiInsightsService._clip_briefing_text(note, 170)
                if metric:
                    return AiInsightsService._clip_briefing_text(metric, 170)
            if action:
                if isinstance(next_steps, list) and next_steps:
                    first_step = str(next_steps[0]).strip()
                    if first_step:
                        return AiInsightsService._clip_briefing_text(
                            f"{action} Next: {first_step}", 170
                        )
                return AiInsightsService._clip_briefing_text(action, 170)
            if why:
                return AiInsightsService._clip_briefing_text(why, 170)
        return ""

    @staticmethod
    def _format_briefing_metric_value(value: Any, metric: str) -> str:
        if value is None:
            return ""
        try:
            numeric = float(value)
        except (TypeError, ValueError):
            return str(value).strip()[:80]
        metric_lower = metric.lower()
        if 0 <= numeric <= 1.5 and (
            "rate" in metric_lower or "ratio" in metric_lower or "margin" in metric_lower
        ):
            return f"{round(numeric * 100, 1)}%"
        if abs(numeric) >= 1000:
            return f"{numeric:,.0f}"
        return f"{round(numeric, 2)}"

    @staticmethod
    def _clip_briefing_text(text: str, max_len: int) -> str:
        compact = " ".join(text.split()).strip()
        if len(compact) <= max_len:
            return compact
        clipped = compact[:max_len].rstrip()
        if " " in clipped:
            clipped = clipped.rsplit(" ", 1)[0]
        return f"{clipped}..."

