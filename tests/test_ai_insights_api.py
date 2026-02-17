from __future__ import annotations

from datetime import date, datetime
import os
from typing import Any, Dict

import pytest
from fastapi.testclient import TestClient

from src.api.dependencies import get_ai_insights_service
from src.core.errors import BadRequestError, NotFoundError
from src.core.config import get_settings
from src.main import create_app
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


class FakeAiInsightsService:
    def __init__(self) -> None:
        evidence = AiInsightEvidence(
            summary="Fixture evidence",
            metrics=[
                AiInsightEvidenceMetric(
                    key="conversion_rate",
                    label="Conversion",
                    current_value=0.28,
                    baseline_value=0.35,
                    delta_pct=-0.07,
                    unit="ratio",
                )
            ],
            source_view_names=["ai_context_travel_consultant_v1"],
            reference_period="2026-02-01",
        )
        self.insight = AiInsightEvent(
            id="insight-1",
            insight_type="coaching_signal",
            domain="travel_consultant",
            severity="high",
            status="new",
            entity_type="employee",
            entity_id="employee-1",
            title="Coach consultant",
            summary="Conversion below baseline.",
            recommended_action="Review open pipeline and qualification.",
            priority=2,
            confidence=0.82,
            evidence=evidence,
            generated_at=datetime(2026, 2, 16, 14, 0, 0),
            model_name="gpt-5",
            model_tier="decision",
            tokens_used=120,
            latency_ms=800,
            run_id="run-1",
            created_at=datetime(2026, 2, 16, 14, 0, 0),
            updated_at=datetime(2026, 2, 16, 14, 0, 0),
        )
        self.recommendation = AiRecommendationItem(
            id="rec-1",
            insight_event_id="insight-1",
            domain="travel_consultant",
            status="new",
            entity_type="employee",
            entity_id="employee-1",
            title="Coach consultant",
            summary="Conversion below baseline.",
            recommended_action="Review open pipeline and qualification.",
            priority=2,
            confidence=0.82,
            owner_user_id=None,
            due_date=None,
            resolution_note=None,
            evidence=evidence,
            generated_at=datetime(2026, 2, 16, 14, 0, 0),
            completed_at=None,
            updated_at=datetime(2026, 2, 16, 14, 0, 0),
        )

    def get_briefing(self, briefing_date: date | None = None) -> AiBriefingDaily:
        _ = briefing_date
        return AiBriefingDaily(
            id="brief-1",
            briefing_date=date(2026, 2, 16),
            title="Daily operating brief",
            summary="Metrics reviewed.",
            highlights=["Conversion soft in consultant funnel."],
            top_actions=["Prioritize advisor coaching."],
            confidence=0.84,
            evidence=self.insight.evidence,
            generated_at=datetime(2026, 2, 16, 14, 0, 0),
            model_name="gpt-5",
            model_tier="decision",
            tokens_used=220,
            latency_ms=900,
            run_id="run-1",
            updated_at=datetime(2026, 2, 16, 14, 0, 0),
        )

    def get_feed(self, filters: AiInsightsFeedFilters) -> tuple[AiInsightFeedResponse, Any]:
        _ = filters
        return AiInsightFeedResponse(items=[self.insight]), build_pagination(1, 25, 1)

    def get_history(self, filters: AiInsightsHistoryFilters) -> tuple[AiInsightHistoryResponse, Any]:
        _ = filters
        return AiInsightHistoryResponse(items=[self.insight]), build_pagination(1, 50, 1)

    def get_recommendations(
        self, filters: AiRecommendationFilters
    ) -> tuple[AiRecommendationQueueResponse, Any]:
        _ = filters
        return AiRecommendationQueueResponse(items=[self.recommendation]), build_pagination(1, 25, 1)

    def update_recommendation(
        self, recommendation_id: str, request: AiRecommendationUpdateRequest
    ) -> AiRecommendationItem:
        if recommendation_id != self.recommendation.id:
            raise NotFoundError("Recommendation not found")
        if request.status not in {"acknowledged", "in_progress", "resolved", "dismissed"}:
            raise BadRequestError("Invalid status")
        self.recommendation.status = request.status
        self.recommendation.updated_at = datetime(2026, 2, 16, 15, 0, 0)
        return self.recommendation

    def get_entity_insights(self, entity_type: str, entity_id: str) -> AiEntityInsightsResponse:
        return AiEntityInsightsResponse(entity_type=entity_type, entity_id=entity_id, items=[self.insight])

    def run_manual_generation(self, trigger: str = "manual") -> Dict[str, Any]:
        return {
            "runId": "run-1",
            "trigger": trigger,
            "status": "completed",
            "createdEvents": 1,
            "createdRecommendations": 1,
        }


@pytest.fixture()
def client() -> TestClient:
    os.environ["AI_MANUAL_RUN_TOKEN"] = "test-token"
    get_settings.cache_clear()
    app = create_app()
    fake_service = FakeAiInsightsService()
    app.dependency_overrides[get_ai_insights_service] = lambda: fake_service
    try:
        yield TestClient(app)
    finally:
        app.dependency_overrides.clear()
        os.environ.pop("AI_MANUAL_RUN_TOKEN", None)
        get_settings.cache_clear()


def test_ai_briefing(client: TestClient) -> None:
    response = client.get("/api/v1/ai-insights/briefing")
    assert response.status_code == 200
    body = response.json()
    assert body["data"]["title"] == "Daily operating brief"
    assert body["meta"]["source"] == "ai_briefings_daily"


def test_ai_feed(client: TestClient) -> None:
    response = client.get("/api/v1/ai-insights/feed?domain=travel_consultant&status=new")
    assert response.status_code == 200
    body = response.json()
    assert len(body["data"]["items"]) == 1
    assert body["data"]["items"][0]["domain"] == "travel_consultant"
    assert body["pagination"]["totalItems"] == 1


def test_ai_recommendation_transition(client: TestClient) -> None:
    response = client.patch(
        "/api/v1/ai-insights/recommendations/rec-1",
        json={"status": "acknowledged"},
    )
    assert response.status_code == 200
    body = response.json()
    assert body["data"]["status"] == "acknowledged"


def test_ai_entity_insights(client: TestClient) -> None:
    response = client.get("/api/v1/ai-insights/entities/employee/employee-1")
    assert response.status_code == 200
    body = response.json()
    assert body["data"]["entityType"] == "employee"
    assert body["data"]["entityId"] == "employee-1"


def test_ai_manual_run(client: TestClient) -> None:
    response = client.post("/api/v1/ai-insights/run", headers={"x-ai-run-token": "test-token"})
    assert response.status_code == 200
    body = response.json()
    assert body["data"]["status"] == "completed"
    assert body["data"]["createdEvents"] == 1


def test_ai_manual_run_requires_token(client: TestClient) -> None:
    response = client.post("/api/v1/ai-insights/run")
    assert response.status_code == 400

