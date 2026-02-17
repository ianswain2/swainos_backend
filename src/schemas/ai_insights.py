from __future__ import annotations

from datetime import date, datetime
from typing import List, Literal, Optional

from pydantic import ConfigDict, Field

from src.shared.base import BaseSchema

InsightDomain = Literal[
    "command_center",
    "travel_consultant",
    "itinerary",
    "fx",
    "destination",
    "invoices",
    "platform",
]
InsightType = Literal[
    "briefing",
    "anomaly",
    "recommendation",
    "forecast_narrative",
    "coaching_signal",
]
InsightSeverity = Literal["low", "medium", "high", "critical"]
InsightStatus = Literal["new", "acknowledged", "in_progress", "resolved", "dismissed"]
ModelTier = Literal["decision", "support", "fallback"]


class AiInsightsFeedFilters(BaseSchema):
    model_config = ConfigDict(populate_by_name=True, extra="forbid")

    domain: Optional[InsightDomain] = None
    insight_type: Optional[InsightType] = None
    severity: Optional[InsightSeverity] = None
    status: Optional[InsightStatus] = None
    entity_type: Optional[str] = Field(default=None, min_length=1, max_length=64)
    entity_id: Optional[str] = Field(default=None, min_length=1, max_length=128)
    page: int = Field(default=1, ge=1)
    page_size: int = Field(default=25, ge=1, le=100)


class AiRecommendationFilters(BaseSchema):
    model_config = ConfigDict(populate_by_name=True, extra="forbid")

    domain: Optional[InsightDomain] = None
    status: Optional[InsightStatus] = None
    priority_min: Optional[int] = Field(default=None, ge=1, le=5)
    priority_max: Optional[int] = Field(default=None, ge=1, le=5)
    owner_user_id: Optional[str] = None
    entity_type: Optional[str] = Field(default=None, min_length=1, max_length=64)
    entity_id: Optional[str] = Field(default=None, min_length=1, max_length=128)
    page: int = Field(default=1, ge=1)
    page_size: int = Field(default=25, ge=1, le=100)


class AiInsightsHistoryFilters(BaseSchema):
    model_config = ConfigDict(populate_by_name=True, extra="forbid")

    domain: Optional[InsightDomain] = None
    insight_type: Optional[InsightType] = None
    status: Optional[InsightStatus] = None
    date_from: Optional[date] = None
    date_to: Optional[date] = None
    page: int = Field(default=1, ge=1)
    page_size: int = Field(default=50, ge=1, le=200)


class AiRecommendationUpdateRequest(BaseSchema):
    model_config = ConfigDict(populate_by_name=True, extra="forbid")

    status: InsightStatus
    owner_user_id: Optional[str] = None
    resolution_note: Optional[str] = Field(default=None, max_length=2000)


class AiInsightEvidenceMetric(BaseSchema):
    key: str
    label: str
    current_value: float
    baseline_value: Optional[float] = None
    delta_pct: Optional[float] = None
    unit: Optional[str] = None


class AiInsightEvidence(BaseSchema):
    summary: Optional[str] = None
    metrics: List[AiInsightEvidenceMetric] = Field(default_factory=list)
    source_view_names: List[str] = Field(default_factory=list)
    reference_period: Optional[str] = None


class AiInsightEvent(BaseSchema):
    id: str
    insight_type: InsightType
    domain: InsightDomain
    severity: InsightSeverity
    status: InsightStatus
    entity_type: Optional[str] = None
    entity_id: Optional[str] = None
    title: str
    summary: str
    recommended_action: Optional[str] = None
    priority: int
    confidence: float
    evidence: AiInsightEvidence
    generated_at: datetime
    model_name: Optional[str] = None
    model_tier: Optional[ModelTier] = None
    tokens_used: Optional[int] = None
    latency_ms: Optional[int] = None
    run_id: Optional[str] = None
    created_at: datetime
    updated_at: datetime


class AiRecommendationItem(BaseSchema):
    id: str
    insight_event_id: Optional[str] = None
    domain: InsightDomain
    status: InsightStatus
    entity_type: Optional[str] = None
    entity_id: Optional[str] = None
    title: str
    summary: str
    recommended_action: str
    priority: int
    confidence: float
    owner_user_id: Optional[str] = None
    due_date: Optional[date] = None
    resolution_note: Optional[str] = None
    evidence: AiInsightEvidence
    generated_at: datetime
    completed_at: Optional[datetime] = None
    updated_at: datetime


class AiBriefingDaily(BaseSchema):
    id: str
    briefing_date: date
    title: str
    summary: str
    highlights: List[str] = Field(default_factory=list)
    top_actions: List[str] = Field(default_factory=list)
    confidence: float
    evidence: AiInsightEvidence
    generated_at: datetime
    model_name: Optional[str] = None
    model_tier: Optional[ModelTier] = None
    tokens_used: Optional[int] = None
    latency_ms: Optional[int] = None
    run_id: Optional[str] = None
    updated_at: datetime


class AiInsightFeedResponse(BaseSchema):
    items: List[AiInsightEvent]


class AiRecommendationQueueResponse(BaseSchema):
    items: List[AiRecommendationItem]


class AiInsightHistoryResponse(BaseSchema):
    items: List[AiInsightEvent]


class AiEntityInsightsResponse(BaseSchema):
    entity_type: str
    entity_id: str
    items: List[AiInsightEvent]

