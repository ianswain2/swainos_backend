from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import Literal

from src.shared.base import BaseSchema

InsightPriority = Literal["high", "medium", "low"]
HealthStatus = Literal["connected", "healthy", "pending", "warning", "missing"]
InsightCategory = Literal[
    "acquisition",
    "conversion",
    "content",
    "geography",
    "device",
    "intent",
    "measurement",
]
InsightFocusArea = Literal["scale", "fix", "cut", "instrument", "localize", "optimize"]
InsightOwner = Literal["marketing", "sales", "web", "analytics"]


class MarketingKpi(BaseSchema):
    metric_key: str
    label: str
    format: Literal["integer", "percent", "currency", "ratio"]
    current_value: Decimal
    previous_value: Decimal
    year_ago_value: Decimal
    day_over_day_delta_pct: Decimal | None = None
    month_over_month_delta_pct: Decimal | None = None
    year_over_year_delta_pct: Decimal | None = None


class MarketingTimeSeriesPoint(BaseSchema):
    snapshot_date: date
    sessions: Decimal
    total_users: Decimal
    engaged_sessions: Decimal
    key_events: Decimal
    engagement_rate: Decimal


class MarketingLandingPagePerformance(BaseSchema):
    snapshot_date: date
    landing_page: str
    sessions: Decimal
    total_users: Decimal
    engagement_rate: Decimal
    key_events: Decimal
    avg_session_duration_seconds: Decimal | None = None


class MarketingChannelPerformance(BaseSchema):
    channel_name: str
    sessions: Decimal
    total_users: Decimal
    engagement_rate: Decimal
    key_events: Decimal


class MarketingTrackingEvent(BaseSchema):
    snapshot_date: date
    event_name: str
    event_count: Decimal
    total_users: Decimal
    event_value_amount: Decimal | None = None


class MarketingEventCatalogItem(BaseSchema):
    event_name: str
    event_count: Decimal
    total_users: Decimal
    event_value_amount: Decimal | None = None
    category: Literal["conversion", "engagement", "navigation", "system", "other"]
    description: str
    is_conversion_event: bool


class MarketingEventCatalog(BaseSchema):
    snapshot_date: date | None = None
    events: list[MarketingEventCatalogItem]


class MarketingPageActivityRow(BaseSchema):
    snapshot_date: date
    page_path: str
    page_title: str | None = None
    screen_page_views: Decimal
    sessions: Decimal
    total_users: Decimal
    engaged_sessions: Decimal
    key_events: Decimal
    engagement_rate: Decimal
    key_event_rate: Decimal
    avg_session_duration_seconds: Decimal | None = None
    quality_score: Decimal
    is_itinerary_page: bool


class MarketingPageActivity(BaseSchema):
    snapshot_date: date | None = None
    metric_guide: str
    best_pages: list[MarketingPageActivityRow]
    worst_pages: list[MarketingPageActivityRow]
    itinerary_pages: list[MarketingPageActivityRow]
    lookbook_pages: list[MarketingPageActivityRow]
    destination_pages: list[MarketingPageActivityRow]
    all_pages: list[MarketingPageActivityRow]


class MarketingGeoRow(BaseSchema):
    snapshot_date: date
    country: str
    region: str | None = None
    city: str | None = None
    sessions: Decimal
    total_users: Decimal
    engaged_sessions: Decimal
    key_events: Decimal
    engagement_rate: Decimal
    key_event_rate: Decimal


class MarketingGeoBreakdown(BaseSchema):
    snapshot_date: date | None = None
    rows: list[MarketingGeoRow]
    top_countries: list[MarketingGeoRow]
    demographics: list[MarketingDemographicRow]
    devices: list[MarketingDeviceRow]


class MarketingOverview(BaseSchema):
    kpis: list[MarketingKpi]
    trend: list[MarketingTimeSeriesPoint]
    top_landing_pages: list[MarketingLandingPagePerformance]
    channels: list[MarketingChannelPerformance]
    events: list[MarketingTrackingEvent]
    search_console_connected: bool
    currency: str
    timezone: str


class MarketingSearchQuery(BaseSchema):
    query: str
    clicks: Decimal
    impressions: Decimal
    ctr: Decimal
    average_position: Decimal


class MarketingSearchPerformance(BaseSchema):
    top_landing_pages: list[MarketingLandingPagePerformance]
    channels: list[MarketingChannelPerformance]
    source_mix: list[MarketingSourcePerformance]
    referral_sources: list[MarketingSourcePerformance]
    top_valuable_sources: list[MarketingSourcePerformance]
    internal_site_search_terms: list[MarketingInternalSiteSearchTerm]


class MarketingSourcePerformance(BaseSchema):
    source_label: str
    source: str
    medium: str
    channel_name: str
    sessions: Decimal
    total_users: Decimal
    engaged_sessions: Decimal
    key_events: Decimal
    engagement_rate: Decimal
    key_event_rate: Decimal
    bounce_rate: Decimal
    qualified_session_rate: Decimal
    quality_label: Literal["qualified", "mixed", "poor"]
    value_score: Decimal


class MarketingSearchConsoleInsights(BaseSchema):
    search_console_connected: bool
    connection_message: str
    data_mode: Literal["proxy", "live_gsc"]
    top_queries: list[MarketingSearchQuery]
    organic_landing_pages: list[MarketingLandingPagePerformance]
    internal_site_search_terms: list[MarketingInternalSiteSearchTerm]


class MarketingDemographicRow(BaseSchema):
    snapshot_date: date
    age_bracket: str
    gender: str
    sessions: Decimal
    total_users: Decimal
    engaged_sessions: Decimal
    key_events: Decimal
    engagement_rate: Decimal


class MarketingDeviceRow(BaseSchema):
    snapshot_date: date
    device_category: str
    sessions: Decimal
    total_users: Decimal
    engaged_sessions: Decimal
    key_events: Decimal
    engagement_rate: Decimal


class MarketingInternalSiteSearchTerm(BaseSchema):
    search_term: str
    event_count: Decimal
    total_users: Decimal


class MarketingAiInsight(BaseSchema):
    insight_id: str
    priority: InsightPriority
    category: InsightCategory
    focus_area: InsightFocusArea
    title: str
    summary: str
    target_label: str
    target_path: str | None = None
    owner_hint: InsightOwner
    primary_metric_label: str
    impact_score: Decimal
    confidence_score: Decimal
    evidence_points: list[str]
    recommended_actions: list[str]


class MarketingHealthStatus(BaseSchema):
    key: str
    label: str
    status: HealthStatus
    detail: str


class MarketingHealth(BaseSchema):
    statuses: list[MarketingHealthStatus]
    last_synced_at: str | None = None
    latest_run_status: str


class MarketingWebAnalyticsSyncResult(BaseSchema):
    run_id: str
    status: str
    records_processed: int
    records_created: int
    message: str
