from __future__ import annotations

from datetime import date
from typing import List

from src.shared.base import BaseSchema


class ItineraryPipelineFilters(BaseSchema):
    time_window: str = "6m"


class ItineraryPipelineStageSummaryItem(BaseSchema):
    stage: str
    itinerary_count: int
    gross_amount: float
    net_amount: float
    pax_count: int


class ItineraryPipelineStageTimelineItem(BaseSchema):
    period_start: date
    period_end: date
    stage: str
    itinerary_count: int
    gross_amount: float
    net_amount: float
    pax_count: int


class ItineraryPipelineForecastPoint(BaseSchema):
    period_start: date
    period_end: date
    projected_confirmed_count: float
    projected_gross_amount: float
    projected_net_amount: float
    projected_pax_count: float


class ItineraryPipelineMonthlyOutlookPoint(BaseSchema):
    period_start: date
    period_end: date
    on_books_gross_amount: float
    potential_gross_amount: float
    expected_gross_amount: float
    on_books_net_amount: float
    potential_net_amount: float
    expected_net_amount: float
    on_books_pax_count: int
    potential_pax_count: float
    expected_pax_count: float


class ItineraryPipelineResponse(BaseSchema):
    stage_summary: List[ItineraryPipelineStageSummaryItem]
    stage_timeline: List[ItineraryPipelineStageTimelineItem]
    close_ratio: float
    forecast_timeline: List[ItineraryPipelineForecastPoint]
    monthly_outlook: List[ItineraryPipelineMonthlyOutlookPoint]
