from __future__ import annotations

from datetime import date

from src.shared.base import BaseSchema

class ItineraryPipelineStageTimelineItem(BaseSchema):
    period_start: date
    period_end: date
    stage: str
    itinerary_count: int
    gross_amount: float
    net_amount: float
    pax_count: int
