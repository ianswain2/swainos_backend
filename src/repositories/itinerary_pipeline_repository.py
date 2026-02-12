from __future__ import annotations

from datetime import date
from typing import List

from src.core.supabase import SupabaseClient
from src.schemas.itinerary_pipeline import ItineraryPipelineStageTimelineItem


class ItineraryPipelineRepository:
    def __init__(self) -> None:
        self.client = SupabaseClient()

    def list_stage_trends(self, start_date: date, end_date: date) -> List[ItineraryPipelineStageTimelineItem]:
        rows, _ = self.client.select(
            table="mv_itinerary_pipeline_stages",
            select="period_start,period_end,stage,itinerary_count,gross_amount,net_amount,pax_count",
            filters=[
                ("period_start", f"gte.{start_date.isoformat()}"),
                ("period_start", f"lte.{end_date.isoformat()}"),
            ],
            order="period_start.asc",
        )
        return [ItineraryPipelineStageTimelineItem.model_validate(row) for row in rows]
