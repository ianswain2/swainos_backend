from __future__ import annotations

from typing import Any, Dict, List, Tuple

from src.core.supabase import SupabaseClient


class TravelTradeSearchRepository:
    def __init__(self) -> None:
        self.client = SupabaseClient()

    def search(
        self,
        query: str,
        entity_type: str,
        limit: int,
    ) -> List[Dict[str, Any]]:
        normalized_query = " ".join(query.strip().split())
        if not normalized_query:
            return []
        escaped_ilike = (
            normalized_query.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
        )
        filters: List[Tuple[str, str]] = [("search_text", f"ilike.*{escaped_ilike}*")]
        if entity_type in {"agent", "agency"}:
            filters.append(("entity_type", f"eq.{entity_type}"))
        ilike_rows, _ = self.client.select(
            table="travel_trade_search_index",
            select=(
                "entity_type,entity_id,entity_external_id,display_name,email,agency_name,iata_code,"
                "host_identifier,rank_score"
            ),
            filters=filters,
            limit=limit,
            order="rank_score.desc",
        )
        fts_filters: List[Tuple[str, str]] = [("search_text", f"wfts.{normalized_query}")]
        if entity_type in {"agent", "agency"}:
            fts_filters.append(("entity_type", f"eq.{entity_type}"))
        fts_rows, _ = self.client.select(
            table="travel_trade_search_index",
            select=(
                "entity_type,entity_id,entity_external_id,display_name,email,agency_name,iata_code,"
                "host_identifier,rank_score"
            ),
            filters=fts_filters,
            limit=limit,
            order="rank_score.desc",
        )
        merged: Dict[tuple[str, str], Dict[str, Any]] = {}
        for row in [*fts_rows, *ilike_rows]:
            key = (str(row.get("entity_type") or ""), str(row.get("entity_id") or ""))
            if key not in merged:
                merged[key] = row
        rows = list(merged.values())
        rows.sort(
            key=lambda row: (
                -float(row.get("rank_score") or 0),
                str(row.get("display_name") or "").lower(),
            )
        )
        return rows[:limit]
