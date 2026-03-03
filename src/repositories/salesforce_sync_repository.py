from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, Optional

from src.core.supabase import SupabaseClient


class SalesforceSyncRepository:
    def __init__(self) -> None:
        self.client = SupabaseClient()

    def get_cursor(self, object_name: str) -> Dict[str, Optional[str]]:
        rows, _ = self.client.select(
            table="salesforce_sync_cursors",
            select="object_name,last_systemmodstamp,last_id,updated_at",
            filters=[("object_name", f"eq.{object_name}")],
            limit=1,
        )
        if not rows:
            return {"last_systemmodstamp": None, "last_id": None}
        row = rows[0]
        return {
            "last_systemmodstamp": row.get("last_systemmodstamp"),
            "last_id": row.get("last_id"),
        }

    def upsert_cursor(
        self,
        object_name: str,
        last_systemmodstamp: Optional[str],
        last_id: Optional[str],
    ) -> None:
        payload = {
            "object_name": object_name,
            "last_systemmodstamp": last_systemmodstamp,
            "last_id": last_id,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        self.client.insert(
            table="salesforce_sync_cursors",
            payload=payload,
            upsert=True,
            on_conflict="object_name",
        )

    def create_run(self, payload: Dict[str, Any]) -> None:
        self.client.insert(table="salesforce_sync_runs", payload=payload, upsert=False)

    def finalize_run(
        self,
        run_id: str,
        status: str,
        finished_at: str,
        error_message: Optional[str],
        object_metrics: Dict[str, Any],
        counters: Dict[str, int],
    ) -> None:
        update_payload: Dict[str, Any] = {
            "status": status,
            "finished_at": finished_at,
            "error_message": error_message,
            "object_metrics": object_metrics,
            "jobs_created": counters.get("jobs_created", 0),
            "polls_made": counters.get("polls_made", 0),
            "result_pages_read": counters.get("result_pages_read", 0),
        }
        self.client.update(
            table="salesforce_sync_runs",
            payload=update_payload,
            filters=[("run_id", f"eq.{run_id}")],
        )

