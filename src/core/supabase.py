from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlencode

import httpx

from src.core.config import get_settings


class SupabaseClient:
    def __init__(self) -> None:
        settings = get_settings()
        self.base_url = settings.supabase_url.rstrip("/") + "/rest/v1"
        self.api_key = settings.supabase_service_role_key or settings.supabase_anon_key
        if not self.api_key:
            raise ValueError("Supabase API key is required")

    def select(
        self,
        table: str,
        select: str,
        filters: Optional[List[Tuple[str, str]]] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        order: Optional[str] = None,
        count: bool = False,
    ) -> Tuple[List[Dict[str, Any]], Optional[int]]:
        params: List[Tuple[str, str]] = [("select", select)]
        if filters:
            params.extend(filters)
        if limit is not None:
            params.append(("limit", str(limit)))
        if offset is not None:
            params.append(("offset", str(offset)))
        if order:
            params.append(("order", order))

        headers = {
            "apikey": self.api_key,
            "Authorization": f"Bearer {self.api_key}",
        }
        if count:
            headers["Prefer"] = "count=exact"

        url = f"{self.base_url}/{table}?{urlencode(params, doseq=True)}"
        response = httpx.get(url, headers=headers, timeout=30.0)
        response.raise_for_status()
        total_count = None
        if count and "content-range" in response.headers:
            content_range = response.headers["content-range"]
            if "/" in content_range:
                total_count = int(content_range.split("/")[-1])
        return response.json(), total_count
