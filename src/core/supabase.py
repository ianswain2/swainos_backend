from __future__ import annotations

from threading import Lock
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlencode

import httpx

from src.core.config import get_settings


class SupabaseClient:
    _shared_client: httpx.Client | None = None
    _client_lock: Lock = Lock()

    def __init__(self) -> None:
        settings = get_settings()
        self.base_url = settings.supabase_url.rstrip("/") + "/rest/v1"
        self.api_key = settings.supabase_service_role_key or settings.supabase_anon_key
        if not self.api_key:
            raise ValueError("Supabase API key is required")
        self._client = self._get_shared_client()

    @classmethod
    def _get_shared_client(cls) -> httpx.Client:
        if cls._shared_client is not None:
            return cls._shared_client
        with cls._client_lock:
            if cls._shared_client is None:
                cls._shared_client = httpx.Client(
                    timeout=30.0,
                    limits=httpx.Limits(max_keepalive_connections=20, max_connections=100),
                )
        return cls._shared_client

    def select(
        self,
        table: str,
        select: str,
        filters: Optional[List[Tuple[str, str]]] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        order: Optional[str] = None,
        count: bool | str = False,
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
            if count is True:
                headers["Prefer"] = "count=exact"
            elif isinstance(count, str):
                headers["Prefer"] = f"count={count}"

        url = f"{self.base_url}/{table}?{urlencode(params, doseq=True)}"
        response = self._client.get(url, headers=headers)
        response.raise_for_status()
        total_count = None
        if count and "content-range" in response.headers:
            content_range = response.headers["content-range"]
            if "/" in content_range:
                total_count = int(content_range.split("/")[-1])
        return response.json(), total_count

    def insert(
        self,
        table: str,
        payload: Dict[str, Any] | List[Dict[str, Any]],
        upsert: bool = False,
        on_conflict: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        params: List[Tuple[str, str]] = []
        if on_conflict:
            params.append(("on_conflict", on_conflict))
        url = f"{self.base_url}/{table}"
        if params:
            url = f"{url}?{urlencode(params, doseq=True)}"
        headers = {
            "apikey": self.api_key,
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
            "Prefer": "return=representation",
        }
        if upsert:
            headers["Prefer"] = "resolution=merge-duplicates,return=representation"
        response = self._client.post(url, headers=headers, json=payload)
        response.raise_for_status()
        if not response.content:
            return []
        data = response.json()
        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            return [data]
        return []

    def update(
        self,
        table: str,
        payload: Dict[str, Any],
        filters: List[Tuple[str, str]],
    ) -> List[Dict[str, Any]]:
        params: List[Tuple[str, str]] = []
        if filters:
            params.extend(filters)
        url = f"{self.base_url}/{table}?{urlencode(params, doseq=True)}"
        headers = {
            "apikey": self.api_key,
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
            "Prefer": "return=representation",
        }
        response = self._client.patch(url, headers=headers, json=payload)
        response.raise_for_status()
        if not response.content:
            return []
        data = response.json()
        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            return [data]
        return []
