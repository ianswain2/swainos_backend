from __future__ import annotations

import json
from decimal import Decimal
from typing import Any
from urllib.parse import quote

import httpx
from google.auth.transport.requests import Request
from google.oauth2 import service_account

from src.core.config import get_settings
from src.core.errors import BadRequestError

GSC_READONLY_SCOPE = "https://www.googleapis.com/auth/webmasters.readonly"
GSC_QUERY_URL = (
    "https://searchconsole.googleapis.com/webmasters/v3/sites/{site_url}/searchAnalytics/query"
)


def _to_decimal(value: str | int | float | None) -> Decimal:
    if value is None or value == "":
        return Decimal("0")
    try:
        return Decimal(str(value))
    except Exception:
        return Decimal("0")


class GoogleSearchConsoleClient:
    def __init__(self) -> None:
        self.settings = get_settings()
        self.site_url = (self.settings.google_gsc_site_url or "").strip()
        self._credentials = self._build_credentials()

    def _build_credentials(self):
        key_json = (self.settings.google_service_account_key_json or "").strip()
        if not key_json:
            raise BadRequestError(
                "GOOGLE_SERVICE_ACCOUNT_KEY_JSON is required for Search Console integration"
            )
        if not self.site_url:
            raise BadRequestError("GOOGLE_GSC_SITE_URL is required for Search Console integration")
        try:
            account_info = json.loads(key_json)
        except json.JSONDecodeError as exc:
            raise BadRequestError("GOOGLE_SERVICE_ACCOUNT_KEY_JSON must be valid JSON") from exc

        return service_account.Credentials.from_service_account_info(
            account_info,
            scopes=[GSC_READONLY_SCOPE],
        )

    def _access_token(self) -> str:
        credentials = self._credentials
        if not credentials.valid or not credentials.token:
            credentials.refresh(Request())
        if not credentials.token:
            raise BadRequestError("Unable to acquire Google access token for Search Console")
        return credentials.token

    def query(
        self,
        *,
        start_date: str,
        end_date: str,
        dimensions: list[str],
        row_limit: int = 25000,
        start_row: int = 0,
        country_filter: str | None = None,
        device_filter: str | None = None,
    ) -> list[dict[str, Any]]:
        token = self._access_token()
        body: dict[str, Any] = {
            "startDate": start_date,
            "endDate": end_date,
            "dimensions": dimensions,
            "rowLimit": row_limit,
            "startRow": start_row,
        }
        filters: list[dict[str, str]] = []
        if country_filter:
            filters.append(
                {"dimension": "country", "operator": "equals", "expression": country_filter}
            )
        if device_filter:
            filters.append(
                {"dimension": "device", "operator": "equals", "expression": device_filter}
            )
        if filters:
            body["dimensionFilterGroups"] = [{"groupType": "and", "filters": filters}]

        url = GSC_QUERY_URL.format(site_url=quote(self.site_url, safe=""))
        response = httpx.post(
            url,
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            },
            json=body,
            timeout=30.0,
        )
        try:
            response.raise_for_status()
        except httpx.HTTPStatusError as exc:
            message = "Search Console API request failed"
            try:
                payload = response.json()
                message = payload.get("error", {}).get("message", message)
            except Exception:
                pass
            raise BadRequestError(message) from exc

        payload = response.json()
        rows: list[dict[str, Any]] = []
        for row in payload.get("rows", []):
            keys = row.get("keys", [])
            mapped: dict[str, Any] = {}
            for idx, dimension in enumerate(dimensions):
                mapped[dimension] = keys[idx] if idx < len(keys) else None
            mapped["clicks"] = _to_decimal(row.get("clicks"))
            mapped["impressions"] = _to_decimal(row.get("impressions"))
            mapped["ctr"] = _to_decimal(row.get("ctr"))
            mapped["position"] = _to_decimal(row.get("position"))
            rows.append(mapped)
        return rows
