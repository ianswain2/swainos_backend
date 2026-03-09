from __future__ import annotations

import json
from decimal import Decimal
from typing import Any

import httpx
from google.auth.transport.requests import Request
from google.oauth2 import service_account

from src.core.config import get_settings
from src.core.errors import BadRequestError

GA4_READONLY_SCOPE = "https://www.googleapis.com/auth/analytics.readonly"
GA4_RUN_REPORT_URL = (
    "https://analyticsdata.googleapis.com/v1beta/properties/{property_id}:runReport"
)


def _to_decimal(value: str | None) -> Decimal:
    if value is None or value == "":
        return Decimal("0")
    try:
        return Decimal(str(value))
    except Exception:
        return Decimal("0")


class GoogleAnalyticsClient:
    def __init__(self) -> None:
        self.settings = get_settings()
        self.property_id = (self.settings.google_ga4_property_id or "").strip()
        self._credentials = self._build_credentials()

    def _build_credentials(self):
        key_json = (self.settings.google_service_account_key_json or "").strip()
        if not key_json:
            raise BadRequestError("GOOGLE_SERVICE_ACCOUNT_KEY_JSON is required for GA4 integration")
        if not self.property_id:
            raise BadRequestError("GOOGLE_GA4_PROPERTY_ID is required for GA4 integration")
        try:
            account_info = json.loads(key_json)
        except json.JSONDecodeError as exc:
            raise BadRequestError("GOOGLE_SERVICE_ACCOUNT_KEY_JSON must be valid JSON") from exc

        return service_account.Credentials.from_service_account_info(
            account_info,
            scopes=[GA4_READONLY_SCOPE],
        )

    def _access_token(self) -> str:
        credentials = self._credentials
        if not credentials.valid or not credentials.token:
            credentials.refresh(Request())
        if not credentials.token:
            raise BadRequestError("Unable to acquire Google access token for GA4")
        return credentials.token

    def run_report(
        self,
        *,
        start_date: str,
        end_date: str,
        metrics: list[str],
        dimensions: list[str] | None = None,
        limit: int = 1000,
        order_bys: list[dict[str, Any]] | None = None,
        dimension_filter: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        token = self._access_token()
        body: dict[str, Any] = {
            "dateRanges": [{"startDate": start_date, "endDate": end_date}],
            "metrics": [{"name": metric} for metric in metrics],
            "limit": limit,
        }
        if dimensions:
            body["dimensions"] = [{"name": dimension} for dimension in dimensions]
        if order_bys:
            body["orderBys"] = order_bys
        if dimension_filter:
            body["dimensionFilter"] = dimension_filter

        url = GA4_RUN_REPORT_URL.format(property_id=self.property_id)
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
            message = "GA4 API request failed"
            try:
                payload = response.json()
                message = payload.get("error", {}).get("message", message)
            except Exception:
                pass
            raise BadRequestError(message) from exc

        payload = response.json()
        metric_headers = [header.get("name", "") for header in payload.get("metricHeaders", [])]
        dimension_headers = [
            header.get("name", "") for header in payload.get("dimensionHeaders", [])
        ]
        rows: list[dict[str, Any]] = []
        for row in payload.get("rows", []):
            mapped: dict[str, Any] = {}
            metric_values = row.get("metricValues", [])
            dimension_values = row.get("dimensionValues", [])
            for idx, header in enumerate(metric_headers):
                value = metric_values[idx].get("value") if idx < len(metric_values) else None
                mapped[header] = _to_decimal(value)
            for idx, header in enumerate(dimension_headers):
                value = dimension_values[idx].get("value") if idx < len(dimension_values) else None
                mapped[header] = value
            rows.append(mapped)
        return rows
