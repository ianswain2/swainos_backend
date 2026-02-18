from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple

import httpx

from src.core.config import get_settings
from src.core.errors import BadRequestError
from src.repositories.fx_repository import FxRepository
from src.schemas.fx import FxIntelligenceItem, FxIntelligenceRunRequest, FxManualRunResult
from src.services.openai_insights_service import OpenAiInsightsService

TRUSTED_SOURCE_HOSTS = (
    "fred.stlouisfed.org",
    "stlouisfed.org",
    "federalreserve.gov",
    "rba.gov.au",
    "rbnz.govt.nz",
    "resbank.co.za",
    "reuters.com",
    "bloomberg.com",
    "ft.com",
    "wsj.com",
)
SUPPORTED_TARGET_CURRENCIES = frozenset({"AUD", "NZD", "ZAR"})


class FxIntelligenceService:
    def __init__(
        self,
        repository: FxRepository,
        openai_service: OpenAiInsightsService,
    ) -> None:
        self.repository = repository
        self.openai_service = openai_service
        self.settings = get_settings()

    def _target_currencies(self) -> List[str]:
        raw = self.settings.fx_target_currencies or ""
        parsed = [item.strip().upper() for item in raw.split(",") if item.strip()]
        return [item for item in parsed if item in SUPPORTED_TARGET_CURRENCIES]

    @staticmethod
    def _now_utc() -> datetime:
        return datetime.now(timezone.utc)

    def list_intelligence(
        self,
        *,
        page: int = 1,
        page_size: int = 50,
        include_totals: bool = False,
        currency_code: Optional[str] = None,
    ) -> Tuple[List[FxIntelligenceItem], int]:
        offset = (page - 1) * page_size
        records, total_count = self.repository.list_intelligence(
            limit=page_size,
            currency_code=currency_code,
            offset=offset,
            include_totals=include_totals,
        )
        return [FxIntelligenceItem(**record.model_dump()) for record in records], total_count

    def run_intelligence(self, request: FxIntelligenceRunRequest) -> FxManualRunResult:
        run = self.repository.create_intelligence_run(
            {
                "run_type": request.run_type,
                "status": "running",
                "metadata": {
                    "macroProvider": self.settings.macro_provider,
                    "newsProvider": self.settings.news_provider,
                },
            }
        )
        try:
            item_rows: List[Dict[str, Any]] = []
            total_sources = 0
            for currency in self._target_currencies():
                macro_items = self._fetch_macro_items(currency)
                news_items = self._fetch_news_items(currency)
                combined = self._dedupe_source_items(macro_items + news_items)
                total_sources += len(combined)
                if not combined:
                    continue

                synthesis = self._synthesize_currency_intelligence(currency, combined)
                trend_tags = synthesis.get("trendTags") if isinstance(synthesis.get("trendTags"), list) else []
                risk_direction = str(synthesis.get("riskDirection") or "neutral")
                confidence = self._to_decimal(synthesis.get("confidence"))
                summary = str(synthesis.get("summary") or "").strip()
                if not summary:
                    summary = f"{currency} intelligence synthesized from current macro and news sources."

                for source_item in combined:
                    item_rows.append(
                        {
                            "run_id": run.id,
                            "currency_code": currency,
                            "source_type": source_item.get("source_type", "news"),
                            "source_title": source_item["source_title"],
                            "source_url": source_item["source_url"],
                            "source_publisher": source_item.get("source_publisher"),
                            "source_credibility_score": source_item.get("source_credibility_score"),
                            "published_at": source_item.get("published_at"),
                            "risk_direction": risk_direction
                            if risk_direction in {"bullish", "bearish", "neutral", "mixed"}
                            else "neutral",
                            "confidence": confidence,
                            "trend_tags": trend_tags[:8],
                            "summary": summary,
                            "raw_payload": source_item.get("raw_payload", {}),
                        }
                    )

            if item_rows:
                self.repository.insert_intelligence_items(item_rows)

            status = "success" if total_sources >= self.settings.fx_intelligence_min_source_count else "partial"
            self.repository.update_intelligence_run(
                run.id,
                {
                    "status": status,
                    "completed_at": self._now_utc().isoformat(),
                    "source_count": total_sources,
                    "model_name": self.settings.openai_model_support,
                    "model_tier": "support",
                },
            )
            return FxManualRunResult(
                run_id=run.id,
                status=status,
                records_processed=total_sources,
                records_created=len(item_rows),
                message="FX intelligence run completed",
            )
        except Exception as exc:
            self.repository.update_intelligence_run(
                run.id,
                {
                    "status": "failed",
                    "completed_at": self._now_utc().isoformat(),
                    "error_message": str(exc),
                },
            )
            raise

    def _fetch_macro_items(self, currency_code: str) -> List[Dict[str, Any]]:
        if self.settings.macro_provider.strip().lower() != "fred":
            return []
        if not self.settings.macro_api_key:
            return []

        series_map = {
            "AUD": "DEXUSAL",
            "NZD": "DEXUSNZ",
            "ZAR": "DEXSFUS",
        }
        series_id = series_map.get(currency_code)
        if not series_id:
            return []
        endpoint = f"{self.settings.macro_base_url.rstrip('/')}/series/observations"
        params = {
            "series_id": series_id,
            "api_key": self.settings.macro_api_key,
            "file_type": "json",
            "sort_order": "desc",
            "limit": "5",
        }
        try:
            response = httpx.get(endpoint, params=params, timeout=20.0)
            response.raise_for_status()
            payload = response.json()
        except Exception:
            return []

        observations = payload.get("observations") if isinstance(payload, dict) else None
        if not isinstance(observations, list):
            return []
        items: List[Dict[str, Any]] = []
        for obs in observations[:5]:
            if not isinstance(obs, dict):
                continue
            date_value = obs.get("date")
            value = obs.get("value")
            if not date_value or value in {None, "."}:
                continue
            source_url = (
                "https://fred.stlouisfed.org/series/"
                + series_id
            )
            items.append(
                {
                    "source_type": "macro",
                    "source_title": f"FRED {series_id} observation",
                    "source_url": source_url,
                    "source_publisher": "FRED",
                    "source_credibility_score": Decimal("0.95"),
                    "published_at": f"{date_value}T00:00:00+00:00",
                    "raw_payload": {
                        "seriesId": series_id,
                        "date": date_value,
                        "value": value,
                    },
                }
            )
        return items

    def _fetch_news_items(self, currency_code: str) -> List[Dict[str, Any]]:
        provider = self.settings.news_provider.strip().lower()
        if provider != "marketaux":
            return []
        if not self.settings.news_api_key:
            return []

        keywords = {
            "AUD": "australian dollar OR RBA OR AUDUSD",
            "NZD": "new zealand dollar OR RBNZ OR NZDUSD",
            "ZAR": "south african rand OR SARB OR USDZAR",
        }
        query = keywords.get(currency_code, currency_code)
        endpoint = f"{self.settings.news_base_url.rstrip('/')}/v1/news/all"
        params = {
            "api_token": self.settings.news_api_key,
            "search": query,
            "language": "en",
            "limit": "10",
        }
        try:
            response = httpx.get(endpoint, params=params, timeout=20.0)
            response.raise_for_status()
            payload = response.json()
        except Exception:
            return []

        data = payload.get("data") if isinstance(payload, dict) else None
        if not isinstance(data, list):
            return []

        items: List[Dict[str, Any]] = []
        for entry in data[:10]:
            if not isinstance(entry, dict):
                continue
            url = entry.get("url")
            title = entry.get("title")
            if not url or not title:
                continue
            domain = str(entry.get("source") or "")
            items.append(
                {
                    "source_type": "news",
                    "source_title": str(title),
                    "source_url": str(url),
                    "source_publisher": domain or None,
                    "source_credibility_score": self._credibility_score(str(url), domain),
                    "published_at": entry.get("published_at"),
                    "raw_payload": entry,
                }
            )
        return items

    def _synthesize_currency_intelligence(
        self,
        currency_code: str,
        source_items: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        payload = {
            "currencyCode": currency_code,
            "sources": [
                {
                    "title": item.get("source_title"),
                    "url": item.get("source_url"),
                    "publisher": item.get("source_publisher"),
                    "credibilityScore": item.get("source_credibility_score"),
                    "publishedAt": item.get("published_at"),
                }
                for item in source_items[:12]
            ],
        }
        fallback_payload = {
            "summary": f"{currency_code} market context synthesized from validated macro and news sources.",
            "trendTags": ["Market Update"],
            "riskDirection": "neutral",
            "confidence": 0.5,
        }
        result = self.openai_service.build_structured_output(
            tier=OpenAiInsightsService.TIER_SUPPORT,
            operation="light_summary",
            system_prompt=(
                "You are an FX intelligence summarizer for finance operators. "
                "Return JSON with keys: summary (string), trendTags (array of max 5 short strings), "
                "riskDirection (bullish|bearish|neutral|mixed), confidence (0..1). "
                "Use concise business language and avoid speculation beyond provided sources."
            ),
            user_payload=payload,
            fallback_payload=fallback_payload,
        )
        return result.payload

    @staticmethod
    def _credibility_score(url: str, publisher: str | None) -> Decimal:
        lowered = f"{url} {publisher or ''}".lower()
        for host in TRUSTED_SOURCE_HOSTS:
            if host in lowered:
                return Decimal("0.90")
        return Decimal("0.55")

    @staticmethod
    def _dedupe_source_items(source_items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        seen = set()
        deduped: List[Dict[str, Any]] = []
        for item in source_items:
            url = str(item.get("source_url") or "").strip()
            title = str(item.get("source_title") or "").strip()
            if not url or not title:
                continue
            # Keep one row per source URL within a run so upsert on (run_id, source_url)
            # cannot collide multiple times inside the same batch insert payload.
            dedupe_key = url
            if dedupe_key in seen:
                continue
            seen.add(dedupe_key)
            deduped.append(item)
        return deduped

    @staticmethod
    def _to_decimal(value: Any) -> Optional[Decimal]:
        if value is None:
            return None
        if isinstance(value, Decimal):
            return value
        try:
            return Decimal(str(value))
        except Exception:
            return None

