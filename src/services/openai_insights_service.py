from __future__ import annotations

import json
import time
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Dict, Optional

import httpx

from src.core.config import get_settings
from src.core.errors import BadRequestError


@dataclass
class ModelExecutionResult:
    payload: Dict[str, Any]
    model_name: str
    model_tier: str
    tokens_used: int
    latency_ms: int
    used_fallback: bool


class OpenAiInsightsService:
    TIER_DECISION = "decision"
    TIER_SUPPORT = "support"
    TIER_FALLBACK = "fallback"

    def __init__(self) -> None:
        self.settings = get_settings()

    def build_structured_output(
        self,
        *,
        tier: str,
        operation: str,
        system_prompt: str,
        user_payload: Dict[str, Any],
        fallback_payload: Dict[str, Any],
    ) -> ModelExecutionResult:
        self._validate_tier_for_operation(operation=operation, tier=tier)
        primary_model_name = self._resolve_model_for_tier(tier)
        api_key = self.settings.openai_api_key
        if not api_key:
            return ModelExecutionResult(
                payload=fallback_payload,
                model_name="deterministic-fallback",
                model_tier=self.TIER_FALLBACK,
                tokens_used=0,
                latency_ms=0,
                used_fallback=True,
            )

        attempts = max(self.settings.openai_max_retries, 0) + 1
        fallback_models = self._fallback_models_for_tier(tier)
        candidate_models: list[str] = [primary_model_name]
        for fallback_model in fallback_models:
            if fallback_model not in candidate_models:
                candidate_models.append(fallback_model)

        last_error: Optional[Exception] = None
        for model_name in candidate_models:
            for _ in range(attempts):
                started = time.perf_counter()
                try:
                    response = httpx.post(
                        "https://api.openai.com/v1/chat/completions",
                        headers={
                            "Authorization": f"Bearer {api_key}",
                            "Content-Type": "application/json",
                        },
                        json={
                            "model": model_name,
                            "temperature": 0.1,
                            "response_format": {"type": "json_object"},
                            "messages": [
                                {"role": "system", "content": system_prompt},
                                {
                                    "role": "user",
                                    "content": json.dumps(
                                        self._to_json_compatible(user_payload),
                                        separators=(",", ":"),
                                    ),
                                },
                            ],
                        },
                        timeout=self.settings.openai_timeout_seconds,
                    )
                    response.raise_for_status()
                    payload = response.json()
                    content = self._extract_content(payload)
                    structured_payload = json.loads(content)
                    if not isinstance(structured_payload, dict):
                        raise ValueError("Model response must be a JSON object")
                    usage = payload.get("usage") or {}
                    latency_ms = int((time.perf_counter() - started) * 1000)
                    tokens_used = int(usage.get("total_tokens", 0) or 0)
                    return ModelExecutionResult(
                        payload=structured_payload,
                        model_name=model_name,
                        model_tier=tier,
                        tokens_used=tokens_used,
                        latency_ms=latency_ms,
                        used_fallback=False,
                    )
                except (httpx.HTTPError, ValueError, json.JSONDecodeError) as exc:
                    last_error = exc
                    continue

        # Deterministic fallback on repeated model failure.
        _ = last_error
        return ModelExecutionResult(
            payload=fallback_payload,
            model_name="deterministic-fallback",
            model_tier=self.TIER_FALLBACK,
            tokens_used=0,
            latency_ms=0,
            used_fallback=True,
        )

    def _resolve_model_for_tier(self, tier: str) -> str:
        if tier == self.TIER_DECISION:
            return self.settings.openai_model_decision
        if tier == self.TIER_SUPPORT:
            return self.settings.openai_model_support
        if tier == self.TIER_FALLBACK:
            return "deterministic-fallback"
        raise BadRequestError("Unsupported model tier")

    def _fallback_models_for_tier(self, tier: str) -> list[str]:
        # Never route AI insights to gpt-4o family; keep the stack on newer GPT-5 lineage models.
        if tier == self.TIER_DECISION:
            return ["gpt-5.2", "gpt-5.1", "gpt-5"]
        if tier == self.TIER_SUPPORT:
            return ["gpt-5-mini", "gpt-5.1", "gpt-5"]
        return []

    def _validate_tier_for_operation(self, operation: str, tier: str) -> None:
        decision_operations = {
            "daily_briefing",
            "recommendation",
            "consultant_coaching",
            "anomaly_explanation",
        }
        support_operations = {
            "label_normalization",
            "metadata_extraction",
            "light_summary",
        }
        if operation in decision_operations and tier != self.TIER_DECISION:
            if not self.settings.ai_allow_support_for_decision:
                raise BadRequestError(
                    "Decision-critical operations must use the decision model tier"
                )
        if operation in support_operations and tier not in {
            self.TIER_SUPPORT,
            self.TIER_DECISION,
        }:
            raise BadRequestError("Support operation must use support or decision tier")

    @staticmethod
    def _extract_content(response_payload: Dict[str, Any]) -> str:
        choices = response_payload.get("choices")
        if not isinstance(choices, list) or not choices:
            raise ValueError("Model response choices missing")
        message = choices[0].get("message") if isinstance(choices[0], dict) else None
        if not isinstance(message, dict):
            raise ValueError("Model response message missing")
        content = message.get("content")
        if not isinstance(content, str):
            raise ValueError("Model response content missing")
        return content

    @staticmethod
    def _to_json_compatible(value: Any) -> Any:
        if isinstance(value, Decimal):
            return float(value)
        if isinstance(value, dict):
            return {k: OpenAiInsightsService._to_json_compatible(v) for k, v in value.items()}
        if isinstance(value, list):
            return [OpenAiInsightsService._to_json_compatible(item) for item in value]
        if isinstance(value, tuple):
            return [OpenAiInsightsService._to_json_compatible(item) for item in value]
        return value

