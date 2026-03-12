from __future__ import annotations

import pytest

from src.core.errors import BadRequestError
from src.services.openai_insights_service import ModelRunBudget, OpenAiInsightsService


def test_decision_operation_rejects_support_tier() -> None:
    service = OpenAiInsightsService()
    with pytest.raises(BadRequestError):
        service.build_structured_output(
            tier=OpenAiInsightsService.TIER_SUPPORT,
            operation="daily_briefing",
            system_prompt="Return JSON",
            user_payload={"value": 1},
            fallback_payload={"value": 1},
        )


def test_decision_operation_rejects_without_api_key() -> None:
    service = OpenAiInsightsService()
    service.settings.openai_api_key = None
    with pytest.raises(BadRequestError):
        service.build_structured_output(
            tier=OpenAiInsightsService.TIER_DECISION,
            operation="consultant_coaching",
            system_prompt="Return JSON",
            user_payload={"value": 1},
            fallback_payload={"value": 1},
        )


def test_build_structured_output_enforces_model_call_budget() -> None:
    service = OpenAiInsightsService()
    service.settings.openai_api_key = "test-key"
    budget = ModelRunBudget(max_model_calls=0, max_tokens=1000)
    with pytest.raises(BadRequestError, match="AI budget exceeded"):
        service.build_structured_output(
            tier=OpenAiInsightsService.TIER_DECISION,
            operation="consultant_coaching",
            system_prompt="Return JSON",
            user_payload={"value": 1},
            fallback_payload={"value": 1},
            run_budget=budget,
        )


def test_build_structured_output_enforces_token_budget(monkeypatch: pytest.MonkeyPatch) -> None:
    class _FakeResponse:
        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict[str, object]:
            return {
                "choices": [{"message": {"content": "{\"ok\": true}"}}],
                "usage": {"total_tokens": 50},
            }

    monkeypatch.setattr("src.services.openai_insights_service.httpx.post", lambda *a, **k: _FakeResponse())
    service = OpenAiInsightsService()
    service.settings.openai_api_key = "test-key"
    budget = ModelRunBudget(max_model_calls=10, max_tokens=10)
    with pytest.raises(BadRequestError, match="AI budget exceeded"):
        service.build_structured_output(
            tier=OpenAiInsightsService.TIER_DECISION,
            operation="consultant_coaching",
            system_prompt="Return JSON",
            user_payload={"value": 1},
            fallback_payload={"value": 1},
            run_budget=budget,
        )

