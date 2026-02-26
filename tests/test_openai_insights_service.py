from __future__ import annotations

import pytest

from src.core.errors import BadRequestError
from src.services.openai_insights_service import OpenAiInsightsService


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

