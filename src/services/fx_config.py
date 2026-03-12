from __future__ import annotations

SUPPORTED_TARGET_CURRENCIES = frozenset({"AUD", "NZD", "ZAR"})


def parse_target_currencies(raw_value: str | None) -> list[str]:
    parsed = [item.strip().upper() for item in (raw_value or "").split(",") if item.strip()]
    return [item for item in parsed if item in SUPPORTED_TARGET_CURRENCIES]
