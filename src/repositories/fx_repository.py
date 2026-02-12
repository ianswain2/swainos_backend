from __future__ import annotations

from typing import List

from src.core.supabase import SupabaseClient
from src.models.fx import FxExposureRecord, FxRateRecord

SUPPORTED_FX_CURRENCIES = frozenset({"ZAR", "USD", "AUD", "NZD"})


def _pair_uses_supported_currencies(currency_pair: str | None) -> bool:
    if not currency_pair or "/" not in currency_pair:
        return False
    parts = [p.strip().upper() for p in currency_pair.split("/", 1)]
    return len(parts) == 2 and parts[0] in SUPPORTED_FX_CURRENCIES and parts[1] in SUPPORTED_FX_CURRENCIES


class FxRepository:
    def __init__(self) -> None:
        self.client = SupabaseClient()

    def list_latest_rates(self, limit: int = 50) -> List[FxRateRecord]:
        rows, _ = self.client.select(
            table="fx_rates",
            select="id,currency_pair,rate_timestamp,bid_rate,ask_rate,mid_rate,source,created_at",
            order="rate_timestamp.desc",
            limit=limit * 3,
        )
        records = [FxRateRecord.model_validate(row) for row in rows]
        filtered = [r for r in records if _pair_uses_supported_currencies(r.currency_pair)]
        return filtered[:limit]

    def list_exposure(self) -> List[FxExposureRecord]:
        rows, _ = self.client.select(
            table="mv_fx_exposure",
            select=(
                "currency_code,confirmed_30d,confirmed_60d,confirmed_90d,"
                "estimated_30d,estimated_60d,estimated_90d,current_holdings,net_exposure"
            ),
            filters=[("currency_code", "in.(ZAR,USD,AUD,NZD)")],
        )
        return [FxExposureRecord.model_validate(row) for row in rows]
