from __future__ import annotations

from typing import List

from src.repositories.fx_repository import FxRepository
from src.schemas.fx import FxExposure, FxRate


class FxService:
    def __init__(self, repository: FxRepository) -> None:
        self.repository = repository

    def get_rates(self, limit: int = 50) -> List[FxRate]:
        records = self.repository.list_latest_rates(limit=limit)
        return [
            FxRate(
                id=r.id,
                currency_pair=r.currency_pair,
                rate_timestamp=r.rate_timestamp,
                bid_rate=r.bid_rate,
                ask_rate=r.ask_rate,
                mid_rate=r.mid_rate,
                source=r.source,
                created_at=r.created_at,
            )
            for r in records
        ]

    def get_exposure(self) -> List[FxExposure]:
        try:
            records = self.repository.list_exposure()
        except Exception:
            return []
        return [
            FxExposure(
                currency_code=r.currency_code,
                confirmed_30d=r.confirmed_30d,
                confirmed_60d=r.confirmed_60d,
                confirmed_90d=r.confirmed_90d,
                estimated_30d=r.estimated_30d,
                estimated_60d=r.estimated_60d,
                estimated_90d=r.estimated_90d,
                current_holdings=r.current_holdings,
                net_exposure=r.net_exposure,
            )
            for r in records
        ]
