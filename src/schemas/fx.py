from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Optional

from src.shared.base import BaseSchema


class FxRate(BaseSchema):
    id: str
    currency_pair: Optional[str] = None
    rate_timestamp: Optional[datetime] = None
    bid_rate: Optional[Decimal] = None
    ask_rate: Optional[Decimal] = None
    mid_rate: Optional[Decimal] = None
    source: Optional[str] = None
    created_at: Optional[datetime] = None


class FxExposure(BaseSchema):
    currency_code: Optional[str] = None
    confirmed_30d: Optional[Decimal] = None
    confirmed_60d: Optional[Decimal] = None
    confirmed_90d: Optional[Decimal] = None
    estimated_30d: Optional[Decimal] = None
    estimated_60d: Optional[Decimal] = None
    estimated_90d: Optional[Decimal] = None
    current_holdings: Optional[Decimal] = None
    net_exposure: Optional[Decimal] = None
