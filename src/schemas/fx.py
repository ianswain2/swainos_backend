from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from uuid import UUID
from typing import Any, Dict, List, Literal, Optional

from pydantic import ConfigDict, Field

from src.shared.base import BaseSchema


FxSignalType = Literal["buy_now", "wait"]
FxSignalStrength = Literal["low", "medium", "high"]
FxTransactionType = Literal["BUY", "SPEND", "ADJUSTMENT"]
FxSignalRunType = Literal["scheduled", "manual", "on_demand"]
FxRatePullRunType = Literal["scheduled", "manual", "on_demand"]
FxSignalRunStatus = Literal["running", "success", "failed", "partial", "skipped"]
FxIntelligenceRunType = Literal["daily", "on_demand"]
FxIntelligenceRunStatus = Literal["running", "success", "failed", "partial", "skipped"]


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


class FxSignalRun(BaseSchema):
    id: str
    run_type: FxSignalRunType
    status: FxSignalRunStatus
    started_at: datetime
    completed_at: Optional[datetime] = None
    rates_source: Optional[str] = None
    target_currencies: List[str] = Field(default_factory=list)
    records_processed: int = 0
    signals_generated: int = 0
    model_name: Optional[str] = None
    model_tier: Optional[str] = None
    calculation_version: str = "v1"
    error_message: Optional[str] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)
    created_at: datetime
    updated_at: datetime


class FxSignal(BaseSchema):
    id: str
    currency_code: Optional[str] = None
    signal_type: Optional[FxSignalType] = None
    signal_strength: Optional[FxSignalStrength] = None
    current_rate: Optional[Decimal] = None
    avg_30d_rate: Optional[Decimal] = None
    exposure_amount: Optional[Decimal] = None
    recommended_amount: Optional[Decimal] = None
    reasoning: Optional[str] = None
    generated_at: Optional[datetime] = None
    expires_at: Optional[datetime] = None
    was_acted_on: Optional[bool] = None
    run_id: Optional[str] = None
    confidence: Optional[Decimal] = None
    reason_summary: Optional[str] = None
    trend_tags: List[str] = Field(default_factory=list)
    source_links: List[str] = Field(default_factory=list)
    exposure_30d_amount: Optional[Decimal] = None
    invoice_pressure_30d: Optional[Decimal] = None
    invoice_pressure_60d: Optional[Decimal] = None
    invoice_pressure_90d: Optional[Decimal] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


class FxSignalRunRequest(BaseSchema):
    model_config = ConfigDict(extra="forbid")

    run_type: FxSignalRunType = "manual"


class FxRatePullRunRequest(BaseSchema):
    model_config = ConfigDict(extra="forbid")

    run_type: FxRatePullRunType = "manual"


class FxManualRunResult(BaseSchema):
    run_id: str
    status: str
    records_processed: int
    records_created: int
    message: str


class FxTransaction(BaseSchema):
    id: str
    currency_code: str
    transaction_type: FxTransactionType
    transaction_date: date
    amount: Decimal
    exchange_rate: Optional[Decimal] = None
    usd_equivalent: Optional[Decimal] = None
    balance_after: Optional[Decimal] = None
    supplier_invoice_id: Optional[str] = None
    signal_id: Optional[str] = None
    reference_number: Optional[str] = None
    notes: Optional[str] = None
    entered_by: Optional[UUID] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


class FxTransactionCreateRequest(BaseSchema):
    model_config = ConfigDict(extra="forbid")

    currency_code: str = Field(min_length=3, max_length=3)
    transaction_type: FxTransactionType
    transaction_date: date
    amount: Decimal
    exchange_rate: Optional[Decimal] = None
    usd_equivalent: Optional[Decimal] = None
    supplier_invoice_id: Optional[str] = None
    signal_id: Optional[str] = None
    reference_number: Optional[str] = Field(default=None, max_length=120)
    notes: Optional[str] = Field(default=None, max_length=2000)
    entered_by: Optional[UUID] = None


class FxHolding(BaseSchema):
    id: str
    currency_code: str
    balance_amount: Optional[Decimal] = None
    avg_purchase_rate: Optional[Decimal] = None
    total_purchased: Optional[Decimal] = None
    total_spent: Optional[Decimal] = None
    last_transaction_date: Optional[date] = None
    last_reconciled_at: Optional[datetime] = None
    notes: Optional[str] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


class FxIntelligenceItem(BaseSchema):
    id: str
    run_id: str
    currency_code: str
    source_type: str
    source_title: str
    source_url: str
    source_publisher: Optional[str] = None
    source_credibility_score: Optional[Decimal] = None
    published_at: Optional[datetime] = None
    risk_direction: Literal["bullish", "bearish", "neutral", "mixed"] = "neutral"
    confidence: Optional[Decimal] = None
    trend_tags: List[str] = Field(default_factory=list)
    summary: str
    raw_payload: Dict[str, Any] = Field(default_factory=dict)
    created_at: datetime


class FxIntelligenceRunRequest(BaseSchema):
    model_config = ConfigDict(extra="forbid")

    run_type: FxIntelligenceRunType = "on_demand"


class FxIntelligenceRun(BaseSchema):
    id: str
    run_type: FxIntelligenceRunType
    status: FxIntelligenceRunStatus
    started_at: datetime
    completed_at: Optional[datetime] = None
    source_count: int = 0
    model_name: Optional[str] = None
    model_tier: Optional[str] = None
    error_message: Optional[str] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)
    created_at: datetime
    updated_at: datetime


class FxInvoicePressure(BaseSchema):
    currency_code: str
    due_7d_amount: Optional[Decimal] = None
    due_30d_amount: Optional[Decimal] = None
    due_60d_amount: Optional[Decimal] = None
    due_90d_amount: Optional[Decimal] = None
    invoices_due_30d_count: Optional[int] = None
    next_due_date: Optional[date] = None
