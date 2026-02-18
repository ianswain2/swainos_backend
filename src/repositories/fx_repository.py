from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from src.core.supabase import SupabaseClient
from src.models.fx import (
    FxExposureRecord,
    FxHoldingRecord,
    FxIntelligenceItemRecord,
    FxIntelligenceRunRecord,
    FxInvoicePressureRecord,
    FxRateRecord,
    FxSignalRecord,
    FxSignalRunRecord,
    FxTransactionRecord,
)

SUPPORTED_FX_CURRENCIES = frozenset({"ZAR", "USD", "AUD", "NZD"})
SUPPORTED_TARGET_CURRENCIES = frozenset({"ZAR", "AUD", "NZD"})
SUPPORTED_FX_PAIRS = tuple(
    f"{base}/{quote}"
    for base in sorted(SUPPORTED_FX_CURRENCIES)
    for quote in sorted(SUPPORTED_FX_CURRENCIES)
    if base != quote
)


def _pair_uses_supported_currencies(currency_pair: str | None) -> bool:
    if not currency_pair or "/" not in currency_pair:
        return False
    parts = [p.strip().upper() for p in currency_pair.split("/", 1)]
    return len(parts) == 2 and parts[0] in SUPPORTED_FX_CURRENCIES and parts[1] in SUPPORTED_FX_CURRENCIES


def _to_str_list(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(item) for item in value if item is not None and str(item).strip()]
    return []


class FxRepository:
    def __init__(self) -> None:
        self.client = SupabaseClient()

    @staticmethod
    def _to_iso_utc(dt: datetime) -> str:
        return dt.astimezone(timezone.utc).isoformat()

    def upsert_rates(self, rows: List[Dict[str, Any]]) -> List[FxRateRecord]:
        if not rows:
            return []
        inserted = self.client.insert(
            table="fx_rates",
            payload=rows,
            upsert=True,
            on_conflict="currency_pair,rate_timestamp,source",
        )
        return [FxRateRecord.model_validate(row) for row in inserted]

    def list_latest_rates(
        self,
        limit: int = 50,
        offset: int = 0,
        include_totals: bool = False,
    ) -> tuple[List[FxRateRecord], int]:
        pair_filter = ",".join(SUPPORTED_FX_PAIRS)
        rows, total_count = self.client.select(
            table="fx_rates",
            select="id,currency_pair,rate_timestamp,bid_rate,ask_rate,mid_rate,source,created_at",
            filters=[("currency_pair", f"in.({pair_filter})")],
            order="rate_timestamp.desc",
            limit=max(limit, 1),
            offset=max(offset, 0),
            count="exact" if include_totals else "planned",
        )
        records = [FxRateRecord.model_validate(row) for row in rows]
        filtered = [r for r in records if _pair_uses_supported_currencies(r.currency_pair)]
        estimated_total = max(offset + len(filtered), len(filtered))
        return filtered[:limit], (total_count if total_count is not None else estimated_total)

    def list_recent_rates_for_pair(self, currency_pair: str, days_back: int = 30) -> List[FxRateRecord]:
        since = datetime.now(timezone.utc) - timedelta(days=max(days_back, 1))
        rows, _ = self.client.select(
            table="fx_rates",
            select="id,currency_pair,rate_timestamp,bid_rate,ask_rate,mid_rate,source,created_at",
            filters=[
                ("currency_pair", f"eq.{currency_pair}"),
                ("rate_timestamp", f"gte.{self._to_iso_utc(since)}"),
            ],
            order="rate_timestamp.desc",
            limit=500,
        )
        return [FxRateRecord.model_validate(row) for row in rows]

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

    def list_invoice_pressure(self) -> List[FxInvoicePressureRecord]:
        rows, _ = self.client.select(
            table="fx_invoice_pressure_v1",
            select=(
                "currency_code,due_7d_amount,due_30d_amount,due_60d_amount,due_90d_amount,"
                "invoices_due_30d_count,next_due_date"
            ),
            order="currency_code.asc",
            limit=20,
        )
        return [FxInvoicePressureRecord.model_validate(row) for row in rows]

    def create_signal_run(self, payload: Dict[str, Any]) -> FxSignalRunRecord:
        rows = self.client.insert(table="fx_signal_runs", payload=payload)
        return FxSignalRunRecord.model_validate(rows[0])

    def update_signal_run(self, run_id: str, payload: Dict[str, Any]) -> Optional[FxSignalRunRecord]:
        rows = self.client.update(
            table="fx_signal_runs",
            payload=payload,
            filters=[("id", f"eq.{run_id}")],
        )
        if not rows:
            return None
        return FxSignalRunRecord.model_validate(rows[0])

    def insert_signals(self, rows: List[Dict[str, Any]]) -> List[FxSignalRecord]:
        if not rows:
            return []
        created = self.client.insert(table="fx_signals", payload=rows)
        return [FxSignalRecord.model_validate(row) for row in created]

    def list_signals(
        self,
        limit: int = 25,
        currency_code: Optional[str] = None,
        offset: int = 0,
        include_totals: bool = False,
    ) -> tuple[List[FxSignalRecord], int]:
        filters: List[tuple[str, str]] = [("currency_code", "in.(AUD,NZD,ZAR)")]
        if currency_code:
            normalized = currency_code.strip().upper()
            if normalized in SUPPORTED_TARGET_CURRENCIES:
                filters = [("currency_code", f"eq.{normalized}")]
        rows, total_count = self.client.select(
            table="fx_signals",
            select=(
                "id,currency_code,signal_type,signal_strength,current_rate,avg_30d_rate,exposure_amount,"
                "recommended_amount,reasoning,generated_at,expires_at,was_acted_on,run_id,confidence,"
                "reason_summary,trend_tags,source_links,exposure_30d_amount,invoice_pressure_30d,"
                "invoice_pressure_60d,invoice_pressure_90d,metadata,created_at,updated_at"
            ),
            filters=filters,
            order="generated_at.desc",
            limit=limit,
            offset=max(offset, 0),
            count="exact" if include_totals else "planned",
        )
        for row in rows:
            row["trend_tags"] = _to_str_list(row.get("trend_tags"))
            row["source_links"] = _to_str_list(row.get("source_links"))
        estimated_total = max(offset + len(rows), len(rows))
        return [FxSignalRecord.model_validate(row) for row in rows], (
            total_count if total_count is not None else estimated_total
        )

    def create_transaction(self, payload: Dict[str, Any]) -> FxTransactionRecord:
        rows = self.client.insert(table="fx_transactions", payload=payload)
        return FxTransactionRecord.model_validate(rows[0])

    def list_transactions(
        self,
        limit: int = 100,
        currency_code: Optional[str] = None,
        transaction_type: Optional[str] = None,
        offset: int = 0,
        include_totals: bool = False,
    ) -> tuple[List[FxTransactionRecord], int]:
        filters: List[tuple[str, str]] = []
        if currency_code:
            filters.append(("currency_code", f"eq.{currency_code.strip().upper()}"))
        if transaction_type:
            filters.append(("transaction_type", f"eq.{transaction_type.strip().upper()}"))
        rows, total_count = self.client.select(
            table="fx_transactions",
            select=(
                "id,currency_code,transaction_type,transaction_date,amount,exchange_rate,usd_equivalent,"
                "balance_after,supplier_invoice_id,signal_id,reference_number,notes,entered_by,"
                "created_at,updated_at"
            ),
            filters=filters,
            order="transaction_date.desc,created_at.desc",
            limit=limit,
            offset=max(offset, 0),
            count="exact" if include_totals else "planned",
        )
        estimated_total = max(offset + len(rows), len(rows))
        return [FxTransactionRecord.model_validate(row) for row in rows], (
            total_count if total_count is not None else estimated_total
        )

    def list_holdings(self, currency_code: Optional[str] = None) -> List[FxHoldingRecord]:
        filters: List[tuple[str, str]] = []
        if currency_code:
            filters.append(("currency_code", f"eq.{currency_code.strip().upper()}"))
        rows, total_count = self.client.select(
            table="fx_holdings",
            select=(
                "id,currency_code,balance_amount,avg_purchase_rate,total_purchased,total_spent,"
                "last_transaction_date,last_reconciled_at,notes,created_at,updated_at"
            ),
            filters=filters,
            order="currency_code.asc",
            limit=20,
        )
        return [FxHoldingRecord.model_validate(row) for row in rows]

    def create_intelligence_run(self, payload: Dict[str, Any]) -> FxIntelligenceRunRecord:
        rows = self.client.insert(table="fx_intelligence_runs", payload=payload)
        return FxIntelligenceRunRecord.model_validate(rows[0])

    def update_intelligence_run(
        self,
        run_id: str,
        payload: Dict[str, Any],
    ) -> Optional[FxIntelligenceRunRecord]:
        rows = self.client.update(
            table="fx_intelligence_runs",
            payload=payload,
            filters=[("id", f"eq.{run_id}")],
        )
        if not rows:
            return None
        return FxIntelligenceRunRecord.model_validate(rows[0])

    def insert_intelligence_items(self, rows: List[Dict[str, Any]]) -> List[FxIntelligenceItemRecord]:
        if not rows:
            return []
        inserted = self.client.insert(
            table="fx_intelligence_items",
            payload=rows,
            upsert=True,
            on_conflict="run_id,source_url",
        )
        return [FxIntelligenceItemRecord.model_validate(row) for row in inserted]

    def list_intelligence(
        self,
        limit: int = 50,
        currency_code: Optional[str] = None,
        offset: int = 0,
        include_totals: bool = False,
    ) -> tuple[List[FxIntelligenceItemRecord], int]:
        filters: List[tuple[str, str]] = []
        if currency_code:
            filters.append(("currency_code", f"eq.{currency_code.strip().upper()}"))
        rows, total_count = self.client.select(
            table="fx_intelligence_items",
            select=(
                "id,run_id,currency_code,source_type,source_title,source_url,source_publisher,"
                "source_credibility_score,published_at,risk_direction,confidence,trend_tags,summary,"
                "raw_payload,created_at"
            ),
            filters=filters,
            order="published_at.desc,created_at.desc",
            limit=limit,
            offset=max(offset, 0),
            count="exact" if include_totals else "planned",
        )
        for row in rows:
            row["trend_tags"] = _to_str_list(row.get("trend_tags"))
        estimated_total = max(offset + len(rows), len(rows))
        return [FxIntelligenceItemRecord.model_validate(row) for row in rows], (
            total_count if total_count is not None else estimated_total
        )

    def refresh_fx_exposure(self) -> Dict[str, Any]:
        payload = self.client.rpc("refresh_fx_exposure_v1", payload={})
        if isinstance(payload, dict):
            return payload
        return {"status": "ok", "result": payload}

    def create_sync_log(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        rows = self.client.insert(table="sync_logs", payload=payload)
        return rows[0] if rows else {}

    def update_sync_log(self, log_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        rows = self.client.update(
            table="sync_logs",
            payload=payload,
            filters=[("id", f"eq.{log_id}")],
        )
        return rows[0] if rows else {}
