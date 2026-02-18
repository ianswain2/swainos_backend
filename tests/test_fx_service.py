from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any, Dict, List, Optional

import pytest
from types import SimpleNamespace

from src.core.errors import BadRequestError
from src.models.fx import FxExposureRecord, FxHoldingRecord, FxRateRecord, FxSignalRunRecord, FxTransactionRecord
from src.schemas.fx import FxSignalRunRequest, FxTransactionCreateRequest
from src.services.fx_service import FxService


class StubFxRepository:
    def __init__(self) -> None:
        self.created_payload: Optional[Dict[str, Any]] = None
        self.holdings: List[FxHoldingRecord] = []

    def list_holdings(self, currency_code: Optional[str] = None) -> List[FxHoldingRecord]:
        _ = currency_code
        return self.holdings

    def create_transaction(self, payload: Dict[str, Any]) -> FxTransactionRecord:
        self.created_payload = payload
        return FxTransactionRecord(
            id="tx-1",
            currency_code=payload["currency_code"],
            transaction_type=payload["transaction_type"],
            transaction_date=date.fromisoformat(payload["transaction_date"]),
            amount=Decimal(str(payload["amount"])),
            exchange_rate=Decimal(str(payload["exchange_rate"]))
            if payload.get("exchange_rate") is not None
            else None,
            usd_equivalent=Decimal(str(payload["usd_equivalent"]))
            if payload.get("usd_equivalent") is not None
            else None,
            balance_after=Decimal("0"),
            notes=payload.get("notes"),
        )

    def create_signal_run(self, payload: Dict[str, Any]) -> FxSignalRunRecord:
        _ = payload
        return FxSignalRunRecord(
            id="run-1",
            run_type="manual",
            status="running",
            started_at=datetime.now(timezone.utc),
            completed_at=None,
            rates_source="twelve_data",
            target_currencies=["AUD", "NZD", "ZAR"],
            records_processed=0,
            signals_generated=0,
            model_name=None,
            model_tier=None,
            calculation_version="v1",
            error_message=None,
            metadata={},
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
        )

    def update_signal_run(self, run_id: str, payload: Dict[str, Any]) -> None:
        _ = run_id, payload

    def refresh_fx_exposure(self) -> Dict[str, Any]:
        return {"status": "ok", "refreshed_at": "2026-02-18T00:00:00+00:00"}

    def list_exposure(self) -> List[FxExposureRecord]:
        return []

    def list_invoice_pressure(self) -> List[Any]:
        return []

    def list_recent_rates_for_pair(self, currency_pair: str, days_back: int = 30) -> List[FxRateRecord]:
        _ = currency_pair, days_back
        return []


def test_create_transaction_spend_forces_negative_amount() -> None:
    repository = StubFxRepository()
    repository.holdings = [
        FxHoldingRecord(
            id="hold-1",
            currency_code="AUD",
            balance_amount=Decimal("500"),
            avg_purchase_rate=None,
            total_purchased=None,
            total_spent=None,
            last_transaction_date=None,
            last_reconciled_at=None,
            notes=None,
            created_at=None,
            updated_at=None,
        )
    ]
    service = FxService(repository=repository)
    service.settings = SimpleNamespace(fx_allow_negative_balance=False)

    request = FxTransactionCreateRequest(
        currency_code="AUD",
        transaction_type="SPEND",
        transaction_date=date.today(),
        amount=Decimal("100"),
        exchange_rate=Decimal("1.5"),
    )
    result = service.create_transaction(request)
    assert result.amount == Decimal("-100")
    assert repository.created_payload is not None
    assert Decimal(str(repository.created_payload["amount"])) == Decimal("-100")


def test_create_transaction_blocks_negative_balance() -> None:
    repository = StubFxRepository()
    repository.holdings = [
        FxHoldingRecord(
            id="hold-1",
            currency_code="NZD",
            balance_amount=Decimal("50"),
            avg_purchase_rate=None,
            total_purchased=None,
            total_spent=None,
            last_transaction_date=None,
            last_reconciled_at=None,
            notes=None,
            created_at=None,
            updated_at=None,
        )
    ]
    service = FxService(repository=repository)
    service.settings = SimpleNamespace(fx_allow_negative_balance=False)

    request = FxTransactionCreateRequest(
        currency_code="NZD",
        transaction_type="SPEND",
        transaction_date=date.today(),
        amount=Decimal("200"),
    )
    with pytest.raises(BadRequestError):
        service.create_transaction(request)


def test_create_transaction_rejects_unsupported_currency() -> None:
    repository = StubFxRepository()
    service = FxService(repository=repository)
    service.settings = SimpleNamespace(fx_allow_negative_balance=False)

    request = FxTransactionCreateRequest(
        currency_code="EUR",
        transaction_type="BUY",
        transaction_date=date.today(),
        amount=Decimal("100"),
    )
    with pytest.raises(BadRequestError):
        service.create_transaction(request)


def test_run_signals_skips_when_prerequisites_fail() -> None:
    repository = StubFxRepository()
    service = FxService(repository=repository)
    service.settings = SimpleNamespace(
        fx_target_currencies="AUD,NZD,ZAR",
        fx_primary_provider="twelve_data",
        fx_base_currency="USD",
        fx_stale_after_minutes=30,
    )

    result = service.run_signals(FxSignalRunRequest(run_type="manual"))
    assert result.status == "skipped"
