from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal
from typing import List

import pytest
from fastapi.testclient import TestClient

from src.api.dependencies import get_fx_intelligence_service, get_fx_service
from src.core.config import get_settings
from src.main import create_app
from src.schemas.fx import (
    FxExposure,
    FxHolding,
    FxIntelligenceItem,
    FxIntelligenceRunRequest,
    FxInvoicePressure,
    FxManualRunResult,
    FxRate,
    FxRatePullRunRequest,
    FxSignal,
    FxSignalRunRequest,
    FxTransaction,
    FxTransactionCreateRequest,
)


class FakeFxService:
    def get_rates(
        self,
        *,
        page: int = 1,
        page_size: int = 50,
        include_totals: bool = False,
    ) -> tuple[List[FxRate], int]:
        _ = page, include_totals
        items = [
            FxRate(
                id="rate-1",
                currency_pair="USD/AUD",
                rate_timestamp=datetime.now(timezone.utc),
                mid_rate=Decimal("1.52"),
                source="twelve_data",
            )
        ][:page_size]
        return items, len(items)

    def pull_rates(self, request: FxRatePullRunRequest) -> FxManualRunResult:
        return FxManualRunResult(
            run_id=f"run-{request.run_type}",
            status="success",
            records_processed=3,
            records_created=3,
            message="pulled",
        )

    def get_latest_rate_timestamp(self) -> datetime:
        return datetime.now(timezone.utc)

    def get_exposure(self) -> List[FxExposure]:
        return [
            FxExposure(
                currency_code="AUD",
                confirmed_30d=Decimal("1000"),
                net_exposure=Decimal("1500"),
            )
        ]

    def get_signals(
        self,
        *,
        page: int = 1,
        page_size: int = 25,
        include_totals: bool = False,
        currency_code: str | None = None,
    ) -> tuple[List[FxSignal], int]:
        _ = page, include_totals
        signal = FxSignal(
            id="signal-1",
            currency_code=currency_code or "AUD",
            signal_type="buy_now",
            signal_strength="medium",
            confidence=Decimal("0.7"),
            reason_summary="Test summary",
            source_links=["https://example.com/article"],
            trend_tags=["Policy Risk"],
            generated_at=datetime.now(timezone.utc),
        )
        items = [signal][:page_size]
        return items, len(items)

    def run_signals(self, request: FxSignalRunRequest) -> FxManualRunResult:
        return FxManualRunResult(
            run_id=f"signal-{request.run_type}",
            status="success",
            records_processed=3,
            records_created=3,
            message="signals",
        )

    def get_transactions(
        self,
        *,
        page: int = 1,
        page_size: int = 100,
        include_totals: bool = False,
        currency_code: str | None = None,
        transaction_type: str | None = None,
    ) -> tuple[List[FxTransaction], int]:
        _ = page, include_totals
        items = [
            FxTransaction(
                id="tx-1",
                currency_code=currency_code or "AUD",
                transaction_type=(transaction_type or "BUY"),  # type: ignore[arg-type]
                transaction_date=date.today(),
                amount=Decimal("100"),
                balance_after=Decimal("100"),
            )
        ][:page_size]
        return items, len(items)

    def create_transaction(self, payload: FxTransactionCreateRequest) -> FxTransaction:
        return FxTransaction(
            id="tx-created",
            currency_code=payload.currency_code.upper(),
            transaction_type=payload.transaction_type,
            transaction_date=payload.transaction_date,
            amount=payload.amount,
            exchange_rate=payload.exchange_rate,
            usd_equivalent=payload.usd_equivalent,
            balance_after=payload.amount,
            notes=payload.notes,
        )

    def get_holdings(self, currency_code: str | None = None) -> List[FxHolding]:
        return [
            FxHolding(
                id="hold-1",
                currency_code=currency_code or "AUD",
                balance_amount=Decimal("1000"),
            )
        ]

    def get_invoice_pressure(self) -> List[FxInvoicePressure]:
        return [
            FxInvoicePressure(
                currency_code="AUD",
                due_30d_amount=Decimal("800"),
                invoices_due_30d_count=2,
            )
        ]


class FakeFxIntelligenceService:
    def list_intelligence(
        self,
        *,
        page: int = 1,
        page_size: int = 50,
        include_totals: bool = False,
        currency_code: str | None = None,
    ) -> tuple[List[FxIntelligenceItem], int]:
        _ = page, include_totals
        items = [
            FxIntelligenceItem(
                id="intel-1",
                run_id="run-1",
                currency_code=currency_code or "AUD",
                source_type="news",
                source_title="Policy update",
                source_url="https://example.com/intel",
                summary="Central bank commentary suggests near-term volatility.",
                trend_tags=["Volatility"],
                risk_direction="mixed",
                confidence=Decimal("0.65"),
                created_at=datetime.now(timezone.utc),
            )
        ][:page_size]
        return items, len(items)

    def run_intelligence(self, request: FxIntelligenceRunRequest) -> FxManualRunResult:
        return FxManualRunResult(
            run_id=f"intel-{request.run_type}",
            status="success",
            records_processed=5,
            records_created=5,
            message="intelligence",
        )


@pytest.fixture()
def client() -> TestClient:
    app = create_app()
    app.dependency_overrides[get_fx_service] = FakeFxService
    app.dependency_overrides[get_fx_intelligence_service] = FakeFxIntelligenceService
    return TestClient(app)


def test_get_fx_rates_envelope(client: TestClient) -> None:
    response = client.get("/api/v1/fx/rates")
    assert response.status_code == 200
    payload = response.json()
    assert isinstance(payload["data"], list)
    assert payload["meta"]["source"] == "supabase"
    assert payload["pagination"]["page"] == 1


def test_get_fx_intelligence_includes_source_links(client: TestClient) -> None:
    response = client.get("/api/v1/fx/intelligence")
    assert response.status_code == 200
    item = response.json()["data"][0]
    assert item["sourceUrl"] == "https://example.com/intel"
    assert "Volatility" in item["trendTags"]


def test_create_fx_transaction(client: TestClient) -> None:
    response = client.post(
        "/api/v1/fx/transactions",
        json={
            "currencyCode": "AUD",
            "transactionType": "BUY",
            "transactionDate": date.today().isoformat(),
            "amount": "1250.50",
            "exchangeRate": "1.52",
            "notes": "Test buy",
        },
    )
    assert response.status_code == 200
    payload = response.json()["data"]
    assert payload["transactionType"] == "BUY"
    assert payload["currencyCode"] == "AUD"


def test_manual_run_token_required_when_configured(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FX_MANUAL_RUN_TOKEN", "secret-token")
    get_settings.cache_clear()
    response = client.post("/api/v1/fx/signals/run", json={"runType": "manual"})
    assert response.status_code == 400
    get_settings.cache_clear()
