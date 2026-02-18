from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Dict, List, Optional, Tuple

import httpx

from src.core.config import get_settings
from src.core.errors import BadRequestError
from src.models.fx import FxExposureRecord, FxInvoicePressureRecord
from src.repositories.fx_repository import FxRepository
from src.schemas.fx import (
    FxExposure,
    FxHolding,
    FxManualRunResult,
    FxInvoicePressure,
    FxRate,
    FxRatePullRunRequest,
    FxSignal,
    FxSignalRunRequest,
    FxTransaction,
    FxTransactionCreateRequest,
)

SUPPORTED_TARGET_CURRENCIES = frozenset({"AUD", "NZD", "ZAR"})
SUPPORTED_LEDGER_CURRENCIES = frozenset({"USD", "AUD", "NZD", "ZAR"})
MIN_SIGNAL_RATE_HISTORY_POINTS = 5


class FxService:
    def __init__(self, repository: FxRepository) -> None:
        self.repository = repository
        self.settings = get_settings()

    def _target_currencies(self) -> list[str]:
        raw = self.settings.fx_target_currencies or ""
        parsed = [item.strip().upper() for item in raw.split(",") if item.strip()]
        return [item for item in parsed if item in SUPPORTED_TARGET_CURRENCIES]

    @staticmethod
    def _now_utc() -> datetime:
        return datetime.now(timezone.utc)

    def _is_stale_timestamp(self, timestamp_value: Optional[datetime]) -> bool:
        if timestamp_value is None:
            return True
        return timestamp_value < self._now_utc() - timedelta(minutes=self.settings.fx_stale_after_minutes)

    @staticmethod
    def _to_decimal(value: object) -> Optional[Decimal]:
        if value is None:
            return None
        if isinstance(value, Decimal):
            return value
        if isinstance(value, (int, float, str)):
            try:
                return Decimal(str(value))
            except Exception:
                return None
        return None

    def get_rates(
        self,
        *,
        page: int = 1,
        page_size: int = 50,
        include_totals: bool = False,
    ) -> Tuple[List[FxRate], int]:
        offset = (page - 1) * page_size
        records, total_count = self.repository.list_latest_rates(
            limit=page_size,
            offset=offset,
            include_totals=include_totals,
        )
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
        ], total_count

    def get_latest_rate_timestamp(self) -> Optional[datetime]:
        rates, _ = self.repository.list_latest_rates(limit=1)
        if not rates:
            return None
        return rates[0].rate_timestamp

    def pull_rates(self, request: FxRatePullRunRequest) -> FxManualRunResult:
        started_at = self._now_utc()
        sync_log = self.repository.create_sync_log(
            {
                "source_system": self.settings.fx_primary_provider,
                "sync_type": "fx_rates_pull",
                "started_at": started_at.isoformat(),
                "status": "running",
            }
        )
        sync_log_id = str(sync_log.get("id") or "")
        try:
            if not self.settings.fx_primary_api_key:
                raise BadRequestError("FX_PRIMARY_API_KEY is required for rate pull")
            pulled_rows = self._pull_primary_rates()
            created = self.repository.upsert_rates(pulled_rows)
            self.repository.refresh_fx_exposure()
            if sync_log_id:
                self.repository.update_sync_log(
                    sync_log_id,
                    {
                        "completed_at": self._now_utc().isoformat(),
                        "records_processed": len(pulled_rows),
                        "records_created": len(created),
                        "records_updated": 0,
                        "status": "success",
                    },
                )
            return FxManualRunResult(
                run_id=sync_log_id or "n/a",
                status="success",
                records_processed=len(pulled_rows),
                records_created=len(created),
                message="FX rates pulled and exposure refreshed",
            )
        except Exception as exc:
            if sync_log_id:
                self.repository.update_sync_log(
                    sync_log_id,
                    {
                        "completed_at": self._now_utc().isoformat(),
                        "status": "failed",
                        "error_message": str(exc),
                    },
                )
            raise

    def _pull_primary_rates(self) -> List[Dict[str, object]]:
        provider = self.settings.fx_primary_provider.strip().lower()
        if provider != "twelve_data":
            raise BadRequestError("Unsupported FX primary provider")

        base_currency = self.settings.fx_base_currency.strip().upper()
        if base_currency != "USD":
            raise BadRequestError("FX_BASE_CURRENCY must be USD for the v1 policy")
        target_currencies = self._target_currencies()
        if not target_currencies:
            raise BadRequestError("FX_TARGET_CURRENCIES must include AUD, NZD, or ZAR")

        rows: List[Dict[str, object]] = []
        for target in target_currencies:
            pair = f"{base_currency}/{target}"
            data = self._fetch_twelve_data_pair(pair)
            rate = self._to_decimal(data.get("rate"))
            timestamp = data.get("timestamp") or self._now_utc().isoformat()
            if rate is None:
                continue
            rows.append(
                {
                    "currency_pair": pair,
                    "rate_timestamp": timestamp,
                    "mid_rate": rate,
                    "bid_rate": data.get("bid"),
                    "ask_rate": data.get("ask"),
                    "source": "twelve_data",
                }
            )
        if not rows:
            raise BadRequestError("Primary FX provider returned no usable rates")
        return rows

    def _fetch_twelve_data_pair(self, pair: str) -> Dict[str, object]:
        base_url = self.settings.fx_primary_base_url.rstrip("/")
        retries = max(self.settings.fx_max_pull_retries, 1)
        params = {"symbol": pair, "apikey": self.settings.fx_primary_api_key or ""}
        last_error: Optional[Exception] = None
        for _ in range(retries):
            try:
                response = httpx.get(
                    f"{base_url}/exchange_rate",
                    params=params,
                    timeout=20.0,
                )
                response.raise_for_status()
                payload = response.json()
                rate_value = payload.get("rate")
                if rate_value is None:
                    raise ValueError("Missing rate in provider payload")
                parsed_rate = self._to_decimal(rate_value)
                if parsed_rate is None:
                    raise ValueError("Invalid rate in provider payload")
                return {
                    "rate": parsed_rate,
                    "timestamp": self._now_utc().isoformat(),
                    "bid": None,
                    "ask": None,
                }
            except Exception as exc:
                last_error = exc
                continue
        raise BadRequestError(f"Failed to fetch rate for {pair}: {last_error}")

    def get_exposure(self) -> List[FxExposure]:
        records = self.repository.list_exposure()
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

    def get_invoice_pressure(self) -> List[FxInvoicePressure]:
        records = self.repository.list_invoice_pressure()
        return [
            FxInvoicePressure(
                currency_code=r.currency_code,
                due_7d_amount=r.due_7d_amount,
                due_30d_amount=r.due_30d_amount,
                due_60d_amount=r.due_60d_amount,
                due_90d_amount=r.due_90d_amount,
                invoices_due_30d_count=r.invoices_due_30d_count,
                next_due_date=r.next_due_date,
            )
            for r in records
        ]

    def run_signals(self, request: FxSignalRunRequest) -> FxManualRunResult:
        target_currencies = self._target_currencies()
        run = self.repository.create_signal_run(
            {
                "run_type": request.run_type,
                "status": "running",
                "rates_source": self.settings.fx_primary_provider,
                "target_currencies": target_currencies,
                "calculation_version": "v1",
                "metadata": {"baseCurrency": self.settings.fx_base_currency},
            }
        )

        try:
            if not target_currencies:
                raise BadRequestError("No supported target currencies configured for signal run")

            refresh_result = self.repository.refresh_fx_exposure()
            if str(refresh_result.get("status", "")).lower() != "ok":
                raise BadRequestError("Exposure refresh failed before signal generation")

            generated_at = self._now_utc()
            expires_at = generated_at + timedelta(hours=24)
            exposure_rows = {row.currency_code: row for row in self.repository.list_exposure()}
            pressure_rows = {row.currency_code: row for row in self.repository.list_invoice_pressure()}
            prerequisite_failures: List[str] = []

            recent_rates_by_currency: Dict[str, List[FxRate]] = {}
            for currency in target_currencies:
                currency_rates = self.repository.list_recent_rates_for_pair(f"USD/{currency}", days_back=30)
                recent_rates_by_currency[currency] = [
                    FxRate(
                        id=row.id,
                        currency_pair=row.currency_pair,
                        rate_timestamp=row.rate_timestamp,
                        bid_rate=row.bid_rate,
                        ask_rate=row.ask_rate,
                        mid_rate=row.mid_rate,
                        source=row.source,
                        created_at=row.created_at,
                    )
                    for row in currency_rates
                ]
                valid_rate_points = [r for r in recent_rates_by_currency[currency] if r.mid_rate is not None]
                if len(valid_rate_points) < MIN_SIGNAL_RATE_HISTORY_POINTS:
                    prerequisite_failures.append(
                        f"{currency}: insufficient rate history ({len(valid_rate_points)} points)"
                    )
                latest_rate_ts = valid_rate_points[0].rate_timestamp if valid_rate_points else None
                if self._is_stale_timestamp(latest_rate_ts):
                    prerequisite_failures.append(f"{currency}: stale or missing latest rate")

                exposure_row = exposure_rows.get(currency)
                if exposure_row is None:
                    prerequisite_failures.append(f"{currency}: missing exposure row")
                elif exposure_row.net_exposure is None:
                    prerequisite_failures.append(f"{currency}: missing net exposure")

            if prerequisite_failures:
                self.repository.update_signal_run(
                    run.id,
                    {
                        "status": "skipped",
                        "completed_at": self._now_utc().isoformat(),
                        "records_processed": 0,
                        "signals_generated": 0,
                        "metadata": {
                            "baseCurrency": self.settings.fx_base_currency,
                            "refreshedAt": refresh_result.get("refreshed_at"),
                            "prerequisiteFailures": prerequisite_failures,
                        },
                    },
                )
                return FxManualRunResult(
                    run_id=run.id,
                    status="skipped",
                    records_processed=0,
                    records_created=0,
                    message="Signal run skipped due to failed prerequisites",
                )

            inserted_rows: List[Dict[str, object]] = []
            for currency in target_currencies:
                signal_payload = self._build_signal_payload(
                    currency=currency,
                    generated_at=generated_at,
                    expires_at=expires_at,
                    run_id=run.id,
                    exposure_row=exposure_rows.get(currency),
                    pressure_row=pressure_rows.get(currency),
                    recent_rates=recent_rates_by_currency.get(currency, []),
                )
                inserted_rows.append(signal_payload)

            created_signals = self.repository.insert_signals(inserted_rows)
            self.repository.update_signal_run(
                run.id,
                {
                    "status": "success",
                    "completed_at": self._now_utc().isoformat(),
                    "records_processed": len(inserted_rows),
                    "signals_generated": len(created_signals),
                    "metadata": {
                        "baseCurrency": self.settings.fx_base_currency,
                        "refreshedAt": refresh_result.get("refreshed_at"),
                        "prerequisiteFailures": [],
                    },
                },
            )
            return FxManualRunResult(
                run_id=run.id,
                status="success",
                records_processed=len(inserted_rows),
                records_created=len(created_signals),
                message="FX signals generated",
            )
        except Exception as exc:
            self.repository.update_signal_run(
                run.id,
                {
                    "status": "failed",
                    "completed_at": self._now_utc().isoformat(),
                    "error_message": str(exc),
                },
            )
            raise

    def _build_signal_payload(
        self,
        *,
        currency: str,
        generated_at: datetime,
        expires_at: datetime,
        run_id: str,
        exposure_row: Optional[FxExposureRecord],
        pressure_row: Optional[FxInvoicePressureRecord],
        recent_rates: List[FxRate],
    ) -> Dict[str, object]:
        rates = recent_rates
        current_rate = rates[0].mid_rate if rates else None
        avg_30d_rate = None
        if rates:
            values = [r.mid_rate for r in rates if r.mid_rate is not None]
            if values:
                avg_30d_rate = sum(values) / Decimal(len(values))

        exposure_amount = exposure_row.net_exposure if exposure_row else Decimal("0")
        exposure_30d_amount = Decimal("0")
        if exposure_row:
            exposure_30d_amount = (
                (exposure_row.confirmed_30d or Decimal("0"))
                + (exposure_row.estimated_30d or Decimal("0"))
            )
        invoice_pressure_30d = pressure_row.due_30d_amount if pressure_row else Decimal("0")
        invoice_pressure_60d = pressure_row.due_60d_amount if pressure_row else Decimal("0")
        invoice_pressure_90d = pressure_row.due_90d_amount if pressure_row else Decimal("0")

        gap_pct = Decimal("0")
        if current_rate is not None and avg_30d_rate not in {None, Decimal("0")}:
            gap_pct = (avg_30d_rate - current_rate) / avg_30d_rate

        should_buy = (
            exposure_amount is not None
            and exposure_amount > 0
            and current_rate is not None
            and avg_30d_rate is not None
            and (gap_pct >= Decimal("0.01") or invoice_pressure_30d > 0)
        )

        confidence = Decimal("0.55")
        if should_buy:
            confidence = Decimal("0.72")
            if gap_pct >= Decimal("0.02"):
                confidence = Decimal("0.82")

        recommended_amount = Decimal("0")
        if should_buy and exposure_amount is not None:
            recommended_amount = max(
                Decimal("0"),
                min(
                    exposure_amount,
                    max(invoice_pressure_30d, exposure_30d_amount),
                ),
            )

        signal_type = "buy_now" if should_buy else "wait"
        signal_strength = "low"
        if confidence >= Decimal("0.8"):
            signal_strength = "high"
        elif confidence >= Decimal("0.65"):
            signal_strength = "medium"

        reason_summary = (
            f"{currency}: favorable pricing vs 30d average with upcoming payable pressure."
            if should_buy
            else f"{currency}: wait for stronger pricing signal or clearer near-term pressure."
        )
        reasoning = (
            f"Current rate={current_rate}, avg30d={avg_30d_rate}, gapPct={gap_pct}, "
            f"netExposure={exposure_amount}, invoicePressure30d={invoice_pressure_30d}."
        )

        intelligence, _ = self.repository.list_intelligence(limit=5, currency_code=currency)
        trend_tags: List[str] = []
        source_links: List[str] = []
        for item in intelligence:
            for tag in item.trend_tags:
                normalized = tag.strip()
                if normalized and normalized not in trend_tags:
                    trend_tags.append(normalized)
            if item.source_url and item.source_url not in source_links:
                source_links.append(item.source_url)
            if len(trend_tags) >= 5 and len(source_links) >= 5:
                break

        return {
            "currency_code": currency,
            "signal_type": signal_type,
            "signal_strength": signal_strength,
            "current_rate": current_rate,
            "avg_30d_rate": avg_30d_rate,
            "exposure_amount": exposure_amount,
            "recommended_amount": recommended_amount,
            "reasoning": reasoning,
            "generated_at": generated_at.isoformat(),
            "expires_at": expires_at.isoformat(),
            "was_acted_on": False,
            "run_id": run_id,
            "confidence": confidence,
            "reason_summary": reason_summary,
            "trend_tags": trend_tags[:5],
            "source_links": source_links[:5],
            "exposure_30d_amount": exposure_30d_amount,
            "invoice_pressure_30d": invoice_pressure_30d,
            "invoice_pressure_60d": invoice_pressure_60d,
            "invoice_pressure_90d": invoice_pressure_90d,
            "metadata": {
                "gapPct": float(gap_pct),
                "baseCurrency": self.settings.fx_base_currency,
            },
        }

    def get_signals(
        self,
        *,
        page: int = 1,
        page_size: int = 25,
        include_totals: bool = False,
        currency_code: Optional[str] = None,
    ) -> Tuple[List[FxSignal], int]:
        offset = (page - 1) * page_size
        records, total_count = self.repository.list_signals(
            limit=page_size,
            currency_code=currency_code,
            offset=offset,
            include_totals=include_totals,
        )
        items = [
            FxSignal(
                id=r.id,
                currency_code=r.currency_code,
                signal_type=r.signal_type,  # type: ignore[arg-type]
                signal_strength=r.signal_strength,  # type: ignore[arg-type]
                current_rate=r.current_rate,
                avg_30d_rate=r.avg_30d_rate,
                exposure_amount=r.exposure_amount,
                recommended_amount=r.recommended_amount,
                reasoning=r.reasoning,
                generated_at=r.generated_at,
                expires_at=r.expires_at,
                was_acted_on=r.was_acted_on,
                run_id=r.run_id,
                confidence=r.confidence,
                reason_summary=r.reason_summary,
                trend_tags=r.trend_tags,
                source_links=r.source_links,
                exposure_30d_amount=r.exposure_30d_amount,
                invoice_pressure_30d=r.invoice_pressure_30d,
                invoice_pressure_60d=r.invoice_pressure_60d,
                invoice_pressure_90d=r.invoice_pressure_90d,
                metadata=r.metadata,
                created_at=r.created_at,
                updated_at=r.updated_at,
            )
            for r in records
        ]
        return items, total_count

    def get_transactions(
        self,
        *,
        page: int = 1,
        page_size: int = 100,
        include_totals: bool = False,
        currency_code: Optional[str] = None,
        transaction_type: Optional[str] = None,
    ) -> Tuple[List[FxTransaction], int]:
        offset = (page - 1) * page_size
        records, total_count = self.repository.list_transactions(
            limit=page_size,
            currency_code=currency_code,
            transaction_type=transaction_type,
            offset=offset,
            include_totals=include_totals,
        )
        return [FxTransaction(**record.model_dump()) for record in records], total_count

    def create_transaction(self, payload: FxTransactionCreateRequest) -> FxTransaction:
        normalized_currency = payload.currency_code.strip().upper()
        if normalized_currency not in SUPPORTED_LEDGER_CURRENCIES:
            raise BadRequestError(
                "Unsupported currency code for FX ledger. Allowed: USD, AUD, NZD, ZAR"
            )
        normalized_type = payload.transaction_type
        amount = payload.amount
        if normalized_type == "BUY" and amount <= 0:
            raise BadRequestError("BUY transactions must have a positive amount")
        if normalized_type == "SPEND":
            amount = -abs(amount)
        if normalized_type == "ADJUSTMENT" and amount == 0:
            raise BadRequestError("ADJUSTMENT transaction amount cannot be zero")

        if normalized_type == "SPEND" and not self.settings.fx_allow_negative_balance:
            holdings = self.repository.list_holdings(currency_code=normalized_currency)
            current_balance = holdings[0].balance_amount if holdings else Decimal("0")
            if current_balance is not None and current_balance + amount < 0:
                raise BadRequestError(
                    f"Insufficient {normalized_currency} balance for SPEND transaction"
                )

        usd_equivalent = payload.usd_equivalent
        if usd_equivalent is None and payload.exchange_rate:
            if payload.exchange_rate == 0:
                raise BadRequestError("Exchange rate cannot be zero")
            usd_equivalent = abs(amount) / payload.exchange_rate

        created = self.repository.create_transaction(
            {
                "currency_code": normalized_currency,
                "transaction_type": normalized_type,
                "transaction_date": payload.transaction_date.isoformat(),
                "amount": amount,
                "exchange_rate": payload.exchange_rate,
                "usd_equivalent": usd_equivalent,
                "supplier_invoice_id": payload.supplier_invoice_id,
                "signal_id": payload.signal_id,
                "reference_number": payload.reference_number,
                "notes": payload.notes,
                "entered_by": payload.entered_by,
            }
        )
        return FxTransaction(**created.model_dump())

    def get_holdings(self, currency_code: Optional[str] = None) -> List[FxHolding]:
        records = self.repository.list_holdings(currency_code=currency_code)
        return [FxHolding(**record.model_dump()) for record in records]
