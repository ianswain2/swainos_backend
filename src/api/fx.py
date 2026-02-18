from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from typing import List, Optional

from fastapi import APIRouter, Depends, Header, Query

from src.api.dependencies import get_fx_intelligence_service, get_fx_service
from src.core.config import get_settings
from src.core.errors import BadRequestError
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
from src.services.fx_intelligence_service import FxIntelligenceService
from src.services.fx_service import FxService
from src.shared.response import Meta, ResponseEnvelope, build_pagination


router = APIRouter(prefix="/fx", tags=["fx"])

FX_CALCULATION_VERSION = "v1"


def _is_stale(timestamp_value: Optional[datetime], stale_after_minutes: int) -> bool:
    if timestamp_value is None:
        return True
    return timestamp_value < datetime.now(timezone.utc) - timedelta(minutes=stale_after_minutes)


def _build_meta(
    *,
    source: str,
    data_status: str = "live",
    is_stale: bool = False,
    generated_at: Optional[datetime] = None,
) -> Meta:
    return Meta(
        as_of_date=date.today().isoformat(),
        source=source,
        time_window="",
        calculation_version=FX_CALCULATION_VERSION,
        currency=None,
        data_status=data_status,
        is_stale=is_stale,
        degraded=data_status in {"degraded", "partial"},
        generated_at=(generated_at or datetime.now(timezone.utc)).isoformat(),
    )


def _validate_manual_run_token(x_fx_run_token: Optional[str]) -> None:
    configured = (get_settings().fx_manual_run_token or "").strip()
    if not configured:
        raise BadRequestError("FX manual run endpoint is disabled")
    if not x_fx_run_token or x_fx_run_token != configured:
        raise BadRequestError("Invalid FX manual run token")


@router.get("/rates")
def fx_rates(
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=200),
    include_totals: bool = Query(default=False),
    service: FxService = Depends(get_fx_service),
) -> ResponseEnvelope[List[FxRate]]:
    data, total_count = service.get_rates(
        page=page,
        page_size=page_size,
        include_totals=include_totals,
    )
    latest_rate_timestamp = max((item.rate_timestamp for item in data if item.rate_timestamp), default=None)
    stale = _is_stale(latest_rate_timestamp, get_settings().fx_stale_after_minutes)
    data_status = "degraded" if stale else "live"
    meta = _build_meta(
        source="supabase",
        data_status=data_status,
        is_stale=stale,
        generated_at=latest_rate_timestamp,
    )
    pagination = build_pagination(page=page, page_size=page_size, total_items=total_count)
    return ResponseEnvelope(data=data, pagination=pagination, meta=meta)


@router.post("/rates/run")
def fx_rates_run(
    request: FxRatePullRunRequest,
    service: FxService = Depends(get_fx_service),
    x_fx_run_token: Optional[str] = Header(default=None),
) -> ResponseEnvelope[FxManualRunResult]:
    _validate_manual_run_token(x_fx_run_token)
    result = service.pull_rates(request)
    return ResponseEnvelope(
        data=result,
        pagination=None,
        meta=_build_meta(source="fx_rate_pull", data_status=result.status),
    )


@router.get("/exposure")
def fx_exposure(
    service: FxService = Depends(get_fx_service),
) -> ResponseEnvelope[List[FxExposure]]:
    data = service.get_exposure()
    latest_rate_timestamp = service.get_latest_rate_timestamp()
    stale = _is_stale(latest_rate_timestamp, get_settings().fx_stale_after_minutes)
    data_status = "degraded" if stale else "live"
    meta = _build_meta(
        source="mv_fx_exposure",
        data_status=data_status,
        is_stale=stale,
        generated_at=latest_rate_timestamp,
    )
    return ResponseEnvelope(data=data, pagination=None, meta=meta)


@router.get("/signals")
def fx_signals(
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=25, ge=1, le=200),
    include_totals: bool = Query(default=False),
    currency_code: Optional[str] = Query(default=None),
    service: FxService = Depends(get_fx_service),
) -> ResponseEnvelope[List[FxSignal]]:
    data, total_count = service.get_signals(
        page=page,
        page_size=page_size,
        include_totals=include_totals,
        currency_code=currency_code,
    )
    latest_generated_at = max((item.generated_at for item in data if item.generated_at), default=None)
    stale = _is_stale(latest_generated_at, get_settings().fx_stale_after_minutes)
    data_status = "partial" if (stale or not data) else "live"
    return ResponseEnvelope(
        data=data,
        pagination=build_pagination(page=page, page_size=page_size, total_items=total_count),
        meta=_build_meta(
            source="fx_signals",
            data_status=data_status,
            is_stale=stale,
            generated_at=latest_generated_at,
        ),
    )


@router.post("/signals/run")
def fx_signals_run(
    request: FxSignalRunRequest,
    service: FxService = Depends(get_fx_service),
    x_fx_run_token: Optional[str] = Header(default=None),
) -> ResponseEnvelope[FxManualRunResult]:
    _validate_manual_run_token(x_fx_run_token)
    result = service.run_signals(request)
    return ResponseEnvelope(
        data=result,
        pagination=None,
        meta=_build_meta(source="fx_signals_run", data_status=result.status),
    )


@router.get("/transactions")
def fx_transactions(
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=100, ge=1, le=500),
    include_totals: bool = Query(default=False),
    currency_code: Optional[str] = Query(default=None),
    transaction_type: Optional[str] = Query(default=None),
    service: FxService = Depends(get_fx_service),
) -> ResponseEnvelope[List[FxTransaction]]:
    data, total_count = service.get_transactions(
        page=page,
        page_size=page_size,
        include_totals=include_totals,
        currency_code=currency_code,
        transaction_type=transaction_type,
    )
    return ResponseEnvelope(
        data=data,
        pagination=build_pagination(page=page, page_size=page_size, total_items=total_count),
        meta=_build_meta(source="fx_transactions", data_status="live"),
    )


@router.post("/transactions")
def fx_transactions_create(
    request: FxTransactionCreateRequest,
    service: FxService = Depends(get_fx_service),
) -> ResponseEnvelope[FxTransaction]:
    created = service.create_transaction(request)
    return ResponseEnvelope(
        data=created,
        pagination=None,
        meta=_build_meta(source="fx_transactions", data_status="live"),
    )


@router.get("/holdings")
def fx_holdings(
    currency_code: Optional[str] = Query(default=None),
    service: FxService = Depends(get_fx_service),
) -> ResponseEnvelope[List[FxHolding]]:
    data = service.get_holdings(currency_code=currency_code)
    return ResponseEnvelope(
        data=data,
        pagination=None,
        meta=_build_meta(source="fx_holdings", data_status="live"),
    )


@router.get("/intelligence")
def fx_intelligence(
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=200),
    include_totals: bool = Query(default=False),
    currency_code: Optional[str] = Query(default=None),
    service: FxIntelligenceService = Depends(get_fx_intelligence_service),
) -> ResponseEnvelope[List[FxIntelligenceItem]]:
    data, total_count = service.list_intelligence(
        page=page,
        page_size=page_size,
        include_totals=include_totals,
        currency_code=currency_code,
    )
    latest_intelligence_at = max(
        ((item.published_at or item.created_at) for item in data if (item.published_at or item.created_at)),
        default=None,
    )
    stale = _is_stale(latest_intelligence_at, get_settings().fx_stale_after_minutes)
    status = "partial" if (stale or not data) else "live"
    return ResponseEnvelope(
        data=data,
        pagination=build_pagination(page=page, page_size=page_size, total_items=total_count),
        meta=_build_meta(
            source="fx_intelligence_items",
            data_status=status,
            is_stale=stale,
            generated_at=latest_intelligence_at,
        ),
    )


@router.post("/intelligence/run")
def fx_intelligence_run(
    request: FxIntelligenceRunRequest,
    service: FxIntelligenceService = Depends(get_fx_intelligence_service),
    x_fx_run_token: Optional[str] = Header(default=None),
) -> ResponseEnvelope[FxManualRunResult]:
    if not get_settings().fx_intelligence_on_demand_enabled:
        raise BadRequestError("FX intelligence on-demand runs are disabled")
    _validate_manual_run_token(x_fx_run_token)
    result = service.run_intelligence(request)
    return ResponseEnvelope(
        data=result,
        pagination=None,
        meta=_build_meta(source="fx_intelligence_run", data_status=result.status),
    )


@router.get("/invoice-pressure")
def fx_invoice_pressure(
    service: FxService = Depends(get_fx_service),
) -> ResponseEnvelope[List[FxInvoicePressure]]:
    data = service.get_invoice_pressure()
    status = "live" if data else "partial"
    return ResponseEnvelope(
        data=data,
        pagination=None,
        meta=_build_meta(source="fx_invoice_pressure_v1", data_status=status),
    )
