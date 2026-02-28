from __future__ import annotations

from datetime import date
from typing import List, Optional

from fastapi import APIRouter, Depends, Query
from fastapi.concurrency import run_in_threadpool

from src.api.dependencies import get_debt_service_service
from src.schemas.debt_service import (
    DebtCovenantSnapshot,
    DebtFacility,
    DebtOverviewResponse,
    DebtPaymentCreateRequest,
    DebtPaymentCreateResponse,
    DebtPaymentRecord,
    DebtScenarioResult,
    DebtScenarioSummary,
    DebtScenarioRunRequest,
    DebtSchedulePoint,
)
from src.services.debt_service_service import DebtServiceService
from src.shared.response import Meta, ResponseEnvelope


router = APIRouter(prefix="/debt-service", tags=["debt-service"])


@router.get("/overview")
async def debt_service_overview(
    service: DebtServiceService = Depends(get_debt_service_service),
) -> ResponseEnvelope[DebtOverviewResponse]:
    data = await run_in_threadpool(service.get_overview)
    return ResponseEnvelope(
        data=data,
        pagination=None,
        meta=Meta(
            as_of_date=date.today().isoformat(),
            source="v_debt_service_overview,debt_covenant_snapshots",
            time_window="90d",
            calculation_version="v1",
            currency="USD",
        ),
    )


@router.get("/facilities")
async def debt_service_facilities(
    service: DebtServiceService = Depends(get_debt_service_service),
) -> ResponseEnvelope[List[DebtFacility]]:
    data = await run_in_threadpool(service.list_facilities)
    return ResponseEnvelope(
        data=data,
        pagination=None,
        meta=Meta(
            as_of_date=date.today().isoformat(),
            source="debt_facilities,debt_facility_terms",
            time_window="na",
            calculation_version="v1",
            currency="USD",
        ),
    )


@router.get("/schedule")
async def debt_service_schedule(
    facility_id: str = Query(alias="facility_id"),
    start_date: Optional[date] = Query(default=None, alias="start_date"),
    end_date: Optional[date] = Query(default=None, alias="end_date"),
    service: DebtServiceService = Depends(get_debt_service_service),
) -> ResponseEnvelope[List[DebtSchedulePoint]]:
    data = await run_in_threadpool(service.get_schedule, facility_id, start_date, end_date)
    return ResponseEnvelope(
        data=data,
        pagination=None,
        meta=Meta(
            as_of_date=date.today().isoformat(),
            source="debt_payment_schedule",
            time_window="full",
            calculation_version="v1",
            currency="USD",
        ),
    )


@router.get("/payments")
async def debt_service_payments(
    facility_id: str = Query(alias="facility_id"),
    service: DebtServiceService = Depends(get_debt_service_service),
) -> ResponseEnvelope[List[DebtPaymentRecord]]:
    data = await run_in_threadpool(service.list_payments, facility_id)
    return ResponseEnvelope(
        data=data,
        pagination=None,
        meta=Meta(
            as_of_date=date.today().isoformat(),
            source="debt_payments_actual",
            time_window="full",
            calculation_version="v1",
            currency="USD",
        ),
    )


@router.post("/payments")
async def debt_service_create_payment(
    payload: DebtPaymentCreateRequest,
    service: DebtServiceService = Depends(get_debt_service_service),
) -> ResponseEnvelope[DebtPaymentCreateResponse]:
    data = await run_in_threadpool(service.create_payment, payload)
    return ResponseEnvelope(
        data=data,
        pagination=None,
        meta=Meta(
            as_of_date=date.today().isoformat(),
            source="debt_payments_actual,debt_balance_snapshots",
            time_window="na",
            calculation_version="v1",
            currency="USD",
        ),
    )


@router.post("/scenarios/run")
async def debt_service_run_scenario(
    payload: DebtScenarioRunRequest,
    service: DebtServiceService = Depends(get_debt_service_service),
) -> ResponseEnvelope[DebtScenarioResult]:
    data = await run_in_threadpool(service.run_scenario, payload)
    return ResponseEnvelope(
        data=data,
        pagination=None,
        meta=Meta(
            as_of_date=date.today().isoformat(),
            source="debt_scenarios,debt_scenario_events,debt_payment_schedule",
            time_window="full",
            calculation_version="v1",
            currency="USD",
        ),
    )


@router.get("/scenarios")
async def debt_service_scenarios(
    facility_id: str = Query(alias="facility_id"),
    service: DebtServiceService = Depends(get_debt_service_service),
) -> ResponseEnvelope[List[DebtScenarioSummary]]:
    data = await run_in_threadpool(service.list_scenarios, facility_id)
    return ResponseEnvelope(
        data=data,
        pagination=None,
        meta=Meta(
            as_of_date=date.today().isoformat(),
            source="debt_scenarios,debt_scenario_events",
            time_window="full",
            calculation_version="v1",
            currency="USD",
        ),
    )


@router.get("/covenants")
async def debt_service_covenants(
    service: DebtServiceService = Depends(get_debt_service_service),
) -> ResponseEnvelope[List[DebtCovenantSnapshot]]:
    data = await run_in_threadpool(service.list_covenant_snapshots)
    return ResponseEnvelope(
        data=data,
        pagination=None,
        meta=Meta(
            as_of_date=date.today().isoformat(),
            source="debt_covenants,debt_covenant_snapshots,v_debt_service_overview",
            time_window="na",
            calculation_version="v1",
            currency="USD",
        ),
    )
