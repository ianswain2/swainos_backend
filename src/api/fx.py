from __future__ import annotations

from datetime import date
from typing import List

from fastapi import APIRouter, Depends, Query

from src.api.dependencies import get_fx_service
from src.schemas.fx import FxExposure, FxRate
from src.services.fx_service import FxService
from src.shared.response import Meta, ResponseEnvelope


router = APIRouter(prefix="/fx", tags=["fx"])


@router.get("/rates")
def fx_rates(
    limit: int = Query(default=50, ge=1, le=200),
    service: FxService = Depends(get_fx_service),
) -> ResponseEnvelope[List[FxRate]]:
    data = service.get_rates(limit=limit)
    meta = Meta(
        as_of_date=date.today().isoformat(),
        source="supabase",
        time_window="",
        calculation_version="v1",
        currency=None,
    )
    return ResponseEnvelope(data=data, pagination=None, meta=meta)


@router.get("/exposure")
def fx_exposure(
    service: FxService = Depends(get_fx_service),
) -> ResponseEnvelope[List[FxExposure]]:
    data = service.get_exposure()
    meta = Meta(
        as_of_date=date.today().isoformat(),
        source="mv_fx_exposure",
        time_window="",
        calculation_version="v1",
        currency=None,
    )
    return ResponseEnvelope(data=data, pagination=None, meta=meta)
