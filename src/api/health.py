from __future__ import annotations

from datetime import date

from fastapi import APIRouter

from src.shared.response import Meta, ResponseEnvelope


router = APIRouter(tags=["health"])


@router.get("/health")
def health_check() -> ResponseEnvelope[dict]:
    meta = Meta(
        as_of_date=date.today().isoformat(),
        source="system",
        time_window="now",
        calculation_version="v1",
    )
    return ResponseEnvelope(data={"status": "ok"}, meta=meta)


@router.get("/healthz")
def health_check_liveness() -> ResponseEnvelope[dict]:
    meta = Meta(
        as_of_date=date.today().isoformat(),
        source="system",
        time_window="now",
        calculation_version="v1",
    )
    return ResponseEnvelope(data={"status": "ok"}, meta=meta)
