from __future__ import annotations

from datetime import date

from fastapi import APIRouter, Depends

from src.api.authz import get_current_user_access
from src.schemas.auth_access import AuthenticatedUserAccess
from src.shared.response import Meta, ResponseEnvelope

router = APIRouter(prefix="/auth", tags=["auth"])
CURRENT_USER_ACCESS_DEP = Depends(get_current_user_access)


def _meta(source: str) -> Meta:
    return Meta(
        as_of_date=date.today().isoformat(),
        source=source,
        time_window="",
        calculation_version="v1",
        data_status="live",
        is_stale=False,
        degraded=False,
    )


@router.get("/me")
async def auth_me(
    access: AuthenticatedUserAccess = CURRENT_USER_ACCESS_DEP,
) -> ResponseEnvelope[AuthenticatedUserAccess]:
    return ResponseEnvelope(data=access, pagination=None, meta=_meta("user_access_summary_v1"))

