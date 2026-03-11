from __future__ import annotations

from datetime import date

from fastapi import APIRouter, Depends

from src.api.authz import get_current_user_access, require_admin
from src.api.dependencies import get_auth_access_service
from src.schemas.auth_access import (
    AuthenticatedUserAccess,
    UserAccessSummary,
    UserAccessUpdateRequest,
)
from src.services.auth_access_service import AuthAccessService
from src.shared.response import Meta, ResponseEnvelope

router = APIRouter(prefix="/settings/user-access", tags=["settings-user-access"])
ADMIN_DEP = Depends(require_admin)
CURRENT_USER_ACCESS_DEP = Depends(get_current_user_access)
AUTH_ACCESS_SERVICE_DEP = Depends(get_auth_access_service)


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


@router.get("")
async def list_user_access(
    _: AuthenticatedUserAccess = ADMIN_DEP,
    service: AuthAccessService = AUTH_ACCESS_SERVICE_DEP,
) -> ResponseEnvelope[list[UserAccessSummary]]:
    rows = service.list_user_access()
    return ResponseEnvelope(data=rows, pagination=None, meta=_meta("user_access_summary_v1"))


@router.get("/{user_id}")
async def get_user_access(
    user_id: str,
    _: AuthenticatedUserAccess = ADMIN_DEP,
    service: AuthAccessService = AUTH_ACCESS_SERVICE_DEP,
) -> ResponseEnvelope[UserAccessSummary]:
    row = service.get_user_access(user_id)
    return ResponseEnvelope(data=row, pagination=None, meta=_meta("user_access_summary_v1"))


@router.put("/{user_id}")
async def update_user_access(
    user_id: str,
    request: UserAccessUpdateRequest,
    actor: AuthenticatedUserAccess = CURRENT_USER_ACCESS_DEP,
    _: AuthenticatedUserAccess = ADMIN_DEP,
    service: AuthAccessService = AUTH_ACCESS_SERVICE_DEP,
) -> ResponseEnvelope[UserAccessSummary]:
    row = service.update_user_access(actor=actor, user_id=user_id, request=request)
    return ResponseEnvelope(data=row, pagination=None, meta=_meta("user_access_summary_v1"))


@router.post("/{user_id}/deactivate")
async def deactivate_user_access(
    user_id: str,
    actor: AuthenticatedUserAccess = CURRENT_USER_ACCESS_DEP,
    _: AuthenticatedUserAccess = ADMIN_DEP,
    service: AuthAccessService = AUTH_ACCESS_SERVICE_DEP,
) -> ResponseEnvelope[UserAccessSummary]:
    row = service.update_user_access(
        actor=actor,
        user_id=user_id,
        request=UserAccessUpdateRequest(is_active=False, reason="deactivated_by_admin"),
    )
    return ResponseEnvelope(data=row, pagination=None, meta=_meta("user_access_summary_v1"))


@router.post("/{user_id}/reactivate")
async def reactivate_user_access(
    user_id: str,
    actor: AuthenticatedUserAccess = CURRENT_USER_ACCESS_DEP,
    _: AuthenticatedUserAccess = ADMIN_DEP,
    service: AuthAccessService = AUTH_ACCESS_SERVICE_DEP,
) -> ResponseEnvelope[UserAccessSummary]:
    row = service.update_user_access(
        actor=actor,
        user_id=user_id,
        request=UserAccessUpdateRequest(is_active=True, reason="reactivated_by_admin"),
    )
    return ResponseEnvelope(data=row, pagination=None, meta=_meta("user_access_summary_v1"))

