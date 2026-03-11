from __future__ import annotations

from collections.abc import Callable

from fastapi import Depends, Request

from src.api.dependencies import get_auth_access_service
from src.core.auth import AuthUser, get_authenticated_user
from src.core.errors import ForbiddenError
from src.schemas.auth_access import AuthenticatedUserAccess
from src.services.auth_access_service import AuthAccessService, AuthPrincipal

AUTH_USER_DEP = Depends(get_authenticated_user)
AUTH_ACCESS_SERVICE_DEP = Depends(get_auth_access_service)


def _build_principal(user: AuthUser) -> AuthPrincipal:
    return AuthPrincipal(user_id=user.user_id, email=user.email)


async def get_current_user_access(
    auth_user: AuthUser = AUTH_USER_DEP,
    service: AuthAccessService = AUTH_ACCESS_SERVICE_DEP,
) -> AuthenticatedUserAccess:
    return service.get_authenticated_user_access(_build_principal(auth_user))


CURRENT_USER_ACCESS_DEP = Depends(get_current_user_access)


def require_permission(
    permission_key: str,
) -> Callable[[AuthenticatedUserAccess], AuthenticatedUserAccess]:
    async def _dependency(
        access: AuthenticatedUserAccess = CURRENT_USER_ACCESS_DEP,
        service: AuthAccessService = AUTH_ACCESS_SERVICE_DEP,
    ) -> AuthenticatedUserAccess:
        service.require_permission(access, permission_key)
        return access

    return _dependency


async def require_admin(
    access: AuthenticatedUserAccess = CURRENT_USER_ACCESS_DEP,
) -> AuthenticatedUserAccess:
    if access.role != "admin" or not access.is_active:
        raise ForbiddenError("Admin access required")
    return access


async def require_marketing_permission(
    request: Request,
    access: AuthenticatedUserAccess = CURRENT_USER_ACCESS_DEP,
    service: AuthAccessService = AUTH_ACCESS_SERVICE_DEP,
) -> AuthenticatedUserAccess:
    path = request.url.path
    if "/search-console" in path:
        service.require_permission(access, "search_console_insights")
    else:
        service.require_permission(access, "marketing_web_analytics")
    return access

