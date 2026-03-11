from __future__ import annotations

from dataclasses import dataclass

from src.core.errors import BadRequestError, ForbiddenError, NotFoundError
from src.repositories.auth_access_repository import AuthAccessRepository
from src.schemas.auth_access import (
    AuthenticatedUserAccess,
    UserAccessSummary,
    UserAccessUpdateRequest,
)


@dataclass(frozen=True)
class AuthPrincipal:
    user_id: str
    email: str


class AuthAccessService:
    def __init__(self, repository: AuthAccessRepository) -> None:
        self.repository = repository

    def get_authenticated_user_access(self, principal: AuthPrincipal) -> AuthenticatedUserAccess:
        summary = self.repository.get_user_access_by_user_id(principal.user_id)
        if not summary:
            if principal.email:
                self.repository.ensure_user_profile_exists(
                    user_id=principal.user_id,
                    email=principal.email,
                )
                summary = self.repository.get_user_access_by_user_id(principal.user_id)
        if not summary:
            raise ForbiddenError("User access profile not found")
        is_admin = summary.role == "admin" and summary.is_active
        return AuthenticatedUserAccess(
            user_id=summary.user_id,
            email=summary.email,
            role=summary.role,
            is_admin=is_admin,
            is_active=summary.is_active,
            permission_keys=summary.permission_keys,
            can_manage_access=is_admin,
        )

    def has_permission(self, access: AuthenticatedUserAccess, permission_key: str) -> bool:
        if not access.is_active:
            return False
        if access.role == "admin":
            return True
        return permission_key in access.permission_keys

    def require_permission(self, access: AuthenticatedUserAccess, permission_key: str) -> None:
        if not self.has_permission(access, permission_key):
            raise ForbiddenError("Permission denied")

    def list_user_access(self) -> list[UserAccessSummary]:
        self.repository.sync_auth_users_into_profiles()
        return self.repository.list_user_access()

    def get_user_access(self, user_id: str) -> UserAccessSummary:
        summary = self.repository.get_user_access_by_user_id(user_id)
        if not summary:
            raise NotFoundError("User access profile not found")
        return summary

    def update_user_access(
        self,
        *,
        actor: AuthenticatedUserAccess,
        user_id: str,
        request: UserAccessUpdateRequest,
    ) -> UserAccessSummary:
        if actor.role != "admin":
            raise ForbiddenError("Only admins can manage user access")
        existing = self.repository.get_user_access_by_user_id(user_id)
        if not existing:
            raise NotFoundError("User access profile not found")

        next_role = request.role or existing.role
        next_is_active = request.is_active if request.is_active is not None else existing.is_active
        next_permissions = (
            request.permission_keys
            if request.permission_keys is not None
            else existing.permission_keys
        )

        if existing.role == "admin" and next_role != "admin":
            admins = [
                row
                for row in self.repository.list_user_access()
                if row.role == "admin" and row.is_active
            ]
            if len(admins) <= 1:
                raise BadRequestError("Cannot remove the last active admin")
        if existing.role == "admin" and not next_is_active:
            admins = [
                row
                for row in self.repository.list_user_access()
                if row.role == "admin" and row.is_active
            ]
            if len(admins) <= 1:
                raise BadRequestError("Cannot deactivate the last active admin")

        self.repository.upsert_user_profile(
            user_id=user_id,
            email=existing.email,
            role=next_role,
            is_active=next_is_active,
            actor_user_id=actor.user_id,
        )
        self.repository.replace_user_permissions(
            user_id=user_id,
            permission_keys=list(dict.fromkeys(next_permissions)),
            actor_user_id=actor.user_id,
        )
        updated = self.repository.get_user_access_by_user_id(user_id)
        if not updated:
            raise NotFoundError("User access profile not found")
        return updated

