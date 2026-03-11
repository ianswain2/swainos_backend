from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import Any
from urllib.parse import urlencode

from src.core.config import get_settings
from src.core.supabase import SupabaseClient
from src.schemas.auth_access import UserAccessSummary


class AuthAccessRepository:
    def __init__(self) -> None:
        self.client = SupabaseClient()
        self.settings = get_settings()
        self.logger = logging.getLogger(__name__)

    @staticmethod
    def _now_iso() -> str:
        return datetime.now(UTC).isoformat()

    def get_user_access_by_user_id(self, user_id: str) -> UserAccessSummary | None:
        rows, _ = self.client.select(
            table="user_access_summary_v1",
            select="user_id,email,role,is_active,permission_keys,created_at,updated_at",
            filters=[("user_id", f"eq.{user_id}")],
            limit=1,
        )
        if not rows:
            return None
        return UserAccessSummary.model_validate(rows[0])

    def list_user_access(self) -> list[UserAccessSummary]:
        rows, _ = self.client.select(
            table="user_access_summary_v1",
            select="user_id,email,role,is_active,permission_keys,created_at,updated_at",
            order="email.asc",
            limit=500,
        )
        return [UserAccessSummary.model_validate(row) for row in rows]

    def ensure_user_profile_exists(self, *, user_id: str, email: str) -> None:
        existing_rows, _ = self.client.select(
            table="user_profiles",
            select="user_id",
            filters=[("user_id", f"eq.{user_id}")],
            limit=1,
        )
        if existing_rows:
            return
        self.client.insert(
            table="user_profiles",
            payload={
                "user_id": user_id,
                "email": email.strip().lower(),
                "role": "member",
                "is_active": True,
                "created_by": user_id,
                "updated_by": user_id,
            },
            upsert=True,
            on_conflict="user_id",
        )

    def sync_auth_users_into_profiles(self) -> None:
        try:
            auth_users = self._list_auth_users()
            if not auth_users:
                return
            profile_rows, _ = self.client.select(
                table="user_profiles",
                select="user_id",
                limit=2000,
            )
            existing_user_ids = {
                str(row.get("user_id"))
                for row in profile_rows
                if row.get("user_id")
            }
            missing_rows = [
                {
                    "user_id": user_id,
                    "email": email,
                    "role": "member",
                    "is_active": True,
                }
                for user_id, email in auth_users
                if user_id not in existing_user_ids
            ]
            if not missing_rows:
                return
            self.client.insert(
                table="user_profiles",
                payload=missing_rows,
                upsert=True,
                on_conflict="user_id",
            )
        except Exception:
            # Keep existing admin operations available even if auth-user sync is unavailable.
            self.logger.exception("sync_auth_users_into_profiles_failed")

    def upsert_user_profile(
        self,
        *,
        user_id: str,
        email: str,
        role: str,
        is_active: bool,
        actor_user_id: str,
    ) -> None:
        existing_rows, _ = self.client.select(
            table="user_profiles",
            select="user_id",
            filters=[("user_id", f"eq.{user_id}")],
            limit=1,
        )
        if existing_rows:
            self.client.update(
                table="user_profiles",
                payload={
                    "email": email,
                    "role": role,
                    "is_active": is_active,
                    "updated_by": actor_user_id,
                    "updated_at": self._now_iso(),
                },
                filters=[("user_id", f"eq.{user_id}")],
            )
            return
        self.client.insert(
            table="user_profiles",
            payload={
                "user_id": user_id,
                "email": email,
                "role": role,
                "is_active": is_active,
                "created_by": actor_user_id,
                "updated_by": actor_user_id,
            },
        )

    def replace_user_permissions(
        self,
        *,
        user_id: str,
        permission_keys: list[str],
        actor_user_id: str,
    ) -> None:
        self._delete_user_permissions(user_id)
        if not permission_keys:
            return
        rows: list[dict[str, Any]] = [
            {
                "user_id": user_id,
                "permission_key": permission_key,
                "created_by": actor_user_id,
            }
            for permission_key in permission_keys
        ]
        self.client.insert(
            table="user_module_permissions",
            payload=rows,
            upsert=True,
            on_conflict="user_id,permission_key",
        )

    def _delete_user_permissions(self, user_id: str) -> None:
        params = [("user_id", f"eq.{user_id}")]
        url = f"{self.client.base_url}/user_module_permissions?{urlencode(params, doseq=True)}"
        headers = {
            "apikey": self.client.api_key,
            "Authorization": f"Bearer {self.client.api_key}",
        }
        response = self.client._client.delete(url, headers=headers)
        response.raise_for_status()

    def _list_auth_users(self) -> list[tuple[str, str]]:
        users: list[tuple[str, str]] = []
        page = 1
        per_page = 200
        headers = {
            "apikey": self.settings.supabase_service_role_key or self.client.api_key,
            "Authorization": (
                f"Bearer {self.settings.supabase_service_role_key or self.client.api_key}"
            ),
        }
        while True:
            url = (
                self.settings.supabase_url.rstrip("/")
                + f"/auth/v1/admin/users?page={page}&per_page={per_page}"
            )
            response = self.client._client.get(url, headers=headers, timeout=30.0)
            response.raise_for_status()
            payload = response.json()
            page_users = payload.get("users", []) if isinstance(payload, dict) else []
            if not isinstance(page_users, list) or not page_users:
                break
            for row in page_users:
                if not isinstance(row, dict):
                    continue
                user_id = str(row.get("id") or "")
                email = str(row.get("email") or "").strip().lower()
                if not user_id or not email:
                    continue
                users.append((user_id, email))
            if len(page_users) < per_page:
                break
            page += 1
        return users

