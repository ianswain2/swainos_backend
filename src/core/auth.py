from __future__ import annotations

from dataclasses import dataclass

import httpx
from fastapi import Header

from src.core.config import get_settings
from src.core.errors import UnauthorizedError


@dataclass(frozen=True)
class AuthUser:
    user_id: str
    email: str
    access_token: str


def _extract_bearer_token(authorization: str | None) -> str:
    if not authorization:
        raise UnauthorizedError("Missing Authorization header")
    parts = authorization.split(" ", 1)
    if len(parts) != 2 or parts[0].lower() != "bearer" or not parts[1].strip():
        raise UnauthorizedError("Invalid Authorization header")
    return parts[1].strip()


async def verify_bearer_token(authorization: str | None) -> AuthUser:
    access_token = _extract_bearer_token(authorization)
    settings = get_settings()
    api_key = settings.supabase_anon_key or settings.supabase_service_role_key
    if not api_key:
        raise UnauthorizedError("Supabase API key is not configured")

    url = settings.supabase_url.rstrip("/") + "/auth/v1/user"
    headers = {
        "apikey": api_key,
        "Authorization": f"Bearer {access_token}",
    }

    async with httpx.AsyncClient(timeout=15.0) as client:
        try:
            response = await client.get(url, headers=headers)
        except httpx.HTTPError as exc:
            raise UnauthorizedError("Unable to verify session token") from exc
    if response.status_code != 200:
        raise UnauthorizedError("Invalid or expired session token")
    payload = response.json()
    user_id = str(payload.get("id") or "")
    email = str(payload.get("email") or "")
    if not user_id:
        raise UnauthorizedError("Invalid session token payload")
    return AuthUser(user_id=user_id, email=email, access_token=access_token)


async def get_authenticated_user(
    authorization: str | None = Header(default=None),
) -> AuthUser:
    return await verify_bearer_token(authorization)

