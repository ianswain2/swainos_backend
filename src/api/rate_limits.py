from __future__ import annotations

from fastapi import Request

from src.core.config import get_settings
from src.core.errors import TooManyRequestsError
from src.core.rate_limit import rate_limiter


def _request_identity(request: Request) -> str:
    for header_name in ("cf-connecting-ip", "x-forwarded-for", "x-real-ip"):
        value = request.headers.get(header_name)
        if value:
            return value.split(",")[0].strip()
    if request.client and request.client.host:
        return request.client.host
    return "unknown"


def enforce_expensive_run_limit(request: Request, scope: str) -> None:
    settings = get_settings()
    allowed = rate_limiter.allow(
        scope=scope,
        key=_request_identity(request),
        max_requests=settings.expensive_run_rate_limit_per_minute,
        window_seconds=60,
    )
    if not allowed:
        raise TooManyRequestsError("Run endpoint rate limit exceeded")


def enforce_mutation_limit(request: Request, scope: str) -> None:
    settings = get_settings()
    allowed = rate_limiter.allow(
        scope=scope,
        key=_request_identity(request),
        max_requests=settings.mutation_rate_limit_per_minute,
        window_seconds=60,
    )
    if not allowed:
        raise TooManyRequestsError("Mutation endpoint rate limit exceeded")
