from __future__ import annotations

from uuid import uuid4

from fastapi import FastAPI
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.trustedhost import TrustedHostMiddleware
from starlette.requests import Request

from src.api.router import api_router
from src.core.config import (
    get_cors_origins,
    get_settings,
    get_trusted_hosts,
    validate_runtime_settings,
)
from src.core.errors import AppError, app_error_handler, validation_error_handler
from src.core.logging import configure_logging
from src.core.request_context import set_request_id


def create_app() -> FastAPI:
    settings = get_settings()
    configure_logging(settings.log_level)
    validate_runtime_settings()

    app = FastAPI(title=settings.app_name)
    if settings.environment.strip().lower() == "production":
        app.add_middleware(
            TrustedHostMiddleware,
            allowed_hosts=get_trusted_hosts(),
        )
    app.add_middleware(
        CORSMiddleware,
        allow_origins=get_cors_origins(),
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    app.include_router(api_router, prefix=settings.api_prefix)

    app.add_exception_handler(AppError, app_error_handler)
    app.add_exception_handler(RequestValidationError, validation_error_handler)

    @app.middleware("http")
    async def request_context_middleware(request: Request, call_next):  # type: ignore[no-untyped-def]
        request_id = request.headers.get("x-request-id") or str(uuid4())
        set_request_id(request_id)
        try:
            response = await call_next(request)
        finally:
            set_request_id(None)
        response.headers["x-request-id"] = request_id
        return response

    return app


app = create_app()
