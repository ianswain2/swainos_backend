from __future__ import annotations

from typing import Any

from fastapi import Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from src.core.request_context import get_request_id


class ErrorDetail(BaseModel):
    code: str
    message: str
    details: dict[str, Any] | None = None


class AppError(Exception):
    def __init__(self, code: str, message: str, status_code: int = 400) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.status_code = status_code


class NotFoundError(AppError):
    def __init__(self, message: str = "Resource not found") -> None:
        super().__init__(code="not_found", message=message, status_code=404)


class BadRequestError(AppError):
    def __init__(self, message: str = "Bad request") -> None:
        super().__init__(code="bad_request", message=message, status_code=400)


class UnauthorizedError(AppError):
    def __init__(self, message: str = "Authentication required") -> None:
        super().__init__(code="unauthorized", message=message, status_code=401)


class ForbiddenError(AppError):
    def __init__(self, message: str = "Forbidden") -> None:
        super().__init__(code="forbidden", message=message, status_code=403)


class ErrorEnvelope(BaseModel):
    error: ErrorDetail


def app_error_handler(_: Request, exc: AppError) -> JSONResponse:
    details = {"requestId": get_request_id()} if get_request_id() else None
    envelope = ErrorEnvelope(error=ErrorDetail(code=exc.code, message=exc.message, details=details))
    return JSONResponse(status_code=exc.status_code, content=envelope.model_dump())


def validation_error_handler(_: Request, exc: RequestValidationError) -> JSONResponse:
    details: dict[str, Any] = {"errors": exc.errors()}
    request_id = get_request_id()
    if request_id:
        details["requestId"] = request_id
    envelope = ErrorEnvelope(
        error=ErrorDetail(
            code="validation_error",
            message="Validation error",
            details=details,
        )
    )
    return JSONResponse(status_code=422, content=envelope.model_dump())
