from __future__ import annotations

from typing import Any, Dict, Optional

from fastapi import Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel


class ErrorDetail(BaseModel):
    code: str
    message: str
    details: Optional[Dict[str, Any]] = None


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


class ErrorEnvelope(BaseModel):
    error: ErrorDetail


def app_error_handler(_: Request, exc: AppError) -> JSONResponse:
    envelope = ErrorEnvelope(error=ErrorDetail(code=exc.code, message=exc.message))
    return JSONResponse(status_code=exc.status_code, content=envelope.model_dump())


def validation_error_handler(_: Request, exc: RequestValidationError) -> JSONResponse:
    envelope = ErrorEnvelope(
        error=ErrorDetail(
            code="validation_error",
            message="Validation error",
            details={"errors": exc.errors()},
        )
    )
    return JSONResponse(status_code=422, content=envelope.model_dump())
