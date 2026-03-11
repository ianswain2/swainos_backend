from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import Field

from src.shared.base import BaseSchema

AppRole = Literal["admin", "member"]
ModulePermissionKey = Literal[
    "command_center",
    "ai_insights",
    "itinerary_forecast",
    "itinerary_actuals",
    "destination",
    "travel_consultant",
    "travel_agencies",
    "marketing_web_analytics",
    "search_console_insights",
    "cash_flow",
    "debt_service",
    "fx_command",
    "operations",
    "settings_job_controls",
    "settings_run_logs",
    "settings_user_access",
]


class AuthenticatedUserAccess(BaseSchema):
    user_id: str
    email: str
    role: AppRole
    is_admin: bool
    is_active: bool
    permission_keys: list[ModulePermissionKey] = Field(default_factory=list)
    can_manage_access: bool


class UserAccessSummary(BaseSchema):
    user_id: str
    email: str
    role: AppRole
    is_active: bool
    permission_keys: list[ModulePermissionKey] = Field(default_factory=list)
    created_at: datetime
    updated_at: datetime


class UserAccessUpdateRequest(BaseSchema):
    role: AppRole | None = None
    is_active: bool | None = None
    permission_keys: list[ModulePermissionKey] | None = None
    reason: str | None = None

