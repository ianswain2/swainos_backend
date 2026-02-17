from __future__ import annotations

from functools import lru_cache
from typing import Optional

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    # Ignore unrelated env keys so local/dev .env can include optional integrations.
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    app_name: str = "SwainOS Backend"
    environment: str = "development"
    api_prefix: str = "/api/v1"
    cors_allow_origins: str = "http://localhost:3000,http://127.0.0.1:3000"

    supabase_url: str = Field(..., alias="SUPABASE_URL")
    supabase_service_role_key: Optional[str] = Field(
        default=None, alias="SUPABASE_SERVICE_ROLE_KEY"
    )
    supabase_anon_key: Optional[str] = Field(default=None, alias="SUPABASE_ANON_KEY")

    openai_api_key: Optional[str] = Field(default=None, alias="OPENAI_API_KEY")
    openai_model_decision: str = Field(default="gpt-5.2", alias="OPENAI_MODEL_DECISION")
    openai_model_support: str = Field(default="gpt-5-mini", alias="OPENAI_MODEL_SUPPORT")
    openai_max_retries: int = Field(default=2, alias="OPENAI_MAX_RETRIES")
    openai_timeout_seconds: float = Field(default=60.0, alias="OPENAI_TIMEOUT_SECONDS")

    ai_generation_enabled: bool = Field(default=True, alias="AI_GENERATION_ENABLED")
    ai_allow_support_for_decision: bool = Field(default=False, alias="AI_ALLOW_SUPPORT_FOR_DECISION")
    ai_max_consultants_per_run: int = Field(default=25, alias="AI_MAX_CONSULTANTS_PER_RUN")
    ai_manual_run_token: Optional[str] = Field(default=None, alias="AI_MANUAL_RUN_TOKEN")


@lru_cache
def get_settings() -> Settings:
    return Settings()


def get_cors_origins() -> list[str]:
    settings = get_settings()
    return [origin.strip() for origin in settings.cors_allow_origins.split(",") if origin.strip()]
