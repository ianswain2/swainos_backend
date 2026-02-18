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

    fx_manual_run_token: Optional[str] = Field(default=None, alias="FX_MANUAL_RUN_TOKEN")
    fx_base_currency: str = Field(default="USD", alias="FX_BASE_CURRENCY")
    fx_target_currencies: str = Field(default="AUD,NZD,ZAR", alias="FX_TARGET_CURRENCIES")

    fx_primary_provider: str = Field(default="twelve_data", alias="FX_PRIMARY_PROVIDER")
    fx_primary_api_key: Optional[str] = Field(default=None, alias="FX_PRIMARY_API_KEY")
    fx_primary_base_url: str = Field(default="https://api.twelvedata.com", alias="FX_PRIMARY_BASE_URL")

    fx_backup_enabled: bool = Field(default=False, alias="FX_BACKUP_ENABLED")
    fx_backup_provider: str = Field(default="exchange_rate_api", alias="FX_BACKUP_PROVIDER")
    fx_backup_api_key: Optional[str] = Field(default=None, alias="FX_BACKUP_API_KEY")
    fx_backup_base_url: str = Field(
        default="https://v6.exchangerate-api.com",
        alias="FX_BACKUP_BASE_URL",
    )

    fx_pull_interval_minutes: int = Field(default=15, alias="FX_PULL_INTERVAL_MINUTES")
    fx_stale_after_minutes: int = Field(default=30, alias="FX_STALE_AFTER_MINUTES")
    fx_max_pull_retries: int = Field(default=3, alias="FX_MAX_PULL_RETRIES")
    fx_ledger_enabled: bool = Field(default=True, alias="FX_LEDGER_ENABLED")
    fx_allow_negative_balance: bool = Field(default=False, alias="FX_ALLOW_NEGATIVE_BALANCE")

    macro_provider: str = Field(default="fred", alias="MACRO_PROVIDER")
    macro_api_key: Optional[str] = Field(default=None, alias="MACRO_API_KEY")
    macro_base_url: str = Field(default="https://api.stlouisfed.org/fred", alias="MACRO_BASE_URL")

    news_provider: str = Field(default="marketaux", alias="NEWS_PROVIDER")
    news_api_key: Optional[str] = Field(default=None, alias="NEWS_API_KEY")
    news_base_url: str = Field(default="https://api.marketaux.com", alias="NEWS_BASE_URL")

    fx_intelligence_daily_enabled: bool = Field(
        default=True,
        alias="FX_INTELLIGENCE_DAILY_ENABLED",
    )
    fx_intelligence_daily_hour_utc: int = Field(default=6, alias="FX_INTELLIGENCE_DAILY_HOUR_UTC")
    fx_intelligence_on_demand_enabled: bool = Field(
        default=True,
        alias="FX_INTELLIGENCE_ON_DEMAND_ENABLED",
    )
    fx_intelligence_min_source_count: int = Field(default=3, alias="FX_INTELLIGENCE_MIN_SOURCE_COUNT")


@lru_cache
def get_settings() -> Settings:
    return Settings()


def get_cors_origins() -> list[str]:
    settings = get_settings()
    return [origin.strip() for origin in settings.cors_allow_origins.split(",") if origin.strip()]
