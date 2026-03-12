from __future__ import annotations

import pytest

from src.core.config import get_settings, validate_runtime_settings


@pytest.mark.parametrize(
    "missing_key",
    ["AI_MANUAL_RUN_TOKEN", "FX_MANUAL_RUN_TOKEN", "DATA_JOBS_SCHEDULER_TOKEN"],
)
def test_validate_runtime_settings_requires_run_tokens_in_production(
    monkeypatch: pytest.MonkeyPatch, missing_key: str
) -> None:
    get_settings.cache_clear()
    monkeypatch.setenv("ENVIRONMENT", "production")
    monkeypatch.setenv("SUPABASE_URL", "https://example.supabase.co")
    monkeypatch.setenv("SUPABASE_ANON_KEY", "anon-key")
    monkeypatch.setenv("AI_MANUAL_RUN_TOKEN", "ai-token")
    monkeypatch.setenv("FX_MANUAL_RUN_TOKEN", "fx-token")
    monkeypatch.setenv("DATA_JOBS_SCHEDULER_TOKEN", "scheduler-token")
    monkeypatch.setenv("TRUSTED_HOSTS", "api.swainos.com")
    monkeypatch.setenv(missing_key, "")

    with pytest.raises(ValueError) as exc:
        validate_runtime_settings()
    assert missing_key in str(exc.value)
    get_settings.cache_clear()


def test_validate_runtime_settings_allows_production_when_tokens_present(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    get_settings.cache_clear()
    monkeypatch.setenv("ENVIRONMENT", "production")
    monkeypatch.setenv("SUPABASE_URL", "https://example.supabase.co")
    monkeypatch.setenv("SUPABASE_ANON_KEY", "anon-key")
    monkeypatch.setenv("AI_MANUAL_RUN_TOKEN", "ai-token")
    monkeypatch.setenv("FX_MANUAL_RUN_TOKEN", "fx-token")
    monkeypatch.setenv("DATA_JOBS_SCHEDULER_TOKEN", "scheduler-token")
    monkeypatch.setenv("TRUSTED_HOSTS", "api.swainos.com")

    validate_runtime_settings()
    get_settings.cache_clear()


def test_validate_runtime_settings_rejects_local_only_trusted_hosts_in_production(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    get_settings.cache_clear()
    monkeypatch.setenv("ENVIRONMENT", "production")
    monkeypatch.setenv("SUPABASE_URL", "https://example.supabase.co")
    monkeypatch.setenv("SUPABASE_ANON_KEY", "anon-key")
    monkeypatch.setenv("AI_MANUAL_RUN_TOKEN", "ai-token")
    monkeypatch.setenv("FX_MANUAL_RUN_TOKEN", "fx-token")
    monkeypatch.setenv("DATA_JOBS_SCHEDULER_TOKEN", "scheduler-token")
    monkeypatch.setenv("TRUSTED_HOSTS", "localhost,127.0.0.1,testserver")

    with pytest.raises(ValueError, match="TRUSTED_HOSTS"):
        validate_runtime_settings()
    get_settings.cache_clear()
