from __future__ import annotations

from src.services.job_runners.base import DataJobRunner
from src.services.job_runners.internal_runners import (
    AiInsightsRunner,
    DebtSchedulePrecomputeRunner,
    FxSignalsRunner,
)
from src.services.job_runners.subprocess_runner import SubprocessScriptRunner


def build_default_runner_registry() -> dict[str, DataJobRunner]:
    return {
        "marketing.ga4.sync": SubprocessScriptRunner(
            runner_key="marketing.ga4.sync",
            script_relative_path="scripts/sync_marketing_web_analytics.py",
        ),
        "marketing.gsc.sync": SubprocessScriptRunner(
            runner_key="marketing.gsc.sync",
            script_relative_path="scripts/sync_marketing_web_analytics.py",
        ),
        "marketing.gsc.rollups.refresh": SubprocessScriptRunner(
            runner_key="marketing.gsc.rollups.refresh",
            script_relative_path="scripts/sync_marketing_web_analytics.py",
        ),
        "fx.rates.pull": SubprocessScriptRunner(
            runner_key="fx.rates.pull",
            script_relative_path="scripts/pull_fx_rates.py",
        ),
        "fx.exposure.refresh": SubprocessScriptRunner(
            runner_key="fx.exposure.refresh",
            script_relative_path="scripts/refresh_fx_exposure.py",
        ),
        "fx.signals.generate": FxSignalsRunner(),
        "fx.intelligence.generate": SubprocessScriptRunner(
            runner_key="fx.intelligence.generate",
            script_relative_path="scripts/generate_fx_intelligence.py",
        ),
        "salesforce.readonly.sync": SubprocessScriptRunner(
            runner_key="salesforce.readonly.sync",
            script_relative_path="scripts/sync_salesforce_readonly.py",
        ),
        "salesforce.travel_trade.rollups.refresh": SubprocessScriptRunner(
            runner_key="salesforce.travel_trade.rollups.refresh",
            script_relative_path="scripts/refresh_travel_trade_rollups.py",
        ),
        "salesforce.consultant_ai.rollups.refresh": SubprocessScriptRunner(
            runner_key="salesforce.consultant_ai.rollups.refresh",
            script_relative_path="scripts/refresh_consultant_ai_rollups.py",
        ),
        "ai.insights.generate": AiInsightsRunner(),
        "imports.bookings.upsert": SubprocessScriptRunner(
            runner_key="imports.bookings.upsert",
            script_relative_path="scripts/upsert_bookings.py",
        ),
        "imports.customer_payments.upsert": SubprocessScriptRunner(
            runner_key="imports.customer_payments.upsert",
            script_relative_path="scripts/upsert_customer_payments.py",
        ),
        "imports.supplier_invoices.upsert": SubprocessScriptRunner(
            runner_key="imports.supplier_invoices.upsert",
            script_relative_path="scripts/upsert_supplier_invoices.py",
        ),
        "imports.supplier_invoice_bookings.upsert": SubprocessScriptRunner(
            runner_key="imports.supplier_invoice_bookings.upsert",
            script_relative_path="scripts/upsert_supplier_invoice_bookings.py",
        ),
        "imports.supplier_invoice_lines.upsert": SubprocessScriptRunner(
            runner_key="imports.supplier_invoice_lines.upsert",
            script_relative_path="scripts/upsert_supplier_invoice_lines.py",
        ),
        "fx.rates.backfill": SubprocessScriptRunner(
            runner_key="fx.rates.backfill",
            script_relative_path="scripts/backfill_fx_rates_history.py",
        ),
        "salesforce.permissions.validate": SubprocessScriptRunner(
            runner_key="salesforce.permissions.validate",
            script_relative_path="scripts/validate_salesforce_readonly_permissions.py",
        ),
        "ai.insights.purge": SubprocessScriptRunner(
            runner_key="ai.insights.purge",
            script_relative_path="scripts/purge_ai_insights.py",
        ),
        "workforce.cleanup.inactive_employees": SubprocessScriptRunner(
            runner_key="workforce.cleanup.inactive_employees",
            script_relative_path="scripts/cleanup_inactive_employees.py",
        ),
        "debt.schedule.precompute": DebtSchedulePrecomputeRunner(),
    }
