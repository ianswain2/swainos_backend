from __future__ import annotations

from src.services.job_runners.base import RunnerResult


class FxSignalsRunner:
    runner_key = "fx.signals.generate"

    def run(self, job_key: str, run_id: str, metadata: dict[str, object]) -> RunnerResult:
        from src.api.dependencies import get_fx_service
        from src.schemas.fx import FxSignalRunRequest

        service = get_fx_service()
        result = service.run_signals(FxSignalRunRequest(run_type="manual"))
        return RunnerResult(
            status=result.status,
            message=result.message,
            output=result.model_dump(by_alias=True),
        )


class AiInsightsRunner:
    runner_key = "ai.insights.generate"

    def run(self, job_key: str, run_id: str, metadata: dict[str, object]) -> RunnerResult:
        from src.api.dependencies import get_ai_insights_service

        service = get_ai_insights_service()
        result = service.run_manual_generation(trigger="data_job")
        return RunnerResult(
            status="success",
            message="AI insights generation completed",
            output={"result": result},
        )


class DebtSchedulePrecomputeRunner:
    runner_key = "debt.schedule.precompute"

    def run(self, job_key: str, run_id: str, metadata: dict[str, object]) -> RunnerResult:
        from src.api.dependencies import get_debt_service_service

        service = get_debt_service_service()
        result = service.precompute_all_schedules()
        return RunnerResult(
            status="success",
            message="Debt schedule precompute completed",
            output=result,
        )
