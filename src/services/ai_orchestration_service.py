from __future__ import annotations

from datetime import date, datetime
from typing import Any, Dict, List, Tuple
from uuid import uuid4

from src.core.config import get_settings
from src.repositories.ai_insights_repository import AiInsightsRepository
from src.services.openai_insights_service import OpenAiInsightsService
from src.schemas.travel_consultants import TravelConsultantLeaderboardFilters
from src.services.travel_consultants_service import TravelConsultantsService


class AiOrchestrationService:
    TARGET_CONVERSION_RATE = 0.35
    TARGET_MARGIN_PCT = 0.08
    TARGET_GROWTH_PCT = 0.12
    STRATEGIC_TARGET_CONVERSION_RATE = 0.35
    STRATEGIC_TARGET_MARGIN_PCT = 0.20
    STRATEGIC_TARGET_GROWTH_PCT = 0.12
    MIN_LEADS_FOR_ACTIONABLE = 10
    MIN_ITINERARIES_FOR_ACTIONABLE = 3

    def __init__(
        self,
        repository: AiInsightsRepository,
        openai_service: OpenAiInsightsService,
        travel_consultants_service: TravelConsultantsService | None = None,
    ) -> None:
        self.repository = repository
        self.openai_service = openai_service
        self.travel_consultants_service = travel_consultants_service
        self.settings = get_settings()

    def generate_insights(self, trigger: str = "manual") -> Dict[str, Any]:
        if not self.settings.ai_generation_enabled:
            return {
                "runId": str(uuid4()),
                "trigger": trigger,
                "status": "skipped",
                "reason": "AI_GENERATION_ENABLED is false",
                "createdEvents": 0,
                "createdRecommendations": 0,
            }

        run_id = str(uuid4())
        generated_events: List[Dict[str, Any]] = []
        consultant_pairs: List[Tuple[Dict[str, Any], Dict[str, Any]]] = []

        briefing_result = self._generate_command_center_briefing(run_id=run_id)

        consultant_context_rows_raw = self._get_consultant_context_rows()
        consultant_context_rows = self._filter_existing_consultants(consultant_context_rows_raw)
        for consultant_row in consultant_context_rows:
            if not self._is_consultant_actionable(consultant_row):
                continue
            event, recommendation = self._generate_consultant_recommendation(
                run_id=run_id,
                consultant_row=consultant_row,
            )
            consultant_pairs.append((event, recommendation))

        itinerary_context_rows = self.repository.list_itinerary_health_context(limit=1)
        if itinerary_context_rows:
            itinerary_event = self._generate_itinerary_health_event(
                run_id=run_id, itinerary_row=itinerary_context_rows[0]
            )
            if itinerary_event:
                generated_events.append(itinerary_event)

        inserted_events = self.repository.insert_insight_events(generated_events)
        inserted_recommendations: List[Dict[str, Any]] = []
        for event_row, recommendation_row in consultant_pairs:
            inserted_pair_event = self.repository.insert_insight_events([event_row])
            if inserted_pair_event:
                recommendation_row["insight_event_id"] = inserted_pair_event[0].get("id")
                inserted_events.extend(inserted_pair_event)
            inserted_pair_recommendation = self.repository.insert_recommendations([recommendation_row])
            inserted_recommendations.extend(inserted_pair_recommendation)

        return {
            "runId": run_id,
            "trigger": trigger,
            "status": "completed",
            "createdEvents": len(inserted_events),
            "createdRecommendations": len(inserted_recommendations),
            "briefingGenerated": bool(briefing_result),
            "consultantsEvaluated": len(consultant_context_rows),
        }

    def _get_consultant_context_rows(self) -> List[Dict[str, Any]]:
        if not self.travel_consultants_service:
            return self.repository.list_travel_consultant_context(
                limit=self.settings.ai_max_consultants_per_run
            )
        rolling_travel = self.travel_consultants_service.get_leaderboard(
            self._build_leaderboard_filters(period_type="rolling12", domain="travel")
        )
        rolling_funnel = self.travel_consultants_service.get_leaderboard(
            self._build_leaderboard_filters(period_type="rolling12", domain="funnel")
        )
        year_travel = self.travel_consultants_service.get_leaderboard(
            self._build_leaderboard_filters(period_type="year", domain="travel")
        )
        monthly_travel = self.travel_consultants_service.get_leaderboard(
            self._build_leaderboard_filters(period_type="monthly", domain="travel")
        )
        rolling_funnel_by_employee = {
            row.employee_id: row for row in rolling_funnel.rankings
        }
        year_travel_by_employee = {row.employee_id: row for row in year_travel.rankings}
        monthly_travel_by_employee = {
            row.employee_id: row for row in monthly_travel.rankings
        }
        benchmark_rows = self.repository.list_consultant_benchmarks_context()
        team_benchmarks = self._resolve_benchmark_context(
            benchmark_rows=benchmark_rows,
            period_type="rolling12",
            domain="travel",
            fallback_rankings=rolling_travel.rankings,
        )
        rows: List[Dict[str, Any]] = []
        for ranking in rolling_travel.rankings[: self.settings.ai_max_consultants_per_run]:
            rolling_funnel_row = rolling_funnel_by_employee.get(ranking.employee_id)
            year_row = year_travel_by_employee.get(ranking.employee_id)
            monthly_row = monthly_travel_by_employee.get(ranking.employee_id)

            funnel_conversion_rate = (
                rolling_funnel_row.conversion_rate if rolling_funnel_row else 0.0
            )
            funnel_close_rate = rolling_funnel_row.close_rate if rolling_funnel_row else 0.0
            funnel_booked_revenue = (
                rolling_funnel_row.booked_revenue if rolling_funnel_row else 0.0
            )

            rows.append(
                {
                    "employee_id": ranking.employee_id,
                    "employee_external_id": ranking.employee_external_id,
                    "first_name": ranking.first_name,
                    "last_name": ranking.last_name,
                    "email": ranking.email,
                    "itinerary_count": ranking.itinerary_count,
                    "booked_revenue_amount": ranking.booked_revenue,
                    "commission_income_amount": ranking.commission_income,
                    "margin_pct": ranking.margin_pct,
                    "lead_count": ranking.lead_count,
                    "closed_won_count": ranking.closed_won_count,
                    "closed_lost_count": ranking.closed_lost_count,
                    "conversion_rate": ranking.conversion_rate,
                    "close_rate": ranking.close_rate,
                    "avg_speed_to_book_days": ranking.avg_speed_to_book_days or 0.0,
                    "growth_target_variance_pct": ranking.growth_target_variance_pct,
                    "yoy_to_date_variance_pct": ranking.yoy_to_date_variance_pct,
                    "as_of_period_start": str(rolling_travel.period_start),
                    "as_of_period_end": str(rolling_travel.period_end),
                    "snapshot_monthly_travel": {
                        "periodType": "monthly",
                        "domain": "travel",
                        "conversionRate": monthly_row.conversion_rate if monthly_row else 0.0,
                        "closeRate": monthly_row.close_rate if monthly_row else 0.0,
                        "bookedRevenue": monthly_row.booked_revenue if monthly_row else 0.0,
                        "leadCount": monthly_row.lead_count if monthly_row else 0,
                        "closedWonCount": monthly_row.closed_won_count if monthly_row else 0,
                        "closedLostCount": monthly_row.closed_lost_count if monthly_row else 0,
                        "marginPct": monthly_row.margin_pct if monthly_row else 0.0,
                    },
                    "snapshot_year_travel": {
                        "periodType": "year",
                        "domain": "travel",
                        "conversionRate": year_row.conversion_rate if year_row else 0.0,
                        "closeRate": year_row.close_rate if year_row else 0.0,
                        "bookedRevenue": year_row.booked_revenue if year_row else 0.0,
                        "leadCount": year_row.lead_count if year_row else 0,
                        "closedWonCount": year_row.closed_won_count if year_row else 0,
                        "closedLostCount": year_row.closed_lost_count if year_row else 0,
                        "marginPct": year_row.margin_pct if year_row else 0.0,
                    },
                    "snapshot_rolling12_travel": {
                        "periodType": "rolling12",
                        "domain": "travel",
                        "conversionRate": ranking.conversion_rate,
                        "closeRate": ranking.close_rate,
                        "bookedRevenue": ranking.booked_revenue,
                        "leadCount": ranking.lead_count,
                        "closedWonCount": ranking.closed_won_count,
                        "closedLostCount": ranking.closed_lost_count,
                        "marginPct": ranking.margin_pct,
                    },
                    "travel_vs_funnel_split_deltas": {
                        "rolling12": {
                            "conversionRateDelta": ranking.conversion_rate - funnel_conversion_rate,
                            "closeRateDelta": ranking.close_rate - funnel_close_rate,
                            "bookedRevenueDelta": ranking.booked_revenue - funnel_booked_revenue,
                        }
                    },
                    "benchmark_context": {
                        **team_benchmarks,
                    },
                }
            )
        return rows

    @staticmethod
    def _build_leaderboard_filters(
        *, period_type: str, domain: str
    ) -> TravelConsultantLeaderboardFilters:
        return TravelConsultantLeaderboardFilters(
            period_type=period_type,
            domain=domain,
            year=None,
            month=None,
            sort_by="booked_revenue",
            sort_order="desc",
            currency_code=None,
        )

    def _resolve_benchmark_context(
        self,
        *,
        benchmark_rows: List[Dict[str, Any]],
        period_type: str,
        domain: str,
        fallback_rankings: List[Any],
    ) -> Dict[str, float]:
        for row in benchmark_rows:
            if (
                str(row.get("period_type") or "") == period_type
                and str(row.get("domain") or "") == domain
            ):
                return self._normalize_benchmark_row(row)
        return self._build_team_benchmarks_fallback(fallback_rankings)

    @staticmethod
    def _normalize_benchmark_row(row: Dict[str, Any]) -> Dict[str, float]:
        return {
            "targetConversionRate": float(row.get("target_conversion_rate") or 0.0),
            "targetMarginPct": float(row.get("target_margin_pct") or 0.0),
            "targetGrowthPct": float(row.get("target_growth_pct") or 0.0),
            "strategicTargetConversionRate": float(
                row.get("strategic_target_conversion_rate")
                or AiOrchestrationService.STRATEGIC_TARGET_CONVERSION_RATE
            ),
            "strategicTargetMarginPct": float(
                row.get("strategic_target_margin_pct")
                or AiOrchestrationService.STRATEGIC_TARGET_MARGIN_PCT
            ),
            "strategicTargetGrowthPct": float(
                row.get("strategic_target_growth_pct")
                or AiOrchestrationService.STRATEGIC_TARGET_GROWTH_PCT
            ),
            "teamAvgConversionRate": float(row.get("team_avg_conversion_rate") or 0.0),
            "teamAvgMarginPct": float(row.get("team_avg_margin_pct") or 0.0),
            "teamAvgCloseRate": float(row.get("team_avg_close_rate") or 0.0),
            "teamAvgSpeedToBookDays": float(row.get("team_avg_speed_to_book_days") or 0.0),
            "teamTopConversionRate": float(row.get("team_top_conversion_rate") or 0.0),
            "teamTopMarginPct": float(row.get("team_top_margin_pct") or 0.0),
            "teamTopCloseRate": float(row.get("team_top_close_rate") or 0.0),
            "teamLowConversionRate": float(row.get("team_low_conversion_rate") or 0.0),
            "teamLowMarginPct": float(row.get("team_low_margin_pct") or 0.0),
            "teamLowCloseRate": float(row.get("team_low_close_rate") or 0.0),
            "teamMedianConversionRate": float(row.get("team_median_conversion_rate") or 0.0),
            "teamMedianMarginPct": float(row.get("team_median_margin_pct") or 0.0),
            "teamMedianCloseRate": float(row.get("team_median_close_rate") or 0.0),
            "teamP20ConversionRate": float(row.get("team_p20_conversion_rate") or 0.0),
            "teamP20MarginPct": float(row.get("team_p20_margin_pct") or 0.0),
            "teamP20CloseRate": float(row.get("team_p20_close_rate") or 0.0),
            "teamP80ConversionRate": float(row.get("team_p80_conversion_rate") or 0.0),
            "teamP80MarginPct": float(row.get("team_p80_margin_pct") or 0.0),
            "teamP80CloseRate": float(row.get("team_p80_close_rate") or 0.0),
            "consultantCount": float(row.get("consultant_count") or 0.0),
        }

    @staticmethod
    def _build_team_benchmarks_fallback(rankings: List[Any]) -> Dict[str, float]:
        if not rankings:
            return {
                "targetConversionRate": AiOrchestrationService.TARGET_CONVERSION_RATE,
                "targetMarginPct": AiOrchestrationService.TARGET_MARGIN_PCT,
                "targetGrowthPct": AiOrchestrationService.TARGET_GROWTH_PCT,
                "strategicTargetConversionRate": AiOrchestrationService.STRATEGIC_TARGET_CONVERSION_RATE,
                "strategicTargetMarginPct": AiOrchestrationService.STRATEGIC_TARGET_MARGIN_PCT,
                "strategicTargetGrowthPct": AiOrchestrationService.STRATEGIC_TARGET_GROWTH_PCT,
                "teamAvgConversionRate": 0.0,
                "teamAvgMarginPct": 0.0,
                "teamAvgCloseRate": 0.0,
                "teamAvgSpeedToBookDays": 0.0,
                "teamTopConversionRate": 0.0,
                "teamTopMarginPct": 0.0,
                "teamTopCloseRate": 0.0,
                "teamLowConversionRate": 0.0,
                "teamLowMarginPct": 0.0,
                "teamLowCloseRate": 0.0,
                "teamMedianConversionRate": 0.0,
                "teamMedianMarginPct": 0.0,
                "teamMedianCloseRate": 0.0,
                "teamP20ConversionRate": 0.0,
                "teamP20MarginPct": 0.0,
                "teamP20CloseRate": 0.0,
                "teamP80ConversionRate": 0.0,
                "teamP80MarginPct": 0.0,
                "teamP80CloseRate": 0.0,
                "consultantCount": 0.0,
            }
        conversion_values = sorted(row.conversion_rate for row in rankings)
        margin_values = sorted(row.margin_pct for row in rankings)
        close_values = sorted(row.close_rate for row in rankings)
        speed_values = [float(row.avg_speed_to_book_days or 0.0) for row in rankings if row.avg_speed_to_book_days is not None]
        total_rows = len(rankings)
        total_conversion = sum(row.conversion_rate for row in rankings)
        total_margin = sum(row.margin_pct for row in rankings)
        total_close = sum(row.close_rate for row in rankings)
        team_avg_conversion = total_conversion / total_rows
        team_avg_margin = total_margin / total_rows
        team_avg_close = total_close / total_rows
        team_low_conversion = conversion_values[max(0, int(total_rows * 0.2) - 1)]
        team_low_margin = margin_values[max(0, int(total_rows * 0.2) - 1)]
        team_low_close = close_values[max(0, int(total_rows * 0.2) - 1)]
        target_conversion_rate = max(
            AiOrchestrationService.TARGET_CONVERSION_RATE,
            team_avg_conversion * 0.85,
        )
        target_margin_pct = max(
            AiOrchestrationService.TARGET_MARGIN_PCT,
            team_avg_margin * 0.85,
        )
        return {
            "targetConversionRate": target_conversion_rate,
            "targetMarginPct": target_margin_pct,
            "targetGrowthPct": AiOrchestrationService.TARGET_GROWTH_PCT,
            "strategicTargetConversionRate": AiOrchestrationService.STRATEGIC_TARGET_CONVERSION_RATE,
            "strategicTargetMarginPct": AiOrchestrationService.STRATEGIC_TARGET_MARGIN_PCT,
            "strategicTargetGrowthPct": AiOrchestrationService.STRATEGIC_TARGET_GROWTH_PCT,
            "teamAvgConversionRate": team_avg_conversion,
            "teamAvgMarginPct": team_avg_margin,
            "teamAvgCloseRate": team_avg_close,
            "teamAvgSpeedToBookDays": (
                sum(speed_values) / len(speed_values) if speed_values else 0.0
            ),
            "teamTopConversionRate": max(row.conversion_rate for row in rankings),
            "teamTopMarginPct": max(row.margin_pct for row in rankings),
            "teamTopCloseRate": max(row.close_rate for row in rankings),
            "teamLowConversionRate": team_low_conversion,
            "teamLowMarginPct": team_low_margin,
            "teamLowCloseRate": team_low_close,
            "teamMedianConversionRate": conversion_values[len(conversion_values) // 2],
            "teamMedianMarginPct": margin_values[len(margin_values) // 2],
            "teamMedianCloseRate": close_values[len(close_values) // 2],
            "teamP20ConversionRate": team_low_conversion,
            "teamP20MarginPct": team_low_margin,
            "teamP20CloseRate": team_low_close,
            "teamP80ConversionRate": conversion_values[min(len(conversion_values) - 1, max(0, int(total_rows * 0.8) - 1))],
            "teamP80MarginPct": margin_values[min(len(margin_values) - 1, max(0, int(total_rows * 0.8) - 1))],
            "teamP80CloseRate": close_values[min(len(close_values) - 1, max(0, int(total_rows * 0.8) - 1))],
            "consultantCount": float(total_rows),
        }

    def _filter_existing_consultants(
        self, consultant_context_rows: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        employee_ids = [
            str(row.get("employee_id") or "")
            for row in consultant_context_rows
            if row.get("employee_id")
        ]
        existing_employee_ids = self.repository.list_existing_employee_ids(employee_ids)
        filtered_rows: List[Dict[str, Any]] = []
        for row in consultant_context_rows:
            employee_id = str(row.get("employee_id") or "")
            if not employee_id or employee_id not in existing_employee_ids:
                continue
            filtered_rows.append(row)
        return filtered_rows

    def _generate_command_center_briefing(self, run_id: str) -> Dict[str, Any]:
        context_rows = self.repository.list_command_center_context()
        if not context_rows:
            return {}
        context_row = context_rows[0]
        company_metrics_rows = self.repository.list_company_metrics_context()
        fallback_payload = self._build_command_center_fallback_payload(
            context_row=context_row,
            company_metrics_rows=company_metrics_rows,
        )
        model_result = self.openai_service.build_structured_output(
            tier=OpenAiInsightsService.TIER_DECISION,
            operation="daily_briefing",
            system_prompt=(
                "You are a business command-center analyst. Return compact JSON with keys: "
                "title, summary, highlights, topActions, confidence."
            ),
            user_payload={
                "domain": "command_center",
                "context": context_row,
                "companyMetrics": company_metrics_rows,
                "constraints": [
                    "Use only facts from context",
                    "Prioritize actionable next steps",
                    "Do not fabricate source metrics",
                    "Return highlights and topActions as plain text string lists only (no JSON objects, no key/value fragments)",
                    "Each highlight must be <= 18 words and one sentence",
                    "Each top action must be <= 22 words and one sentence",
                ],
            },
            fallback_payload=fallback_payload,
        )
        payload = model_result.payload
        normalized_highlights = self._normalize_briefing_items(
            payload.get("highlights"),
            fallback_payload["highlights"],
            item_kind="highlight",
        )
        normalized_top_actions = self._normalize_briefing_items(
            payload.get("topActions"),
            fallback_payload["topActions"],
            item_kind="action",
        )
        row = {
            "briefing_date": date.today().isoformat(),
            "title": str(payload.get("title") or fallback_payload["title"]),
            "summary": str(payload.get("summary") or fallback_payload["summary"]),
            "highlights": normalized_highlights,
            "top_actions": normalized_top_actions,
            "confidence": self._coerce_confidence(
                payload.get("confidence"),
                fallback=float(fallback_payload["confidence"]),
            ),
            "evidence": fallback_payload["evidence"],
            "generated_at": datetime.utcnow().isoformat(),
            "model_name": model_result.model_name,
            "model_tier": model_result.model_tier,
            "tokens_used": model_result.tokens_used,
            "latency_ms": model_result.latency_ms,
            "run_id": run_id,
            "updated_at": datetime.utcnow().isoformat(),
        }
        saved = self.repository.upsert_daily_briefing(row)
        return saved or {}

    def _generate_consultant_recommendation(
        self,
        *,
        run_id: str,
        consultant_row: Dict[str, Any],
    ) -> tuple[Dict[str, Any], Dict[str, Any]]:
        fallback_payload = self._build_consultant_fallback_payload(consultant_row)
        model_result = self.openai_service.build_structured_output(
            tier=OpenAiInsightsService.TIER_DECISION,
            operation="consultant_coaching",
            system_prompt=(
                "You are a travel consultant performance coach. Return compact JSON with keys: "
                "title, summary, recommendedAction, severity, priority, confidence."
            ),
            user_payload={
                "domain": "travel_consultant",
                "consultant": consultant_row,
                "constraints": [
                    "Use only provided metrics",
                    "This is an individual coaching note for one consultant, not a team summary",
                    "Summary must begin with consultant full name",
                    "Summary must be exactly 2 short sentences and under 45 words total",
                    "Sentence 1: core issue or opportunity for this consultant",
                    "Sentence 2: one explicit benchmark comparison in format '<metric> X% vs team Y% (target Z%)'",
                    "Keep language direct, plain-English, and manager-grade",
                    "RecommendedAction must be a single short sentence under 30 words",
                    "Use severity low/medium/high/critical",
                    "Use priority 1 to 5",
                ],
            },
            fallback_payload=fallback_payload,
        )
        payload = model_result.payload
        title = self._normalize_consultant_title(
            title=str(payload.get("title") or fallback_payload["title"]),
            consultant_row=consultant_row,
        )
        summary = self._build_metric_anchored_summary(
            summary=str(payload.get("summary") or fallback_payload["summary"]),
            consultant_row=consultant_row,
        )
        recommended_action = self._normalize_recommended_action(
            recommended_action=str(
                payload.get("recommendedAction") or fallback_payload["recommendedAction"]
            ),
            consultant_row=consultant_row,
        )
        severity = str(payload.get("severity") or fallback_payload["severity"])
        if severity not in {"low", "medium", "high", "critical"}:
            severity = "medium"
        priority = int(payload.get("priority") or fallback_payload["priority"])
        confidence = self._coerce_confidence(
            payload.get("confidence"),
            fallback=float(fallback_payload["confidence"]),
        )
        evidence = fallback_payload["evidence"]
        now_iso = datetime.utcnow().isoformat()

        event_row = {
            "insight_type": "coaching_signal",
            "domain": "travel_consultant",
            "severity": severity,
            "status": "new",
            "entity_type": "employee",
            "entity_id": str(consultant_row.get("employee_id") or ""),
            "title": title,
            "summary": summary,
            "recommended_action": recommended_action,
            "priority": max(1, min(priority, 5)),
            "confidence": max(0.0, min(confidence, 1.0)),
            "evidence": evidence,
            "source_metrics": {"consultantContext": consultant_row},
            "metadata": {"trigger": "manual", "reason": "coaching_signal"},
            "generated_at": now_iso,
            "model_name": model_result.model_name,
            "model_tier": model_result.model_tier,
            "tokens_used": model_result.tokens_used,
            "latency_ms": model_result.latency_ms,
            "run_id": run_id,
            "created_at": now_iso,
            "updated_at": now_iso,
        }

        recommendation_row = {
            "domain": "travel_consultant",
            "status": "new",
            "entity_type": "employee",
            "entity_id": str(consultant_row.get("employee_id") or ""),
            "title": title,
            "summary": summary,
            "recommended_action": recommended_action,
            "priority": max(1, min(priority, 5)),
            "confidence": max(0.0, min(confidence, 1.0)),
            "evidence": evidence,
            "generated_at": now_iso,
            "model_name": model_result.model_name,
            "model_tier": model_result.model_tier,
            "tokens_used": model_result.tokens_used,
            "latency_ms": model_result.latency_ms,
            "run_id": run_id,
            "created_at": now_iso,
            "updated_at": now_iso,
        }
        return event_row, recommendation_row

    def _generate_itinerary_health_event(
        self, *, run_id: str, itinerary_row: Dict[str, Any]
    ) -> Dict[str, Any] | None:
        conversion_rate = float(itinerary_row.get("conversion_rate") or 0)
        deposit_coverage = float(itinerary_row.get("deposit_coverage_ratio") or 0)
        if conversion_rate >= 0.30 and deposit_coverage >= 0.95:
            return None
        severity = "high" if conversion_rate < 0.2 or deposit_coverage < 0.8 else "medium"
        now_iso = datetime.utcnow().isoformat()
        return {
            "insight_type": "anomaly",
            "domain": "itinerary",
            "severity": severity,
            "status": "new",
            "entity_type": "itinerary_health",
            "entity_id": str(itinerary_row.get("period_start") or date.today().isoformat()),
            "title": "Itinerary funnel health needs intervention",
            "summary": (
                f"Conversion is {round(conversion_rate * 100, 1)}% and deposit coverage is "
                f"{round(deposit_coverage * 100, 1)}% in the latest health window."
            ),
            "recommended_action": (
                "Review lead qualification and deposit follow-up for near-term departures."
            ),
            "priority": 2,
            "confidence": 0.86,
            "evidence": {
                "summary": "Latest itinerary health context breached configured thresholds.",
                "metrics": [
                    {
                        "key": "conversion_rate",
                        "label": "Lead conversion rate",
                        "currentValue": conversion_rate,
                        "baselineValue": 0.30,
                        "deltaPct": conversion_rate - 0.30,
                        "unit": "ratio",
                    },
                    {
                        "key": "deposit_coverage_ratio",
                        "label": "Deposit coverage ratio",
                        "currentValue": deposit_coverage,
                        "baselineValue": 0.95,
                        "deltaPct": deposit_coverage - 0.95,
                        "unit": "ratio",
                    },
                ],
                "sourceViewNames": ["ai_context_itinerary_health_v1"],
                "referencePeriod": str(itinerary_row.get("period_start") or ""),
            },
            "source_metrics": {"itineraryHealthContext": itinerary_row},
            "metadata": {"trigger": "manual", "reason": "threshold_breach"},
            "generated_at": now_iso,
            "model_name": "deterministic-fallback",
            "model_tier": "fallback",
            "tokens_used": 0,
            "latency_ms": 0,
            "run_id": run_id,
            "created_at": now_iso,
            "updated_at": now_iso,
        }

    @staticmethod
    def _is_consultant_actionable(consultant_row: Dict[str, Any]) -> bool:
        conversion_rate = float(consultant_row.get("conversion_rate") or 0)
        growth_variance = float(consultant_row.get("growth_target_variance_pct") or 0)
        margin_pct = float(consultant_row.get("margin_pct") or 0)
        yoy_to_date_variance_pct = float(consultant_row.get("yoy_to_date_variance_pct") or 0)
        lead_count = int(consultant_row.get("lead_count") or 0)
        itinerary_count = int(consultant_row.get("itinerary_count") or 0)
        monthly_snapshot = consultant_row.get("snapshot_monthly_travel") or {}
        monthly_conversion_rate = float(monthly_snapshot.get("conversionRate") or 0.0)
        conversion_delta_monthly_vs_rolling = monthly_conversion_rate - conversion_rate
        benchmark_context = consultant_row.get("benchmark_context") or {}
        target_conversion_rate = float(
            benchmark_context.get("targetConversionRate")
            or AiOrchestrationService.TARGET_CONVERSION_RATE
        )
        target_margin_pct = float(
            benchmark_context.get("targetMarginPct")
            or AiOrchestrationService.TARGET_MARGIN_PCT
        )

        has_sufficient_volume = (
            lead_count >= AiOrchestrationService.MIN_LEADS_FOR_ACTIONABLE
            or itinerary_count >= AiOrchestrationService.MIN_ITINERARIES_FOR_ACTIONABLE
        )
        if not has_sufficient_volume:
            return False

        risk_flags = 0
        if conversion_rate < target_conversion_rate:
            risk_flags += 1
        if margin_pct < target_margin_pct:
            risk_flags += 1
        if growth_variance < -0.10:
            risk_flags += 1
        if yoy_to_date_variance_pct < -0.10:
            risk_flags += 1
        if conversion_delta_monthly_vs_rolling <= -0.05:
            risk_flags += 1

        severe_conversion_gap = conversion_rate < (target_conversion_rate * 0.6)
        severe_margin_gap = margin_pct < (target_margin_pct * 0.6)
        if severe_conversion_gap or severe_margin_gap:
            return True
        return risk_flags >= 2

    @staticmethod
    def _build_command_center_fallback_payload(
        context_row: Dict[str, Any],
        company_metrics_rows: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        conversion_rate = float(context_row.get("lead_conversion_rate_12m") or 0)
        deposit_coverage = float(context_row.get("avg_deposit_coverage_ratio_6m") or 0)
        net_cash_30d = float(context_row.get("net_cash_flow_30d") or 0)
        rolling_company_row = next(
            (
                row
                for row in company_metrics_rows
                if str(row.get("period_type") or "") == "rolling12"
                and str(row.get("domain") or "") == "travel"
            ),
            {},
        )
        rolling_margin_pct = float(rolling_company_row.get("weighted_margin_pct") or 0.0)
        rolling_close_rate = float(rolling_company_row.get("weighted_close_rate") or 0.0)
        highlights = [
            f"Lead conversion 12m is {round(conversion_rate * 100, 1)}%.",
            f"Deposit coverage 6m average is {round(deposit_coverage * 100, 1)}%.",
            f"Projected 30d net cash flow is {round(net_cash_30d, 2)}.",
            f"Rolling12 company margin is {round(rolling_margin_pct * 100, 1)}% with close rate {round(rolling_close_rate * 100, 1)}%.",
        ]
        top_actions = [
            "Prioritize consultant coaching for low-conversion segments.",
            "Escalate deposit follow-up on at-risk open itineraries.",
            "Review short-horizon cash constraints before large commitments.",
        ]
        return {
            "title": "Daily operating brief",
            "summary": "Command center metrics reviewed with focus on cash, conversion, and deposits.",
            "highlights": highlights,
            "topActions": top_actions,
            "confidence": 0.84,
            "evidence": {
                "summary": "Built from ai_context_command_center_v1 aggregate metrics.",
                "metrics": [
                    {
                        "key": "lead_conversion_rate_12m",
                        "label": "Lead conversion (12m)",
                        "currentValue": conversion_rate,
                        "baselineValue": 0.35,
                        "deltaPct": conversion_rate - 0.35,
                        "unit": "ratio",
                    },
                    {
                        "key": "avg_deposit_coverage_ratio_6m",
                        "label": "Deposit coverage (6m avg)",
                        "currentValue": deposit_coverage,
                        "baselineValue": 1.0,
                        "deltaPct": deposit_coverage - 1.0,
                        "unit": "ratio",
                    },
                    {
                        "key": "company_margin_pct_rolling12_travel",
                        "label": "Company margin % (rolling12, travel basis)",
                        "currentValue": rolling_margin_pct,
                        "baselineValue": AiOrchestrationService.STRATEGIC_TARGET_MARGIN_PCT,
                        "deltaPct": rolling_margin_pct - AiOrchestrationService.STRATEGIC_TARGET_MARGIN_PCT,
                        "unit": "ratio",
                    },
                    {
                        "key": "company_close_rate_rolling12_travel",
                        "label": "Company close rate (rolling12, travel basis)",
                        "currentValue": rolling_close_rate,
                        "baselineValue": None,
                        "deltaPct": None,
                        "unit": "ratio",
                    },
                ],
                "sourceViewNames": [
                    "ai_context_command_center_v1",
                    "ai_context_company_metrics_v1",
                ],
                "referencePeriod": str(context_row.get("as_of_date") or ""),
            },
        }

    @staticmethod
    def _build_consultant_fallback_payload(consultant_row: Dict[str, Any]) -> Dict[str, Any]:
        first_name = str(consultant_row.get("first_name") or "Consultant")
        conversion_rate = float(consultant_row.get("conversion_rate") or 0)
        growth_variance = float(consultant_row.get("growth_target_variance_pct") or 0)
        margin_pct = float(consultant_row.get("margin_pct") or 0)
        yoy_to_date_variance_pct = float(consultant_row.get("yoy_to_date_variance_pct") or 0)
        monthly_snapshot = consultant_row.get("snapshot_monthly_travel") or {}
        rolling_snapshot = consultant_row.get("snapshot_rolling12_travel") or {}
        monthly_conversion_rate = float(monthly_snapshot.get("conversionRate") or 0.0)
        conversion_delta_monthly_vs_rolling = monthly_conversion_rate - conversion_rate
        rolling_booked_revenue = float(rolling_snapshot.get("bookedRevenue") or 0.0)
        rolling_lead_count = float(rolling_snapshot.get("leadCount") or consultant_row.get("lead_count") or 0.0)
        split = consultant_row.get("travel_vs_funnel_split_deltas") or {}
        rolling_split = split.get("rolling12") if isinstance(split, dict) else {}
        booked_revenue_delta_travel_vs_funnel = float(
            (rolling_split or {}).get("bookedRevenueDelta") or 0.0
        )
        benchmark_context = consultant_row.get("benchmark_context") or {}
        target_conversion_rate = float(
            benchmark_context.get("targetConversionRate")
            or AiOrchestrationService.TARGET_CONVERSION_RATE
        )
        target_margin_pct = float(
            benchmark_context.get("targetMarginPct") or AiOrchestrationService.TARGET_MARGIN_PCT
        )
        severity = "high" if conversion_rate < 0.25 or growth_variance < -0.10 else "medium"
        priority = 1 if severity == "high" else 2
        return {
            "title": f"{first_name} coaching opportunity",
            "summary": AiOrchestrationService._build_consultant_summary(consultant_row),
            "recommendedAction": (
                "Sales manager and consultant should run a 30-minute pipeline review this week, tighten lead qualification on new opportunities, and commit to a weekly close-plan checkpoint."
            ),
            "severity": severity,
            "priority": priority,
            "confidence": 0.82,
            "evidence": {
                "summary": (
                    "Built from Travel Consultant leaderboard canonical rollups "
                    "(rolling12/monthly/year travel + rolling12 funnel)."
                ),
                "metrics": [
                    {
                        "key": "conversion_rate_rolling12_travel",
                        "label": "Conversion rate (rolling12, travel basis)",
                        "currentValue": conversion_rate,
                        "baselineValue": float(
                            (
                                consultant_row.get("benchmark_context") or {}
                            ).get("targetConversionRate")
                            or AiOrchestrationService.TARGET_CONVERSION_RATE
                        ),
                        "deltaPct": conversion_rate
                        - float(
                            (
                                consultant_row.get("benchmark_context") or {}
                            ).get("targetConversionRate")
                            or AiOrchestrationService.TARGET_CONVERSION_RATE
                        ),
                        "unit": "ratio",
                    },
                    {
                        "key": "growth_target_variance_pct_rolling12_travel",
                        "label": "Growth target variance (rolling12, travel basis)",
                        "currentValue": growth_variance,
                        "baselineValue": 0.0,
                        "deltaPct": growth_variance,
                        "unit": "ratio",
                    },
                    {
                        "key": "margin_pct_rolling12_travel",
                        "label": "Margin % (rolling12, travel basis)",
                        "currentValue": margin_pct,
                        "baselineValue": target_margin_pct,
                        "deltaPct": margin_pct - target_margin_pct,
                        "unit": "ratio",
                    },
                    {
                        "key": "yoy_to_date_variance_pct_travel",
                        "label": "YoY-to-date variance (travel basis)",
                        "currentValue": yoy_to_date_variance_pct,
                        "baselineValue": 0.0,
                        "deltaPct": yoy_to_date_variance_pct,
                        "unit": "ratio",
                    },
                    {
                        "key": "conversion_delta_monthly_vs_rolling12_travel",
                        "label": "Conversion delta (monthly vs rolling12, travel basis)",
                        "currentValue": conversion_delta_monthly_vs_rolling,
                        "baselineValue": 0.0,
                        "deltaPct": conversion_delta_monthly_vs_rolling,
                        "unit": "ratio",
                    },
                    {
                        "key": "lead_count_rolling12_funnel_basis",
                        "label": "Lead count (rolling12, funnel basis)",
                        "currentValue": rolling_lead_count,
                        "baselineValue": None,
                        "deltaPct": None,
                        "unit": "count",
                    },
                    {
                        "key": "booked_revenue_rolling12_travel_basis",
                        "label": "Booked revenue (rolling12, travel basis)",
                        "currentValue": rolling_booked_revenue,
                        "baselineValue": None,
                        "deltaPct": None,
                        "unit": "currency",
                    },
                    {
                        "key": "booked_revenue_delta_travel_vs_funnel_rolling12",
                        "label": "Booked revenue delta (travel vs funnel, rolling12)",
                        "currentValue": booked_revenue_delta_travel_vs_funnel,
                        "baselineValue": 0.0,
                        "deltaPct": None,
                        "unit": "currency",
                    },
                ],
                "sourceViewNames": [
                    "mv_travel_consultant_leaderboard_monthly",
                    "mv_travel_consultant_funnel_monthly",
                    "ai_context_consultant_benchmarks_v1",
                ],
                "referencePeriod": str(consultant_row.get("as_of_period_start") or ""),
            },
        }

    @staticmethod
    def _build_consultant_summary(consultant_row: Dict[str, Any]) -> str:
        rolling = consultant_row.get("snapshot_rolling12_travel") or {}
        monthly = consultant_row.get("snapshot_monthly_travel") or {}
        conversion_rolling = float(rolling.get("conversionRate") or consultant_row.get("conversion_rate") or 0.0)
        margin_rolling = float(rolling.get("marginPct") or consultant_row.get("margin_pct") or 0.0)
        growth_variance = float(consultant_row.get("growth_target_variance_pct") or 0.0)
        yoy_to_date_variance = float(consultant_row.get("yoy_to_date_variance_pct") or 0.0)
        conversion_monthly = float(monthly.get("conversionRate") or 0.0)
        conversion_delta_month_vs_rolling = conversion_monthly - conversion_rolling
        trend_note = "stable month over month"
        if conversion_delta_month_vs_rolling >= 0.05:
            trend_note = "improving month over month"
        elif conversion_delta_month_vs_rolling <= -0.05:
            trend_note = "cooling month over month"
        conversion_pct = round(conversion_rolling * 100, 1)
        margin_pct = round(margin_rolling * 100, 1)
        growth_pct = round(growth_variance * 100, 1)
        yoy_pct = round(yoy_to_date_variance * 100, 1)

        if conversion_rolling < AiOrchestrationService.TARGET_CONVERSION_RATE:
            return (
                f"Conversion is below target at {conversion_pct}% and the sales pace is {trend_note}, "
                "so lead quality and follow-through need immediate coaching attention. "
                f"Margin is {margin_pct}% with growth variance at {growth_pct}% and YoY at {yoy_pct}%, "
                "which suggests tightening qualification and weekly close plans to protect outcomes."
            )

        if margin_rolling < AiOrchestrationService.TARGET_MARGIN_PCT:
            return (
                f"Conversion remains workable at {conversion_pct}%, but margin is under target at {margin_pct}%, "
                "which points to discounting or weak package mix. "
                f"Growth variance is {growth_pct}% and YoY is {yoy_pct}% ({trend_note}), "
                "so the focus should shift to higher-yield itineraries and value-based selling."
            )

        return (
            f"Performance is generally on track with conversion at {conversion_pct}% and margin at {margin_pct}%, "
            f"while growth variance is {growth_pct}% and YoY is {yoy_pct}% ({trend_note}). "
            "Use this window to reinforce the strongest funnel behaviors and replicate them across current deals."
        )

    @staticmethod
    def _coerce_confidence(value: Any, fallback: float) -> float:
        if isinstance(value, (int, float)):
            return max(0.0, min(float(value), 1.0))
        if isinstance(value, str):
            normalized = value.strip().lower()
            try:
                numeric = float(normalized)
                return max(0.0, min(numeric, 1.0))
            except ValueError:
                lookup = {
                    "very high": 0.95,
                    "high": 0.85,
                    "medium": 0.65,
                    "low": 0.45,
                    "very low": 0.30,
                }
                if normalized in lookup:
                    return lookup[normalized]
        return max(0.0, min(float(fallback), 1.0))

    @staticmethod
    def _build_metric_anchored_summary(summary: str, consultant_row: Dict[str, Any]) -> str:
        normalized_summary = " ".join(summary.replace("\n", " ").split()).strip()
        if not normalized_summary:
            normalized_summary = AiOrchestrationService._build_consultant_summary(consultant_row)
        # Remove verbose context tail from earlier format if present.
        normalized_summary = normalized_summary.split("Metric context:")[0].strip()

        full_name = (
            f"{str(consultant_row.get('first_name') or '').strip()} "
            f"{str(consultant_row.get('last_name') or '').strip()}"
        ).strip()
        if not full_name:
            full_name = "This consultant"

        first_sentence = normalized_summary.split(".")[0].strip()
        if not first_sentence:
            first_sentence = f"{full_name} needs focused coaching attention this week"
        if full_name.lower() not in first_sentence.lower():
            first_sentence = f"{full_name}: {first_sentence[0].lower() + first_sentence[1:]}" if len(first_sentence) > 1 else f"{full_name}: needs focused coaching attention"

        metric_sentence = AiOrchestrationService._build_metric_context_sentence(consultant_row)
        compact_summary = f"{first_sentence}. {metric_sentence}"
        return compact_summary[:420].strip()

    @staticmethod
    def _build_metric_context_sentence(consultant_row: Dict[str, Any]) -> str:
        benchmark_context = consultant_row.get("benchmark_context") or {}
        conversion_rate = float(consultant_row.get("conversion_rate") or 0.0)
        margin_pct = float(consultant_row.get("margin_pct") or 0.0)
        team_avg_conversion = float(benchmark_context.get("teamAvgConversionRate") or 0.0)
        team_avg_margin = float(benchmark_context.get("teamAvgMarginPct") or 0.0)
        action_target_conversion = float(
            benchmark_context.get("targetConversionRate")
            or AiOrchestrationService.TARGET_CONVERSION_RATE
        )
        action_target_margin = float(
            benchmark_context.get("targetMarginPct")
            or AiOrchestrationService.TARGET_MARGIN_PCT
        )
        conversion_gap = conversion_rate - action_target_conversion
        margin_gap = margin_pct - action_target_margin
        if conversion_gap <= margin_gap:
            return (
                f"Conversion {round(conversion_rate * 100, 1)}% vs team {round(team_avg_conversion * 100, 1)}% "
                f"(target {round(action_target_conversion * 100, 1)}%)."
            )
        return (
            f"Margin {round(margin_pct * 100, 1)}% vs team {round(team_avg_margin * 100, 1)}% "
            f"(target {round(action_target_margin * 100, 1)}%)."
        )

    @staticmethod
    def _normalize_recommended_action(recommended_action: str, consultant_row: Dict[str, Any]) -> str:
        normalized_action = " ".join(recommended_action.replace("\n", " ").split()).strip()
        first_name = str(consultant_row.get("first_name") or "consultant").strip()
        if not normalized_action:
            normalized_action = (
                f"Within 7 days, run a 30-minute pipeline review with {first_name} and lock one conversion and one margin commitment."
            )
        if len(normalized_action) > 160:
            clipped = normalized_action[:160].rstrip()
            if " " in clipped:
                clipped = clipped.rsplit(" ", 1)[0]
            normalized_action = clipped + "..."
        if not normalized_action.endswith("."):
            normalized_action = f"{normalized_action}."
        return normalized_action

    @staticmethod
    def _normalize_consultant_title(title: str, consultant_row: Dict[str, Any]) -> str:
        normalized_title = " ".join(title.replace("\n", " ").split()).strip()
        first_name = str(consultant_row.get("first_name") or "").strip()
        last_name = str(consultant_row.get("last_name") or "").strip()
        full_name = f"{first_name} {last_name}".strip() or "Consultant"
        if not normalized_title:
            normalized_title = "Performance focus"
        lowered = normalized_title.lower()
        if first_name and first_name.lower() in lowered:
            return normalized_title
        if last_name and last_name.lower() in lowered:
            return normalized_title
        return f"{full_name} - {normalized_title}"

    @staticmethod
    def _normalize_briefing_items(
        raw_items: Any,
        fallback_items: List[str],
        *,
        item_kind: str,
    ) -> List[str]:
        source_items = raw_items if isinstance(raw_items, list) else fallback_items
        normalized: List[str] = []
        for raw_item in source_items:
            text = AiOrchestrationService._coerce_briefing_item_text(raw_item, item_kind=item_kind)
            if text:
                normalized.append(text)
            if len(normalized) >= 6:
                break
        if normalized:
            return normalized
        return [str(item) for item in fallback_items][:6]

    @staticmethod
    def _coerce_briefing_item_text(raw_item: Any, *, item_kind: str) -> str:
        if isinstance(raw_item, str):
            compact = " ".join(raw_item.replace("\n", " ").split()).strip()
            return AiOrchestrationService._clip_briefing_text(compact, 170) if compact else ""
        if isinstance(raw_item, dict):
            if item_kind == "highlight":
                note = str(raw_item.get("note") or "").strip()
                metric = str(raw_item.get("metric") or "").strip()
                value = raw_item.get("value")
                formatted_value = AiOrchestrationService._format_briefing_metric_value(value, metric)
                if note and metric:
                    if formatted_value:
                        return AiOrchestrationService._clip_briefing_text(
                            f"{note} ({metric}: {formatted_value})", 170
                        )
                    return AiOrchestrationService._clip_briefing_text(f"{note} ({metric})", 170)
                if note:
                    return AiOrchestrationService._clip_briefing_text(note, 170)
                if metric:
                    return AiOrchestrationService._clip_briefing_text(metric, 170)
            action = str(raw_item.get("action") or "").strip()
            next_steps = raw_item.get("nextSteps")
            if action:
                if isinstance(next_steps, list) and next_steps:
                    next_step_text = str(next_steps[0]).strip()
                    if next_step_text:
                        return f"{action} Next: {next_step_text}"[:220]
                return AiOrchestrationService._clip_briefing_text(action, 170)
            why = str(raw_item.get("why") or "").strip()
            if why:
                return AiOrchestrationService._clip_briefing_text(why, 170)
        return ""

    @staticmethod
    def _format_briefing_metric_value(value: Any, metric: str) -> str:
        if value is None:
            return ""
        try:
            numeric = float(value)
        except (TypeError, ValueError):
            return str(value).strip()[:80]
        metric_lower = metric.lower()
        if 0 <= numeric <= 1.5 and (
            "rate" in metric_lower or "ratio" in metric_lower or "margin" in metric_lower
        ):
            return f"{round(numeric * 100, 1)}%"
        if abs(numeric) >= 1000:
            return f"{numeric:,.0f}"
        return f"{round(numeric, 2)}"

    @staticmethod
    def _clip_briefing_text(text: str, max_len: int) -> str:
        compact = " ".join(text.split()).strip()
        if len(compact) <= max_len:
            return compact
        clipped = compact[:max_len].rstrip()
        if " " in clipped:
            clipped = clipped.rsplit(" ", 1)[0]
        return f"{clipped}..."

