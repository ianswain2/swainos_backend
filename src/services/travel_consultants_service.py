from __future__ import annotations

import calendar
from collections import defaultdict
from datetime import date, datetime
from typing import Dict, List, Optional, Tuple

from src.core.errors import NotFoundError
from src.repositories.travel_consultants_repository import TravelConsultantsRepository
from src.schemas.travel_consultants import (
    TravelConsultantComparisonContext,
    TravelConsultantCompensationImpact,
    TravelConsultantForecastFilters,
    TravelConsultantForecastPoint,
    TravelConsultantForecastResponse,
    TravelConsultantForecastSection,
    TravelConsultantForecastSummary,
    TravelConsultantFunnelHealth,
    TravelConsultantHighlight,
    TravelConsultantIdentity,
    TravelConsultantInsightCard,
    TravelConsultantKpiCard,
    TravelConsultantLeaderboardFilters,
    TravelConsultantLeaderboardResponse,
    TravelConsultantLeaderboardRow,
    TravelConsultantOperationalItinerary,
    TravelConsultantOperationalSnapshot,
    TravelConsultantProfileFilters,
    TravelConsultantProfileResponse,
    TravelConsultantSignal,
    TravelConsultantThreeYearMatrix,
    TravelConsultantThreeYearPerformance,
    TravelConsultantThreeYearSeries,
    TravelConsultantThreeYearVariance,
    TravelConsultantTrendStory,
    TravelConsultantTrendStoryPoint,
)

MONTH_LABELS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
SECTION_ORDER = [
    "heroKpis",
    "trendStory",
    "funnelHealth",
    "operationalSnapshot",
    "forecastAndTarget",
    "compensationImpact",
    "signals",
    "insightCards",
]


class TravelConsultantsService:
    def __init__(self, repository: TravelConsultantsRepository) -> None:
        self.repository = repository

    def get_leaderboard(
        self, filters: TravelConsultantLeaderboardFilters
    ) -> TravelConsultantLeaderboardResponse:
        period_start, period_end = self._resolve_period_window(
            filters.period_type, filters.year, filters.month
        )
        baseline_start, baseline_end = self._resolve_baseline_window(
            filters.period_type, period_start, period_end
        )

        travel_rows = self.repository.list_leaderboard_monthly(period_start, period_end)
        funnel_rows = self.repository.list_funnel_monthly(period_start, period_end)
        baseline_rows = self.repository.list_leaderboard_monthly(baseline_start, baseline_end)
        baseline_revenue_by_employee = self._sum_baseline_revenue_by_employee(baseline_rows)
        ytd_current_start, ytd_current_end, ytd_baseline_start, ytd_baseline_end = (
            self._resolve_ytd_comparison_windows(filters.year)
        )
        ytd_current_rows = self.repository.list_leaderboard_monthly(ytd_current_start, ytd_current_end)
        ytd_baseline_rows = self.repository.list_leaderboard_monthly(ytd_baseline_start, ytd_baseline_end)
        ytd_current_revenue_by_employee = self._sum_baseline_revenue_by_employee(ytd_current_rows)
        ytd_baseline_revenue_by_employee = self._sum_baseline_revenue_by_employee(ytd_baseline_rows)

        aggregate = self._aggregate_leaderboard_rows(travel_rows, funnel_rows, filters.domain)
        sort_multiplier = -1 if filters.sort_order == "desc" else 1
        rankings = sorted(
            aggregate.values(),
            key=lambda row: sort_multiplier * self._leaderboard_sort_value(row, filters.sort_by),
        )
        ranked_rows: List[TravelConsultantLeaderboardRow] = []
        for index, row in enumerate(rankings, start=1):
            baseline_revenue = baseline_revenue_by_employee.get(row["employee_id"], 0.0)
            target_revenue = baseline_revenue * 1.12 if baseline_revenue > 0 else 0.0
            growth_variance_pct = (
                (row["booked_revenue"] - target_revenue) / target_revenue if target_revenue else 0.0
            )
            ytd_current_revenue = ytd_current_revenue_by_employee.get(row["employee_id"], 0.0)
            ytd_baseline_revenue = ytd_baseline_revenue_by_employee.get(row["employee_id"], 0.0)
            yoy_to_date_variance_pct = (
                (ytd_current_revenue - ytd_baseline_revenue) / ytd_baseline_revenue
                if ytd_baseline_revenue
                else 0.0
            )
            ranked_rows.append(
                TravelConsultantLeaderboardRow(
                    rank=index,
                    employee_id=row["employee_id"],
                    employee_external_id=row["employee_external_id"],
                    first_name=row["first_name"],
                    last_name=row["last_name"],
                    email=row["email"],
                    itinerary_count=row["itinerary_count"],
                    pax_count=row["pax_count"],
                    booked_revenue=row["booked_revenue"],
                    commission_income=row["commission_income"],
                    margin_amount=row["margin_amount"],
                    margin_pct=row["margin_pct"],
                    lead_count=row["lead_count"],
                    closed_won_count=row["closed_won_count"],
                    closed_lost_count=row["closed_lost_count"],
                    conversion_rate=row["conversion_rate"],
                    close_rate=row["close_rate"],
                    avg_speed_to_book_days=row["avg_speed_to_book_days"],
                    spend_to_book=None,
                    growth_target_variance_pct=round(growth_variance_pct, 4),
                    yoy_to_date_variance_pct=round(yoy_to_date_variance_pct, 4),
                )
            )
        highlights = self._build_leaderboard_highlights(ranked_rows)
        return TravelConsultantLeaderboardResponse(
            period_start=period_start,
            period_end=period_end,
            period_type=filters.period_type,
            domain=filters.domain,
            sort_by=filters.sort_by,
            sort_order=filters.sort_order,
            rankings=ranked_rows,
            highlights=highlights,
        )

    def get_profile(
        self, employee_id: str, filters: TravelConsultantProfileFilters
    ) -> TravelConsultantProfileResponse:
        employee = self._get_employee_identity(employee_id)
        period_start, period_end = self._resolve_period_window(
            filters.period_type, filters.year, filters.month
        )
        profile_rows = self.repository.list_profile_monthly(period_start, period_end, employee_id)
        funnel_rows = self.repository.list_funnel_monthly(period_start, period_end, employee_id)
        compensation_rows = self.repository.list_compensation_monthly(period_start, period_end, employee_id)
        closed_won_status_values = self.repository.list_closed_won_status_values()
        closed_won_itineraries = self.repository.list_closed_won_itineraries_by_travel_period(
            employee_id=employee_id,
            start_date=period_start,
            end_date=period_end,
            closed_won_status_values=closed_won_status_values,
        )

        booked_revenue = sum(self._to_float(row.get("booked_revenue_amount")) for row in profile_rows)
        commission_income = sum(
            self._to_float(row.get("commission_income_amount")) for row in profile_rows
        )
        margin_amount = sum(self._to_float(row.get("margin_amount")) for row in profile_rows)
        margin_pct = (margin_amount / booked_revenue) if booked_revenue else 0.0
        itinerary_count = sum(self._to_int(row.get("itinerary_count")) for row in profile_rows)
        pax_count = sum(self._to_int(row.get("pax_count")) for row in profile_rows)
        avg_gross_profit = (commission_income / itinerary_count) if itinerary_count else 0.0
        avg_group_size = (pax_count / itinerary_count) if itinerary_count else 0.0

        weighted_nights_total = 0.0
        weighted_nights_denominator = 0
        for row in profile_rows:
            row_itinerary_count = self._to_int(row.get("itinerary_count"))
            if row_itinerary_count <= 0:
                continue
            weighted_nights_total += self._to_float(row.get("avg_number_of_nights")) * row_itinerary_count
            weighted_nights_denominator += row_itinerary_count
        avg_itinerary_nights = (
            weighted_nights_total / weighted_nights_denominator if weighted_nights_denominator else 0.0
        )

        lead_count = sum(self._to_int(row.get("lead_count")) for row in funnel_rows)
        closed_won_count = sum(self._to_int(row.get("closed_won_count")) for row in funnel_rows)
        closed_lost_count = sum(self._to_int(row.get("closed_lost_count")) for row in funnel_rows)
        conversion_rate = (closed_won_count / lead_count) if lead_count else 0.0
        close_denominator = closed_won_count + closed_lost_count
        close_rate = (closed_won_count / close_denominator) if close_denominator else 0.0
        speed_values = [
            self._to_float(row.get("median_speed_to_book_days"))
            for row in funnel_rows
            if row.get("median_speed_to_book_days") is not None
        ]
        avg_speed_to_book_days = (sum(speed_values) / len(speed_values)) if speed_values else None

        lead_time_samples: List[float] = []
        speed_to_close_samples: List[float] = []
        for itinerary in closed_won_itineraries:
            created_date = self._optional_datetime_to_date(itinerary.get("created_at"))
            travel_start_date = self._optional_to_date(itinerary.get("travel_start_date"))
            close_date = self._optional_to_date(itinerary.get("close_date"))

            if created_date and travel_start_date:
                lead_time_days = (travel_start_date - created_date).days
                if lead_time_days >= 0:
                    lead_time_samples.append(float(lead_time_days))

            if created_date and close_date:
                speed_to_close_days = (close_date - created_date).days
                if speed_to_close_days >= 0:
                    speed_to_close_samples.append(float(speed_to_close_days))

        avg_lead_time_days = (
            sum(lead_time_samples) / len(lead_time_samples) if lead_time_samples else None
        )
        avg_speed_to_close_days = (
            sum(speed_to_close_samples) / len(speed_to_close_samples) if speed_to_close_samples else None
        )

        trend_story, comparison_context = self._build_trend_story(employee_id, period_end, filters.yoy_mode)
        ytd_variance_pct = self._calculate_employee_ytd_variance(employee_id, period_end.year)
        three_year_performance = self._build_three_year_performance(employee_id, period_end.year)
        forecast_response = self.get_forecast(
            employee_id=employee_id,
            filters=TravelConsultantForecastFilters(horizon_months=12, currency_code=filters.currency_code),
        )
        compensation_impact = self._build_compensation_impact(compensation_rows)
        operational_snapshot = self._build_operational_snapshot(employee_id)
        hero_kpis = self._build_hero_kpis(
            booked_revenue=booked_revenue,
            conversion_rate=conversion_rate,
            close_rate=close_rate,
            margin_pct=margin_pct,
            trend_story=trend_story,
            avg_gross_profit=avg_gross_profit,
            avg_itinerary_nights=avg_itinerary_nights,
            avg_group_size=avg_group_size,
            avg_lead_time_days=avg_lead_time_days,
            avg_speed_to_close_days=avg_speed_to_close_days,
        )
        funnel_health = TravelConsultantFunnelHealth(
            lead_count=lead_count,
            closed_won_count=closed_won_count,
            closed_lost_count=closed_lost_count,
            conversion_rate=round(conversion_rate, 4),
            close_rate=round(close_rate, 4),
            avg_speed_to_book_days=round(avg_speed_to_book_days, 1) if avg_speed_to_book_days is not None else None,
        )
        signals = self._build_signals(
            trend_story=trend_story,
            conversion_rate=conversion_rate,
            close_rate=close_rate,
            margin_pct=margin_pct,
            speed_to_book_days=avg_speed_to_book_days,
        )
        insight_cards = self._build_insight_cards(signals, employee.first_name, itinerary_count)
        return TravelConsultantProfileResponse(
            employee=employee,
            section_order=SECTION_ORDER,
            hero_kpis=hero_kpis,
            trend_story=trend_story,
            three_year_performance=three_year_performance,
            ytd_variance_pct=round(ytd_variance_pct, 4),
            funnel_health=funnel_health,
            forecast_and_target=TravelConsultantForecastSection(
                timeline=forecast_response.timeline,
                summary=forecast_response.summary,
            ),
            compensation_impact=compensation_impact,
            operational_snapshot=operational_snapshot,
            signals=signals,
            insight_cards=insight_cards,
            comparison_context=comparison_context,
        )

    def get_forecast(
        self, employee_id: str, filters: TravelConsultantForecastFilters
    ) -> TravelConsultantForecastResponse:
        employee = self._get_employee_identity(employee_id)
        today = date.today()
        forecast_start = date(today.year, today.month, 1)
        history_start = self._add_months(forecast_start, -24)
        history_end = self._month_end(self._add_months(forecast_start, -1))
        history_rows = self.repository.list_profile_monthly(history_start, history_end, employee_id)
        revenue_by_period = {
            self._to_date(row["period_start"]): self._to_float(row.get("booked_revenue_amount"))
            for row in history_rows
            if row.get("period_start")
        }
        trailing_periods = sorted(revenue_by_period.keys())[-12:]
        trailing_avg = (
            sum(revenue_by_period[period] for period in trailing_periods) / len(trailing_periods)
            if trailing_periods
            else 0.0
        )

        timeline: List[TravelConsultantForecastPoint] = []
        for offset in range(filters.horizon_months):
            period_start = self._add_months(forecast_start, offset)
            period_end = self._month_end(period_start)
            month_samples = [
                amount
                for period, amount in revenue_by_period.items()
                if period.month == period_start.month
            ]
            projected_revenue = (
                sum(month_samples) / len(month_samples) if month_samples else trailing_avg
            )
            last_year_period = self._add_months(period_start, -12)
            last_year_revenue = revenue_by_period.get(last_year_period, 0.0)
            target_revenue = last_year_revenue * 1.12 if last_year_revenue > 0 else projected_revenue * 1.12
            growth_gap_pct = (
                (projected_revenue - target_revenue) / target_revenue if target_revenue else 0.0
            )
            timeline.append(
                TravelConsultantForecastPoint(
                    period_start=period_start,
                    period_end=period_end,
                    projected_revenue_amount=round(projected_revenue, 2),
                    target_revenue_amount=round(target_revenue, 2),
                    growth_gap_pct=round(growth_gap_pct, 4),
                )
            )
        total_projected = sum(item.projected_revenue_amount for item in timeline)
        total_target = sum(item.target_revenue_amount for item in timeline)
        total_gap_pct = ((total_projected - total_target) / total_target) if total_target else 0.0
        summary = TravelConsultantForecastSummary(
            total_projected_revenue_amount=round(total_projected, 2),
            total_target_revenue_amount=round(total_target, 2),
            total_growth_gap_pct=round(total_gap_pct, 4),
        )
        return TravelConsultantForecastResponse(employee=employee, timeline=timeline, summary=summary)

    def _get_employee_identity(self, employee_id: str) -> TravelConsultantIdentity:
        employee = self.repository.get_employee(employee_id)
        if not employee:
            raise NotFoundError("Travel consultant not found")
        return TravelConsultantIdentity(
            employee_id=str(employee["id"]),
            employee_external_id=str(employee.get("external_id") or ""),
            first_name=str(employee.get("first_name") or ""),
            last_name=str(employee.get("last_name") or ""),
            email=str(employee.get("email") or ""),
        )

    def _aggregate_leaderboard_rows(
        self, travel_rows: List[dict], funnel_rows: List[dict], domain: str
    ) -> Dict[str, Dict[str, float | int | str | None]]:
        aggregate: Dict[str, Dict[str, float | int | str | None]] = {}
        for row in travel_rows:
            employee_id = str(row.get("employee_id") or "")
            if not employee_id:
                continue
            bucket = aggregate.setdefault(
                employee_id,
                self._new_leaderboard_bucket(employee_id, row),
            )
            bucket["itinerary_count"] = int(bucket["itinerary_count"]) + self._to_int(
                row.get("itinerary_count")
            )
            bucket["pax_count"] = int(bucket["pax_count"]) + self._to_int(row.get("pax_count"))
            bucket["booked_revenue_travel"] = float(bucket["booked_revenue_travel"]) + self._to_float(
                row.get("booked_revenue_amount")
            )
            bucket["commission_income"] = float(bucket["commission_income"]) + self._to_float(
                row.get("commission_income_amount")
            )
            bucket["margin_amount"] = float(bucket["margin_amount"]) + self._to_float(
                row.get("margin_amount")
            )
        for row in funnel_rows:
            employee_id = str(row.get("employee_id") or "")
            if not employee_id:
                continue
            bucket = aggregate.setdefault(
                employee_id,
                self._new_leaderboard_bucket(employee_id, row),
            )
            bucket["lead_count"] = int(bucket["lead_count"]) + self._to_int(row.get("lead_count"))
            bucket["closed_won_count"] = int(bucket["closed_won_count"]) + self._to_int(
                row.get("closed_won_count")
            )
            bucket["closed_lost_count"] = int(bucket["closed_lost_count"]) + self._to_int(
                row.get("closed_lost_count")
            )
            bucket["booked_revenue_funnel"] = float(bucket["booked_revenue_funnel"]) + self._to_float(
                row.get("booked_revenue_amount")
            )
            speed_to_book_days = row.get("median_speed_to_book_days")
            if speed_to_book_days is not None:
                bucket["speed_samples"].append(self._to_float(speed_to_book_days))
        for bucket in aggregate.values():
            lead_count = int(bucket["lead_count"])
            closed_won_count = int(bucket["closed_won_count"])
            closed_lost_count = int(bucket["closed_lost_count"])
            close_denominator = closed_won_count + closed_lost_count
            bucket["conversion_rate"] = (closed_won_count / lead_count) if lead_count else 0.0
            bucket["close_rate"] = (closed_won_count / close_denominator) if close_denominator else 0.0
            booked_revenue_travel = float(bucket["booked_revenue_travel"])
            booked_revenue_funnel = float(bucket["booked_revenue_funnel"])
            bucket["booked_revenue"] = (
                booked_revenue_funnel if domain == "funnel" else booked_revenue_travel
            )
            bucket["margin_pct"] = (
                float(bucket["margin_amount"]) / booked_revenue_travel if booked_revenue_travel else 0.0
            )
            speed_samples = [value for value in bucket["speed_samples"] if value > 0]
            bucket["avg_speed_to_book_days"] = (
                (sum(speed_samples) / len(speed_samples)) if speed_samples else None
            )
        return aggregate

    @staticmethod
    def _sum_baseline_revenue_by_employee(rows: List[dict]) -> Dict[str, float]:
        result: Dict[str, float] = defaultdict(float)
        for row in rows:
            employee_id = str(row.get("employee_id") or "")
            if not employee_id:
                continue
            result[employee_id] += float(row.get("booked_revenue_amount") or 0.0)
        return result

    @staticmethod
    def _new_leaderboard_bucket(
        employee_id: str, row: dict
    ) -> Dict[str, float | int | str | None]:
        return {
            "employee_id": employee_id,
            "employee_external_id": str(row.get("employee_external_id") or ""),
            "first_name": str(row.get("first_name") or ""),
            "last_name": str(row.get("last_name") or ""),
            "email": str(row.get("email") or ""),
            "itinerary_count": 0,
            "pax_count": 0,
            "booked_revenue_travel": 0.0,
            "booked_revenue_funnel": 0.0,
            "booked_revenue": 0.0,
            "commission_income": 0.0,
            "margin_amount": 0.0,
            "lead_count": 0,
            "closed_won_count": 0,
            "closed_lost_count": 0,
            "speed_samples": [],
            "avg_speed_to_book_days": None,
            "conversion_rate": 0.0,
            "close_rate": 0.0,
            "margin_pct": 0.0,
        }

    def _build_leaderboard_highlights(
        self, rankings: List[TravelConsultantLeaderboardRow]
    ) -> List[TravelConsultantHighlight]:
        if not rankings:
            return []
        top_mover = max(rankings, key=lambda row: row.growth_target_variance_pct)
        best_conversion = max(rankings, key=lambda row: row.conversion_rate)
        margin_risk = min(rankings, key=lambda row: row.margin_pct)
        target_gap_total = sum(
            row.growth_target_variance_pct for row in rankings if row.growth_target_variance_pct < 0
        )
        return [
            TravelConsultantHighlight(
                key="top_mover",
                title="Top Mover",
                description=(
                    f"{top_mover.first_name} {top_mover.last_name} leads target pace "
                    f"({round(top_mover.growth_target_variance_pct * 100, 1)}%)."
                ),
                trend_direction="up",
                trend_strength="high",
            ),
            TravelConsultantHighlight(
                key="best_conversion",
                title="Best Conversion",
                description=(
                    f"{best_conversion.first_name} {best_conversion.last_name} has strongest conversion "
                    f"({round(best_conversion.conversion_rate * 100, 1)}%)."
                ),
                trend_direction="up",
                trend_strength="medium",
            ),
            TravelConsultantHighlight(
                key="margin_risk",
                title="Margin Risk",
                description=(
                    f"{margin_risk.first_name} {margin_risk.last_name} has the lowest margin "
                    f"({round(margin_risk.margin_pct * 100, 1)}%)."
                ),
                trend_direction="down",
                trend_strength="medium",
            ),
            TravelConsultantHighlight(
                key="target_gap",
                title="Team Target Gap",
                description=(
                    f"Combined negative target variance is {round(abs(target_gap_total) * 100, 1)}% "
                    "across lagging consultants."
                ),
                trend_direction="down" if target_gap_total < 0 else "up",
                trend_strength="medium",
            ),
        ]

    def _build_trend_story(
        self, employee_id: str, period_end: date, yoy_mode: str
    ) -> Tuple[TravelConsultantTrendStory, TravelConsultantComparisonContext]:
        current_year = period_end.year
        baseline_year = current_year - 1
        if yoy_mode == "full_year":
            current_start = date(current_year, 1, 1)
            current_end = date(current_year, 12, 31)
            baseline_start = date(baseline_year, 1, 1)
            baseline_end = date(baseline_year, 12, 31)
            month_limit = 12
        else:
            current_start = date(current_year, 1, 1)
            current_end = period_end
            baseline_start = date(baseline_year, 1, 1)
            baseline_end = date(
                baseline_year,
                period_end.month,
                min(period_end.day, calendar.monthrange(baseline_year, period_end.month)[1]),
            )
            month_limit = period_end.month

        current_rows = self.repository.list_profile_monthly(current_start, current_end, employee_id)
        baseline_rows = self.repository.list_profile_monthly(baseline_start, baseline_end, employee_id)
        current_by_month = self._sum_revenue_by_month(current_rows)
        baseline_by_month = self._sum_revenue_by_month(baseline_rows)
        points: List[TravelConsultantTrendStoryPoint] = []
        for month in range(1, month_limit + 1):
            current_value = current_by_month.get(month, 0.0)
            baseline_value = baseline_by_month.get(month, 0.0)
            yoy_delta_pct = ((current_value - baseline_value) / baseline_value) if baseline_value else 0.0
            point_period_start = date(current_year, month, 1)
            points.append(
                TravelConsultantTrendStoryPoint(
                    period_start=point_period_start,
                    period_end=self._month_end(point_period_start),
                    month_label=MONTH_LABELS[month - 1],
                    current_value=round(current_value, 2),
                    baseline_value=round(baseline_value, 2),
                    yoy_delta_pct=round(yoy_delta_pct, 4),
                )
            )
        current_total = sum(point.current_value for point in points)
        baseline_total = sum(point.baseline_value for point in points)
        yoy_total_delta_pct = ((current_total - baseline_total) / baseline_total) if baseline_total else 0.0
        trend_story = TravelConsultantTrendStory(
            points=points,
            current_total=round(current_total, 2),
            baseline_total=round(baseline_total, 2),
            yoy_delta_pct=round(yoy_total_delta_pct, 4),
        )
        context = TravelConsultantComparisonContext(
            current_period=f"{current_start.isoformat()}..{current_end.isoformat()}",
            baseline_period=f"{baseline_start.isoformat()}..{baseline_end.isoformat()}",
            yoy_mode=yoy_mode,
        )
        return trend_story, context

    @staticmethod
    def _sum_revenue_by_month(rows: List[dict]) -> Dict[int, float]:
        totals: Dict[int, float] = defaultdict(float)
        for row in rows:
            period_start = row.get("period_start")
            if not period_start:
                continue
            period = date.fromisoformat(period_start) if isinstance(period_start, str) else period_start
            totals[period.month] += float(row.get("booked_revenue_amount") or 0.0)
        return totals

    @staticmethod
    def _build_compensation_impact(rows: List[dict]) -> TravelConsultantCompensationImpact:
        if not rows:
            return TravelConsultantCompensationImpact(
                salary_annual_amount=0.0,
                salary_period_amount=0.0,
                commission_rate=0.15,
                estimated_commission_amount=0.0,
                estimated_total_pay_amount=0.0,
            )
        salary_annual_amount = float(rows[-1].get("salary_annual_amount") or 0.0)
        commission_rate = float(rows[-1].get("commission_rate") or 0.15)
        salary_period_amount = sum(float(row.get("salary_monthly_amount") or 0.0) for row in rows)
        estimated_commission_amount = sum(
            float(row.get("estimated_commission_amount") or 0.0) for row in rows
        )
        estimated_total_pay_amount = sum(
            float(row.get("estimated_total_pay_amount") or 0.0) for row in rows
        )
        return TravelConsultantCompensationImpact(
            salary_annual_amount=round(salary_annual_amount, 2),
            salary_period_amount=round(salary_period_amount, 2),
            commission_rate=round(commission_rate, 4),
            estimated_commission_amount=round(estimated_commission_amount, 2),
            estimated_total_pay_amount=round(estimated_total_pay_amount, 2),
        )

    @staticmethod
    def _build_hero_kpis(
        booked_revenue: float,
        conversion_rate: float,
        close_rate: float,
        margin_pct: float,
        trend_story: TravelConsultantTrendStory,
        avg_gross_profit: float,
        avg_itinerary_nights: float,
        avg_group_size: float,
        avg_lead_time_days: Optional[float],
        avg_speed_to_close_days: Optional[float],
    ) -> List[TravelConsultantKpiCard]:
        trend_direction = "up" if trend_story.yoy_delta_pct >= 0 else "down"
        trend_strength = "high" if abs(trend_story.yoy_delta_pct) >= 0.1 else "medium"
        return [
            TravelConsultantKpiCard(
                key="booked_revenue",
                display_label="Booked Revenue",
                description="Closed-won realized travel revenue for selected period.",
                value=round(booked_revenue, 2),
                trend_direction=trend_direction,
                trend_strength=trend_strength,
                is_lagging_indicator=False,
            ),
            TravelConsultantKpiCard(
                key="conversion_rate",
                display_label="Conversion Rate",
                description="Closed won divided by lead count.",
                value=round(conversion_rate, 4),
                trend_direction="up" if conversion_rate >= 0.35 else "down",
                trend_strength="medium",
                is_lagging_indicator=True,
            ),
            TravelConsultantKpiCard(
                key="close_rate",
                display_label="Close Rate",
                description="Closed won divided by closed won plus closed lost.",
                value=round(close_rate, 4),
                trend_direction="up" if close_rate >= 0.45 else "down",
                trend_strength="medium",
                is_lagging_indicator=True,
            ),
            TravelConsultantKpiCard(
                key="margin_pct",
                display_label="Margin %",
                description="Margin amount divided by booked revenue.",
                value=round(margin_pct, 4),
                trend_direction="up" if margin_pct >= 0.2 else "down",
                trend_strength="medium",
                is_lagging_indicator=True,
            ),
            TravelConsultantKpiCard(
                key="avg_gross_profit",
                display_label="Average Gross Profit",
                description="Average gross profit per closed-won itinerary in selected period.",
                value=round(avg_gross_profit, 2),
                trend_direction="up" if avg_gross_profit > 0 else "flat",
                trend_strength="medium",
                is_lagging_indicator=False,
            ),
            TravelConsultantKpiCard(
                key="avg_itinerary_nights",
                display_label="Average Itinerary Nights",
                description="Weighted average itinerary nights for closed-won travel.",
                value=round(avg_itinerary_nights, 1),
                trend_direction="up" if avg_itinerary_nights >= 6 else "flat",
                trend_strength="low",
                is_lagging_indicator=False,
            ),
            TravelConsultantKpiCard(
                key="avg_group_size",
                display_label="Average Group Size",
                description="Average passengers per closed-won itinerary.",
                value=round(avg_group_size, 1),
                trend_direction="up" if avg_group_size >= 2 else "flat",
                trend_strength="low",
                is_lagging_indicator=False,
            ),
            TravelConsultantKpiCard(
                key="avg_lead_time",
                display_label="Average Lead Time",
                description="Average days from lead created to travel start for closed-won itineraries.",
                value=round(avg_lead_time_days or 0.0, 1),
                trend_direction="up" if (avg_lead_time_days or 0.0) >= 30 else "down",
                trend_strength="low",
                is_lagging_indicator=False,
            ),
            TravelConsultantKpiCard(
                key="avg_speed_to_close",
                display_label="Average Speed to Close",
                description="Average days from lead created to booking close date.",
                value=round(avg_speed_to_close_days or 0.0, 1),
                trend_direction="up" if (avg_speed_to_close_days or 0.0) <= 45 else "down",
                trend_strength="medium",
                is_lagging_indicator=True,
            ),
        ]

    @staticmethod
    def _build_signals(
        trend_story: TravelConsultantTrendStory,
        conversion_rate: float,
        close_rate: float,
        margin_pct: float,
        speed_to_book_days: Optional[float],
    ) -> List[TravelConsultantSignal]:
        signals: List[TravelConsultantSignal] = []
        signals.append(
            TravelConsultantSignal(
                key="growth_target",
                display_label="12% Growth Trajectory",
                description=(
                    "Current year-over-year pace is "
                    f"{round(trend_story.yoy_delta_pct * 100, 1)}% against 12% target."
                ),
                trend_direction="up" if trend_story.yoy_delta_pct >= 0.12 else "down",
                trend_strength="high" if abs(trend_story.yoy_delta_pct) >= 0.12 else "medium",
                is_lagging_indicator=True,
            )
        )
        signals.append(
            TravelConsultantSignal(
                key="conversion_health",
                display_label="Conversion Health",
                description=f"Conversion rate is {round(conversion_rate * 100, 1)}% for selected period.",
                trend_direction="up" if conversion_rate >= 0.35 else "down",
                trend_strength="medium",
                is_lagging_indicator=True,
            )
        )
        signals.append(
            TravelConsultantSignal(
                key="close_rate",
                display_label="Close Rate Stability",
                description=f"Close rate is {round(close_rate * 100, 1)}% for selected period.",
                trend_direction="up" if close_rate >= 0.45 else "down",
                trend_strength="medium",
                is_lagging_indicator=True,
            )
        )
        signals.append(
            TravelConsultantSignal(
                key="margin_compression",
                display_label="Margin Compression",
                description=f"Margin is {round(margin_pct * 100, 1)}% on current revenue mix.",
                trend_direction="up" if margin_pct >= 0.2 else "down",
                trend_strength="medium",
                is_lagging_indicator=True,
            )
        )
        if speed_to_book_days is not None:
            signals.append(
                TravelConsultantSignal(
                    key="speed_to_book",
                    display_label="Speed to Book",
                    description=f"Average speed to book is {round(speed_to_book_days, 1)} days.",
                    trend_direction="up" if speed_to_book_days <= 45 else "down",
                    trend_strength="medium",
                    is_lagging_indicator=True,
                )
            )
        return signals

    @staticmethod
    def _build_insight_cards(
        signals: List[TravelConsultantSignal], first_name: str, itinerary_count: int
    ) -> List[TravelConsultantInsightCard]:
        cards: List[TravelConsultantInsightCard] = []
        if not signals:
            return cards
        cards.append(
            TravelConsultantInsightCard(
                title="Performance Snapshot",
                description=(
                    f"{first_name} handled {itinerary_count} itineraries in scope with current signal mix."
                ),
                trend_direction="up",
                trend_strength="low",
            )
        )
        for signal in signals[:3]:
            cards.append(
                TravelConsultantInsightCard(
                    title=signal.display_label,
                    description=signal.description,
                    trend_direction=signal.trend_direction,
                    trend_strength=signal.trend_strength,
                )
            )
        return cards

    @staticmethod
    def _leaderboard_sort_value(row: Dict[str, float | int | str | None], sort_by: str) -> float:
        if sort_by == "booked_revenue":
            return float(row["booked_revenue"])
        if sort_by == "conversion_rate":
            return float(row["conversion_rate"])
        if sort_by == "close_rate":
            return float(row["close_rate"])
        if sort_by == "margin_pct":
            return float(row["margin_pct"])
        return float(row["booked_revenue"])

    @staticmethod
    def _resolve_period_window(
        period_type: str, year: Optional[int], month: Optional[int]
    ) -> Tuple[date, date]:
        today = date.today()
        if period_type == "rolling12":
            end_month_start = date(today.year, today.month, 1)
            period_start = TravelConsultantsService._add_months(end_month_start, -11)
            period_end = TravelConsultantsService._month_end(end_month_start)
            return period_start, period_end
        if period_type == "year":
            period_year = year or today.year
            return date(period_year, 1, 1), date(period_year, 12, 31)
        period_year = year or today.year
        period_month = month or today.month
        period_start = date(period_year, period_month, 1)
        period_end = TravelConsultantsService._month_end(period_start)
        return period_start, period_end

    @staticmethod
    def _resolve_ytd_comparison_windows(
        selected_year: Optional[int],
    ) -> Tuple[date, date, date, date]:
        today = date.today()
        target_year = selected_year or today.year
        # Month-grain YTD: Jan -> current month for current year, otherwise full Jan-Dec.
        ytd_month = today.month if target_year == today.year else 12
        current_start = date(target_year, 1, 1)
        current_end = TravelConsultantsService._month_end(date(target_year, ytd_month, 1))
        baseline_year = target_year - 1
        baseline_start = date(baseline_year, 1, 1)
        baseline_end = TravelConsultantsService._month_end(date(baseline_year, ytd_month, 1))
        return current_start, current_end, baseline_start, baseline_end

    @staticmethod
    def _resolve_baseline_window(period_type: str, period_start: date, period_end: date) -> Tuple[date, date]:
        if period_type == "rolling12":
            baseline_start = TravelConsultantsService._add_months(period_start, -12)
            baseline_end = TravelConsultantsService._add_months(period_end, -12)
            return baseline_start, baseline_end
        if period_type == "year":
            baseline_year = period_start.year - 1
            return date(baseline_year, 1, 1), date(baseline_year, 12, 31)
        return date(period_start.year - 1, period_start.month, 1), date(
            period_end.year - 1, period_end.month, min(
                period_end.day, calendar.monthrange(period_end.year - 1, period_end.month)[1]
            )
        )

    def _build_operational_snapshot(self, employee_id: str) -> TravelConsultantOperationalSnapshot:
        today = date.today()
        open_status_values = self.repository.list_open_status_values()
        traveling_rows = self.repository.list_current_traveling_itineraries(employee_id, today, limit=10)
        open_rows = self.repository.list_top_open_itineraries(employee_id, open_status_values, limit=5)
        return TravelConsultantOperationalSnapshot(
            current_traveling_files=[self._map_operational_itinerary(row) for row in traveling_rows],
            top_open_itineraries=[self._map_operational_itinerary(row) for row in open_rows],
        )

    def _calculate_employee_ytd_variance(self, employee_id: str, target_year: int) -> float:
        current_start, current_end, baseline_start, baseline_end = self._resolve_ytd_comparison_windows(
            target_year
        )
        current_rows = self.repository.list_leaderboard_monthly(current_start, current_end, employee_id)
        baseline_rows = self.repository.list_leaderboard_monthly(baseline_start, baseline_end, employee_id)
        current_total = sum(self._to_float(row.get("booked_revenue_amount")) for row in current_rows)
        baseline_total = sum(self._to_float(row.get("booked_revenue_amount")) for row in baseline_rows)
        return ((current_total - baseline_total) / baseline_total) if baseline_total else 0.0

    def _build_three_year_performance(
        self, employee_id: str, anchor_year: int
    ) -> TravelConsultantThreeYearPerformance:
        years = [anchor_year - 2, anchor_year - 1, anchor_year]
        range_start = date(years[0], 1, 1)
        range_end = date(years[-1], 12, 31)
        travel_rows = self.repository.list_profile_monthly(range_start, range_end, employee_id)
        funnel_rows = self.repository.list_funnel_monthly(range_start, range_end, employee_id)

        travel_by_year_month: Dict[int, Dict[int, float]] = {year: defaultdict(float) for year in years}
        funnel_by_year_month: Dict[int, Dict[int, float]] = {year: defaultdict(float) for year in years}

        for row in travel_rows:
            period_start = row.get("period_start")
            if not period_start:
                continue
            period_date = self._to_date(period_start)
            if period_date.year not in travel_by_year_month:
                continue
            travel_by_year_month[period_date.year][period_date.month] += self._to_float(
                row.get("booked_revenue_amount")
            )

        for row in funnel_rows:
            period_start = row.get("period_start")
            if not period_start:
                continue
            period_date = self._to_date(period_start)
            if period_date.year not in funnel_by_year_month:
                continue
            funnel_by_year_month[period_date.year][period_date.month] += self._to_float(
                row.get("booked_revenue_amount")
            )

        return TravelConsultantThreeYearPerformance(
            travel_closed_files=self._build_three_year_matrix(
                key="travel_closed_files",
                title="Closed Travel Revenue (Travel Date Basis)",
                metric_label="revenue",
                years=years,
                values_by_year_month=travel_by_year_month,
            ),
            lead_funnel=self._build_three_year_matrix(
                key="lead_funnel",
                title="Lead Funnel Revenue (Created/Booked Basis)",
                metric_label="revenue",
                years=years,
                values_by_year_month=funnel_by_year_month,
            ),
        )

    def _build_three_year_matrix(
        self,
        key: str,
        title: str,
        metric_label: str,
        years: List[int],
        values_by_year_month: Dict[int, Dict[int, float]],
    ) -> TravelConsultantThreeYearMatrix:
        series: List[TravelConsultantThreeYearSeries] = []
        for year in years:
            monthly_values = [
                round(values_by_year_month.get(year, {}).get(month, 0.0), 2) for month in range(1, 13)
            ]
            total = round(sum(monthly_values), 2)
            series.append(
                TravelConsultantThreeYearSeries(
                    year=year,
                    monthly_values=monthly_values,
                    total=total,
                )
            )

        variances: List[TravelConsultantThreeYearVariance] = []
        for index in range(1, len(series)):
            current = series[index]
            baseline = series[index - 1]
            monthly_variance_pct = []
            for month_index in range(12):
                baseline_value = baseline.monthly_values[month_index]
                current_value = current.monthly_values[month_index]
                variance = ((current_value - baseline_value) / baseline_value) if baseline_value else 0.0
                monthly_variance_pct.append(round(variance, 4))
            total_variance_pct = (
                ((current.total - baseline.total) / baseline.total) if baseline.total else 0.0
            )
            variances.append(
                TravelConsultantThreeYearVariance(
                    label=f"{current.year} vs {baseline.year}",
                    monthly_variance_pct=monthly_variance_pct,
                    total_variance_pct=round(total_variance_pct, 4),
                )
            )

        return TravelConsultantThreeYearMatrix(
            key=key,
            title=title,
            metric_label=metric_label,
            series=series,
            variances=variances,
        )

    def _map_operational_itinerary(self, row: dict) -> TravelConsultantOperationalItinerary:
        return TravelConsultantOperationalItinerary(
            itinerary_id=str(row.get("id") or ""),
            itinerary_number=str(row.get("itinerary_number") or ""),
            itinerary_name=str(row.get("itinerary_name") or "") or None,
            itinerary_status=str(row.get("itinerary_status") or ""),
            primary_country=str(row.get("primary_country") or "") or None,
            travel_start_date=self._optional_to_date(row.get("travel_start_date")),
            travel_end_date=self._optional_to_date(row.get("travel_end_date")),
            gross_amount=self._to_float(row.get("gross_amount")),
            pax_count=self._to_int(row.get("pax_count")),
        )

    @staticmethod
    def _month_end(period_start: date) -> date:
        last_day = calendar.monthrange(period_start.year, period_start.month)[1]
        return date(period_start.year, period_start.month, last_day)

    @staticmethod
    def _add_months(base: date, months: int) -> date:
        month_index = base.month - 1 + months
        year = base.year + month_index // 12
        month = month_index % 12 + 1
        return date(year, month, 1)

    @staticmethod
    def _to_int(value: object) -> int:
        if value is None:
            return 0
        return int(float(value))

    @staticmethod
    def _to_float(value: object) -> float:
        if value is None:
            return 0.0
        return float(value)

    @staticmethod
    def _to_date(value: object) -> date:
        if isinstance(value, date):
            return value
        return date.fromisoformat(str(value))

    @staticmethod
    def _optional_to_date(value: object) -> Optional[date]:
        if value in (None, ""):
            return None
        if isinstance(value, date):
            return value
        return date.fromisoformat(str(value))

    @staticmethod
    def _optional_datetime_to_date(value: object) -> Optional[date]:
        if value in (None, ""):
            return None
        if isinstance(value, datetime):
            return value.date()
        if isinstance(value, date):
            return value
        text = str(value)
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            return datetime.fromisoformat(text).date()
        except ValueError:
            return None
