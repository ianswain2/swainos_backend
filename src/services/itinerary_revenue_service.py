from __future__ import annotations

from collections import defaultdict
from datetime import date, timedelta
from typing import Dict, List, Optional, Tuple

from src.repositories.itinerary_pipeline_repository import ItineraryPipelineRepository
from src.repositories.itinerary_revenue_repository import ItineraryRevenueRepository
from src.schemas.itinerary_revenue import (
    ItineraryActualsYoyMonthPoint,
    ItineraryActualsYoyResponse,
    ItineraryActualsYoyYearSummary,
    ItineraryChannelPoint,
    ItineraryChannelsResponse,
    ItineraryConversionPoint,
    ItineraryConversionResponse,
    ItineraryDepositsResponse,
    ItineraryDepositTrendPoint,
    ItineraryRevenueOutlookPoint,
    ItineraryRevenueOutlookResponse,
    ItineraryRevenueOutlookSummary,
    ItineraryTradeDirectBreakdown,
    ItineraryTradeDirectMonthPoint,
    ItineraryTradeDirectTotals,
)
from src.shared.time import parse_forward_time_window, parse_time_window


class ItineraryRevenueService:
    def __init__(
        self,
        revenue_repository: ItineraryRevenueRepository,
        pipeline_repository: ItineraryPipelineRepository,
    ) -> None:
        self.revenue_repository = revenue_repository
        self.pipeline_repository = pipeline_repository

    def get_outlook(
        self,
        time_window: str,
        grain: str,
    ) -> ItineraryRevenueOutlookResponse:
        start_date, end_date = parse_forward_time_window(time_window)
        revenue_rows = self.revenue_repository.list_revenue_outlook(start_date, end_date, grain)
        close_ratio = self._calculate_lookback_close_ratio()
        history_start = start_date - timedelta(days=730)
        history_end = start_date - timedelta(days=1)
        historical_rows = self.revenue_repository.list_revenue_outlook(history_start, history_end, "monthly")
        timeline = self._build_outlook_timeline(revenue_rows, close_ratio, historical_rows)
        summary = self._build_outlook_summary(timeline)
        return ItineraryRevenueOutlookResponse(
            summary=summary,
            timeline=timeline,
            close_ratio=close_ratio,
        )

    def get_deposits(self, time_window: str) -> ItineraryDepositsResponse:
        # Deposit_received is confirmation/collection behavior; trailing window is more representative.
        start_date, end_date = parse_time_window(time_window)
        rows = self.revenue_repository.list_deposit_trends(start_date, end_date)
        timeline = [
            ItineraryDepositTrendPoint.model_validate(row)
            for row in rows
        ]
        return ItineraryDepositsResponse(timeline=timeline)

    def get_conversion(self, time_window: str, grain: str) -> ItineraryConversionResponse:
        start_date, end_date = parse_forward_time_window(time_window)
        timeline_rows = self.pipeline_repository.list_stage_trends(start_date, end_date)
        ratio_expected, ratio_best, ratio_worst = self._calculate_close_ratio_scenarios()
        income_yield = self._calculate_commission_income_yield()
        timeline_by_period: Dict[date, Dict[str, float]] = defaultdict(
            lambda: {
                "quoted": 0.0,
                "confirmed": 0.0,
                "quoted_gross": 0.0,
                "confirmed_gross": 0.0,
                "period_end_ordinal": 0.0,
            }
        )
        for row in timeline_rows:
            bucket = timeline_by_period[row.period_start]
            bucket["period_end_ordinal"] = row.period_end.toordinal()
            if row.stage == "Quoted":
                bucket["quoted"] += row.itinerary_count
                bucket["quoted_gross"] += row.gross_amount
            elif row.stage in ("Confirmed", "Traveling", "Traveled"):
                bucket["confirmed"] += row.itinerary_count
                bucket["confirmed_gross"] += row.gross_amount

        timeline: List[ItineraryConversionPoint] = []
        for period_start in sorted(timeline_by_period.keys()):
            values = timeline_by_period[period_start]
            quoted_count = int(values["quoted"])
            confirmed_count = int(values["confirmed"])
            denominator = quoted_count + confirmed_count
            close_ratio = (confirmed_count / denominator) if denominator else 0.0
            projected_confirmed_count = quoted_count * ratio_expected
            projected_gross_expected = values["confirmed_gross"] + (values["quoted_gross"] * ratio_expected)
            projected_gross_best = values["confirmed_gross"] + (values["quoted_gross"] * ratio_best)
            projected_gross_worst = values["confirmed_gross"] + (values["quoted_gross"] * ratio_worst)
            timeline.append(
                ItineraryConversionPoint(
                    period_start=period_start,
                    period_end=date.fromordinal(int(values["period_end_ordinal"])),
                    quoted_count=quoted_count,
                    confirmed_count=confirmed_count,
                    close_ratio=round(close_ratio, 4),
                    projected_confirmed_count=round(projected_confirmed_count, 2),
                    projected_commission_income_expected=round(projected_gross_expected * income_yield, 2),
                    projected_commission_income_best_case=round(projected_gross_best * income_yield, 2),
                    projected_commission_income_worst_case=round(projected_gross_worst * income_yield, 2),
                )
            )
        return ItineraryConversionResponse(
            timeline=timeline,
            lookback_close_ratio=ratio_expected,
        )

    def get_channels(self, time_window: str) -> ItineraryChannelsResponse:
        start_date, end_date = parse_forward_time_window(time_window)
        consortia_rows = self.revenue_repository.list_consortia_channels(start_date, end_date)
        agency_rows = self.revenue_repository.list_trade_agency_channels(start_date, end_date)

        consortia_rollup = self._rollup_channels(consortia_rows, label_key="consortia")
        agency_rollup = self._rollup_channels(agency_rows, label_key="agency_name")
        top_consortia = sorted(consortia_rollup.values(), key=lambda item: item.gross_amount, reverse=True)[
            :10
        ]
        top_trade_agencies = sorted(
            agency_rollup.values(), key=lambda item: item.gross_amount, reverse=True
        )[:10]

        return ItineraryChannelsResponse(
            top_consortia=top_consortia,
            top_trade_agencies=top_trade_agencies,
        )

    def get_actuals_yoy(self, years_back: int) -> ItineraryActualsYoyResponse:
        current_year = date.today().year
        first_year = current_year - years_back + 1
        start_date = date(first_year, 1, 1)
        end_date = date(current_year, 12, 31)
        rows = self.revenue_repository.list_actuals_yoy(start_date, end_date)
        consortia_rows = self.revenue_repository.list_actuals_consortia_channels(start_date, end_date)

        by_period: Dict[Tuple[int, int], Dict[str, float]] = defaultdict(
            lambda: {
                "itinerary_count": 0.0,
                "pax_count": 0.0,
                "gross_amount": 0.0,
                "commission_income_amount": 0.0,
                "margin_amount": 0.0,
                "trade_commission_amount": 0.0,
                "days_weighted_sum": 0.0,
                "nights_weighted_sum": 0.0,
            }
        )

        for row in rows:
            period_value = row.get("period_start")
            period_start = date.fromisoformat(period_value) if isinstance(period_value, str) else period_value
            if not period_start:
                continue
            key = (period_start.year, period_start.month)
            bucket = by_period[key]
            itinerary_count = float(row.get("itinerary_count") or 0.0)
            pax_count = float(row.get("pax_count") or 0.0)
            gross_amount = float(row.get("gross_amount") or 0.0)
            commission_income_amount = float(row.get("commission_income_amount") or 0.0)
            margin_amount = float(
                row.get("margin_amount") or (gross_amount - commission_income_amount)
            )
            trade_commission_amount = float(row.get("trade_commission_amount") or 0.0)
            avg_number_of_days = float(row.get("avg_number_of_days") or 0.0)
            avg_number_of_nights = float(row.get("avg_number_of_nights") or 0.0)

            bucket["itinerary_count"] += itinerary_count
            bucket["pax_count"] += pax_count
            bucket["gross_amount"] += gross_amount
            bucket["commission_income_amount"] += commission_income_amount
            bucket["margin_amount"] += margin_amount
            bucket["trade_commission_amount"] += trade_commission_amount
            bucket["days_weighted_sum"] += avg_number_of_days * itinerary_count
            bucket["nights_weighted_sum"] += avg_number_of_nights * itinerary_count

        years = list(range(first_year, current_year + 1))
        month_labels = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]

        year_totals: Dict[int, Dict[str, float]] = defaultdict(
            lambda: {"itinerary_count": 0.0, "pax_count": 0.0, "gross_amount": 0.0, "commission_income_amount": 0.0, "margin_amount": 0.0, "trade_commission_amount": 0.0, "days_weighted_sum": 0.0, "nights_weighted_sum": 0.0}
        )
        for year in years:
            for month in range(1, 13):
                values = by_period[(year, month)]
                totals = year_totals[year]
                totals["itinerary_count"] += values["itinerary_count"]
                totals["pax_count"] += values["pax_count"]
                totals["gross_amount"] += values["gross_amount"]
                totals["commission_income_amount"] += values["commission_income_amount"]
                totals["margin_amount"] += values["margin_amount"]
                totals["trade_commission_amount"] += values["trade_commission_amount"]
                totals["days_weighted_sum"] += values["days_weighted_sum"]
                totals["nights_weighted_sum"] += values["nights_weighted_sum"]

        timeline: List[ItineraryActualsYoyMonthPoint] = []
        for year in years:
            gross_total_for_year = year_totals[year]["gross_amount"]
            itinerary_total_for_year = year_totals[year]["itinerary_count"]
            for month in range(1, 13):
                values = by_period[(year, month)]
                itinerary_count = int(round(values["itinerary_count"]))
                pax_count = int(round(values["pax_count"]))
                gross_amount = values["gross_amount"]
                commission_income_amount = values["commission_income_amount"]
                margin_amount = values["margin_amount"]
                margin_pct = (margin_amount / gross_amount) if gross_amount else 0.0
                avg_gross_per_itinerary = (gross_amount / values["itinerary_count"]) if values["itinerary_count"] else 0.0
                avg_commission_income_per_itinerary = (
                    commission_income_amount / values["itinerary_count"]
                ) if values["itinerary_count"] else 0.0
                avg_gross_per_pax = (gross_amount / values["pax_count"]) if values["pax_count"] else 0.0
                avg_commission_income_per_pax = (
                    commission_income_amount / values["pax_count"]
                ) if values["pax_count"] else 0.0
                avg_number_of_days = (
                    values["days_weighted_sum"] / values["itinerary_count"]
                    if values["itinerary_count"]
                    else 0.0
                )
                avg_number_of_nights = (
                    values["nights_weighted_sum"] / values["itinerary_count"]
                    if values["itinerary_count"]
                    else 0.0
                )
                gross_share_of_year_pct = (
                    gross_amount / gross_total_for_year if gross_total_for_year else 0.0
                )
                itinerary_share_of_year_pct = (
                    values["itinerary_count"] / itinerary_total_for_year
                    if itinerary_total_for_year
                    else 0.0
                )

                timeline.append(
                    ItineraryActualsYoyMonthPoint(
                        year=year,
                        month=month,
                        month_label=month_labels[month - 1],
                        itinerary_count=itinerary_count,
                        pax_count=pax_count,
                        gross_amount=gross_amount,
                        commission_income_amount=commission_income_amount,
                        margin_amount=margin_amount,
                        trade_commission_amount=values["trade_commission_amount"],
                        margin_pct=margin_pct,
                        avg_gross_per_itinerary=avg_gross_per_itinerary,
                        avg_commission_income_per_itinerary=avg_commission_income_per_itinerary,
                        avg_gross_per_pax=avg_gross_per_pax,
                        avg_commission_income_per_pax=avg_commission_income_per_pax,
                        avg_number_of_days=avg_number_of_days,
                        avg_number_of_nights=avg_number_of_nights,
                        gross_share_of_year_pct=gross_share_of_year_pct,
                        itinerary_share_of_year_pct=itinerary_share_of_year_pct,
                    )
                )

        year_summaries: List[ItineraryActualsYoyYearSummary] = []
        for year in years:
            totals = year_totals[year]
            gross_amount = totals["gross_amount"]
            commission_income_amount = totals["commission_income_amount"]
            margin_amount = totals["margin_amount"]
            margin_pct = (margin_amount / gross_amount) if gross_amount else 0.0
            itinerary_count = int(round(totals["itinerary_count"]))
            pax_count = int(round(totals["pax_count"]))
            year_summaries.append(
                ItineraryActualsYoyYearSummary(
                    year=year,
                    itinerary_count=itinerary_count,
                    pax_count=pax_count,
                    gross_amount=gross_amount,
                    commission_income_amount=commission_income_amount,
                    margin_amount=margin_amount,
                    trade_commission_amount=totals["trade_commission_amount"],
                    margin_pct=margin_pct,
                    avg_gross_per_itinerary=(gross_amount / totals["itinerary_count"]) if totals["itinerary_count"] else 0.0,
                    avg_commission_income_per_itinerary=(commission_income_amount / totals["itinerary_count"]) if totals["itinerary_count"] else 0.0,
                    avg_gross_per_pax=(gross_amount / totals["pax_count"]) if totals["pax_count"] else 0.0,
                    avg_commission_income_per_pax=(commission_income_amount / totals["pax_count"]) if totals["pax_count"] else 0.0,
                    avg_number_of_days=(totals["days_weighted_sum"] / totals["itinerary_count"]) if totals["itinerary_count"] else 0.0,
                    avg_number_of_nights=(totals["nights_weighted_sum"] / totals["itinerary_count"]) if totals["itinerary_count"] else 0.0,
                )
            )

        trade_vs_direct = self._build_trade_vs_direct_breakdown(
            consortia_rows=consortia_rows,
            first_year=first_year,
            current_year=current_year,
        )

        return ItineraryActualsYoyResponse(
            years=years,
            timeline=timeline,
            year_summaries=year_summaries,
            trade_vs_direct=trade_vs_direct,
        )

    def get_actuals_channels(
        self, years_back: int, actuals_year: int | None = None
    ) -> ItineraryChannelsResponse:
        current_year = date.today().year
        if actuals_year is not None:
            start_date = date(actuals_year, 1, 1)
            end_date = date(actuals_year, 12, 31)
        else:
            first_year = current_year - years_back + 1
            start_date = date(first_year, 1, 1)
            end_date = date(current_year, 12, 31)
        consortia_rows = self.revenue_repository.list_actuals_consortia_channels(start_date, end_date)
        agency_rows = self.revenue_repository.list_actuals_trade_agency_channels(start_date, end_date)
        consortia_rollup = self._rollup_channels(consortia_rows, label_key="consortia")
        agency_rollup = self._rollup_channels(agency_rows, label_key="agency_name")
        top_consortia = sorted(consortia_rollup.values(), key=lambda item: item.gross_amount, reverse=True)[
            :10
        ]
        top_trade_agencies = sorted(
            agency_rollup.values(), key=lambda item: item.gross_amount, reverse=True
        )[:10]
        return ItineraryChannelsResponse(
            top_consortia=top_consortia,
            top_trade_agencies=top_trade_agencies,
        )

    def _build_outlook_timeline(
        self, revenue_rows: List[dict], close_ratio: float, historical_rows: List[dict]
    ) -> List[ItineraryRevenueOutlookPoint]:
        by_period: Dict[Tuple[str, str], Dict[str, float]] = defaultdict(
            lambda: {
                "on_books_gross": 0.0,
                "potential_gross_raw": 0.0,
                "potential_gross_weighted": 0.0,
                "on_books_commission_income": 0.0,
                "potential_commission_income_raw": 0.0,
                "potential_commission_income_weighted": 0.0,
                "on_books_pax": 0.0,
                "potential_pax_raw": 0.0,
                "potential_pax_weighted": 0.0,
            }
        )

        for row in revenue_rows:
            key = (row["period_start"], row["period_end"])
            bucket = by_period[key]
            bucket_name = str(row.get("pipeline_bucket") or "").lower()
            gross = float(row.get("gross_amount") or 0.0)
            commission_income = float(row.get("commission_income_amount") or 0.0)
            pax = float(row.get("pax_count") or 0.0)

            if bucket_name in {"closed_won"}:
                bucket["on_books_gross"] += gross
                bucket["on_books_commission_income"] += commission_income
                bucket["on_books_pax"] += pax
            elif bucket_name in {"open", "holding"}:
                bucket["potential_gross_raw"] += gross
                bucket["potential_gross_weighted"] += gross * close_ratio
                bucket["potential_commission_income_raw"] += commission_income
                bucket["potential_commission_income_weighted"] += commission_income * close_ratio
                bucket["potential_pax_raw"] += pax
                bucket["potential_pax_weighted"] += pax * close_ratio

        forecast_periods = [period_start for period_start, _ in sorted(by_period.keys())]
        historical_model = self._build_historical_forecast_model(
            historical_rows=historical_rows,
            close_ratio=close_ratio,
            forecast_periods=forecast_periods,
        )
        timeline: List[ItineraryRevenueOutlookPoint] = []
        for period_start, period_end in sorted(by_period.keys()):
            values = by_period[(period_start, period_end)]
            expected_gross = values["on_books_gross"] + values["potential_gross_weighted"]
            expected_commission_income = (
                values["on_books_commission_income"] + values["potential_commission_income_weighted"]
            )
            expected_margin = expected_gross - expected_commission_income
            expected_margin_pct = expected_margin / expected_gross if expected_gross else 0.0
            forecast_metrics = historical_model.get(period_start, {})
            forecast_gross = float(forecast_metrics.get("forecast_gross", expected_gross))
            target_gross = float(forecast_metrics.get("target_gross", forecast_gross * 1.12))
            forecast_commission_income = float(
                forecast_metrics.get("forecast_commission_income", expected_commission_income)
            )
            target_commission_income = float(
                forecast_metrics.get("target_commission_income", forecast_commission_income * 1.12)
            )
            forecast_pax = float(
                forecast_metrics.get("forecast_pax", values["on_books_pax"] + values["potential_pax_weighted"])
            )
            target_pax = float(forecast_metrics.get("target_pax", forecast_pax * 1.12))

            timeline.append(
                ItineraryRevenueOutlookPoint(
                    period_start=period_start,
                    period_end=period_end,
                    on_books_gross_amount=values["on_books_gross"],
                    potential_gross_amount=values["potential_gross_raw"],
                    expected_gross_amount=expected_gross,
                    on_books_commission_income_amount=values["on_books_commission_income"],
                    potential_commission_income_amount=values["potential_commission_income_raw"],
                    expected_commission_income_amount=expected_commission_income,
                    on_books_pax_count=int(round(values["on_books_pax"])),
                    potential_pax_count=values["potential_pax_raw"],
                    expected_pax_count=values["on_books_pax"] + values["potential_pax_weighted"],
                    expected_margin_amount=expected_margin,
                    expected_margin_pct=expected_margin_pct,
                    forecast_gross_amount=forecast_gross,
                    target_gross_amount=target_gross,
                    forecast_commission_income_amount=forecast_commission_income,
                    target_commission_income_amount=target_commission_income,
                    forecast_pax_count=forecast_pax,
                    target_pax_count=target_pax,
                )
            )
        return timeline

    @staticmethod
    def _build_outlook_summary(
        timeline: List[ItineraryRevenueOutlookPoint],
    ) -> ItineraryRevenueOutlookSummary:
        total_on_books_gross_amount = sum(item.on_books_gross_amount for item in timeline)
        total_potential_gross_amount = sum(item.potential_gross_amount for item in timeline)
        total_expected_gross_amount = sum(item.expected_gross_amount for item in timeline)
        total_expected_commission_income_amount = sum(
            item.expected_commission_income_amount for item in timeline
        )
        total_expected_margin_amount = sum(item.expected_margin_amount for item in timeline)
        total_on_books_pax_count = sum(item.on_books_pax_count for item in timeline)
        total_potential_pax_count = sum(item.potential_pax_count for item in timeline)
        total_expected_pax_count = sum(item.expected_pax_count for item in timeline)
        total_forecast_gross_amount = sum(item.forecast_gross_amount for item in timeline)
        total_target_gross_amount = sum(item.target_gross_amount for item in timeline)
        total_forecast_commission_income_amount = sum(
            item.forecast_commission_income_amount for item in timeline
        )
        total_target_commission_income_amount = sum(
            item.target_commission_income_amount for item in timeline
        )
        total_forecast_pax_count = sum(item.forecast_pax_count for item in timeline)
        total_target_pax_count = sum(item.target_pax_count for item in timeline)
        return ItineraryRevenueOutlookSummary(
            total_on_books_gross_amount=total_on_books_gross_amount,
            total_potential_gross_amount=total_potential_gross_amount,
            total_expected_gross_amount=total_expected_gross_amount,
            total_expected_commission_income_amount=total_expected_commission_income_amount,
            total_expected_margin_amount=total_expected_margin_amount,
            total_on_books_pax_count=total_on_books_pax_count,
            total_potential_pax_count=total_potential_pax_count,
            total_expected_pax_count=total_expected_pax_count,
            total_forecast_gross_amount=total_forecast_gross_amount,
            total_target_gross_amount=total_target_gross_amount,
            total_forecast_commission_income_amount=total_forecast_commission_income_amount,
            total_target_commission_income_amount=total_target_commission_income_amount,
            total_forecast_pax_count=total_forecast_pax_count,
            total_target_pax_count=total_target_pax_count,
        )

    @staticmethod
    def _build_historical_forecast_model(
        historical_rows: List[dict], close_ratio: float, forecast_periods: List[str]
    ) -> Dict[str, Dict[str, float]]:
        monthly_closed_won: Dict[str, Dict[str, float]] = defaultdict(
            lambda: {"gross": 0.0, "income": 0.0, "pax": 0.0, "open_income": 0.0}
        )
        for row in historical_rows:
            period_start = str(row.get("period_start") or "")[:10]
            if not period_start:
                continue
            bucket = str(row.get("pipeline_bucket") or "").lower()
            gross = float(row.get("gross_amount") or 0.0)
            income = float(row.get("commission_income_amount") or 0.0)
            pax = float(row.get("pax_count") or 0.0)
            month_bucket = monthly_closed_won[period_start]
            if bucket == "closed_won":
                month_bucket["gross"] += gross
                month_bucket["income"] += income
                month_bucket["pax"] += pax
            elif bucket in {"open", "holding"}:
                month_bucket["open_income"] += income

        ordered_periods = sorted(monthly_closed_won.keys())
        trailing_periods = ordered_periods[-12:]
        trailing_gross_avg = (
            sum(monthly_closed_won[period]["gross"] for period in trailing_periods) / len(trailing_periods)
            if trailing_periods
            else 0.0
        )
        trailing_income_avg = (
            sum(monthly_closed_won[period]["income"] for period in trailing_periods) / len(trailing_periods)
            if trailing_periods
            else 0.0
        )
        trailing_pax_avg = (
            sum(monthly_closed_won[period]["pax"] for period in trailing_periods) / len(trailing_periods)
            if trailing_periods
            else 0.0
        )
        historical_close_ratios = []
        for period in trailing_periods:
            closed_income = monthly_closed_won[period]["income"]
            open_income = monthly_closed_won[period]["open_income"]
            denominator = closed_income + open_income
            if denominator > 0:
                historical_close_ratios.append(closed_income / denominator)
        avg_historical_close = (
            sum(historical_close_ratios) / len(historical_close_ratios)
            if historical_close_ratios
            else close_ratio
        )
        close_factor = 1.0
        if avg_historical_close > 0:
            close_factor = max(0.8, min(1.2, close_ratio / avg_historical_close))

        by_month: Dict[int, Dict[str, List[float]]] = defaultdict(
            lambda: {"gross": [], "income": [], "pax": []}
        )
        for period in ordered_periods[-24:]:
            parsed = date.fromisoformat(period)
            by_month[parsed.month]["gross"].append(monthly_closed_won[period]["gross"])
            by_month[parsed.month]["income"].append(monthly_closed_won[period]["income"])
            by_month[parsed.month]["pax"].append(monthly_closed_won[period]["pax"])

        result: Dict[str, Dict[str, float]] = {}
        for future_month in range(1, 13):
            samples = by_month[future_month]
            seasonal_gross = (
                sum(samples["gross"]) / len(samples["gross"]) if samples["gross"] else trailing_gross_avg
            )
            seasonal_income = (
                sum(samples["income"]) / len(samples["income"]) if samples["income"] else trailing_income_avg
            )
            seasonal_pax = (
                sum(samples["pax"]) / len(samples["pax"]) if samples["pax"] else trailing_pax_avg
            )
            modeled_gross = (seasonal_gross * 0.7 + trailing_gross_avg * 0.3) * close_factor
            modeled_income = (seasonal_income * 0.7 + trailing_income_avg * 0.3) * close_factor
            modeled_pax = (seasonal_pax * 0.7 + trailing_pax_avg * 0.3) * close_factor
            result[str(future_month)] = {
                "forecast_gross": modeled_gross,
                "forecast_income": modeled_income,
                "forecast_pax": modeled_pax,
            }

        target_by_period: Dict[str, Dict[str, float]] = {}
        for period in ordered_periods:
            period_dt = date.fromisoformat(period)
            target_by_period[period] = {
                "gross": monthly_closed_won[period]["gross"],
                "income": monthly_closed_won[period]["income"],
                "pax": monthly_closed_won[period]["pax"],
                "month": float(period_dt.month),
            }

        modeled: Dict[str, Dict[str, float]] = {}
        for month in range(1, 13):
            key = str(month)
            modeled[key] = {
                "forecast_gross": result[key]["forecast_gross"],
                "forecast_commission_income": result[key]["forecast_income"],
                "forecast_pax": result[key]["forecast_pax"],
            }

        mapped: Dict[str, Dict[str, float]] = {}
        for period in forecast_periods:
            period_dt = date.fromisoformat(str(period)[:10])
            last_year_period = date(period_dt.year - 1, period_dt.month, 1).isoformat()
            last_year = target_by_period.get(last_year_period)
            month_model = modeled[str(period_dt.month)]
            mapped[period] = {
                "forecast_gross": month_model["forecast_gross"],
                "forecast_commission_income": month_model["forecast_commission_income"],
                "forecast_pax": month_model["forecast_pax"],
                "target_gross": (
                    last_year["gross"] * 1.12 if last_year and last_year["gross"] > 0 else month_model["forecast_gross"] * 1.12
                ),
                "target_commission_income": (
                    last_year["income"] * 1.12
                    if last_year and last_year["income"] > 0
                    else month_model["forecast_commission_income"] * 1.12
                ),
                "target_pax": (
                    last_year["pax"] * 1.12 if last_year and last_year["pax"] > 0 else month_model["forecast_pax"] * 1.12
                ),
            }
        return mapped

    @staticmethod
    def _rollup_channels(rows: List[dict], label_key: str) -> Dict[str, ItineraryChannelPoint]:
        rollup: Dict[str, ItineraryChannelPoint] = {}
        for row in rows:
            label = str(row.get(label_key) or "Unassigned")
            itinerary_count = int(row.get("itinerary_count") or 0)
            pax_count = int(row.get("pax_count") or 0)
            gross_amount = float(row.get("gross_amount") or 0.0)
            net_amount = float(row.get("net_amount") or 0.0)
            commission_income_amount = float(row.get("commission_income_amount") or 0.0)
            margin_amount_raw = row.get("margin_amount")
            margin_amount = (
                float(margin_amount_raw) if margin_amount_raw is not None else (gross_amount - net_amount)
            )
            trade_commission_amount = float(row.get("trade_commission_amount") or 0.0)

            current = rollup.get(label)
            if not current:
                rollup[label] = ItineraryChannelPoint(
                    label=label,
                    itinerary_count=itinerary_count,
                    pax_count=pax_count,
                    gross_amount=gross_amount,
                    commission_income_amount=commission_income_amount,
                    margin_amount=margin_amount,
                    trade_commission_amount=trade_commission_amount,
                )
                continue

            current.itinerary_count += itinerary_count
            current.pax_count += pax_count
            current.gross_amount += gross_amount
            current.commission_income_amount += commission_income_amount
            current.margin_amount += margin_amount
            current.trade_commission_amount += trade_commission_amount
        return rollup

    def _calculate_lookback_close_ratio(self) -> float:
        lookback_start, lookback_end = parse_time_window("12m")
        lookback_rows = self.pipeline_repository.list_stage_trends(lookback_start, lookback_end)
        quoted = sum(float(item.itinerary_count) for item in lookback_rows if item.stage == "Quoted")
        confirmed = sum(
            float(item.itinerary_count)
            for item in lookback_rows
            if item.stage in ("Confirmed", "Traveling", "Traveled")
        )
        stage_denominator = confirmed + quoted
        stage_ratio: Optional[float] = None
        if stage_denominator > 0:
            stage_ratio = confirmed / stage_denominator

        revenue_rows = self.revenue_repository.list_revenue_outlook(
            lookback_start, lookback_end, "monthly"
        )
        bucket_closed_won = sum(
            float(row.get("itinerary_count") or 0.0)
            for row in revenue_rows
            if str(row.get("pipeline_bucket") or "").lower() == "closed_won"
        )
        bucket_open = sum(
            float(row.get("itinerary_count") or 0.0)
            for row in revenue_rows
            if str(row.get("pipeline_bucket") or "").lower() in {"open", "holding"}
        )
        bucket_denominator = bucket_closed_won + bucket_open
        bucket_ratio: Optional[float] = None
        if bucket_denominator > 0:
            bucket_ratio = bucket_closed_won / bucket_denominator

        if stage_ratio is not None and bucket_ratio is not None:
            blended = (stage_ratio * 0.5) + (bucket_ratio * 0.5)
        elif stage_ratio is not None:
            blended = stage_ratio
        elif bucket_ratio is not None:
            blended = bucket_ratio
        else:
            blended = 0.0
        # Guardrails avoid runaway 0%/100% rates when one source is sparse.
        bounded = max(0.1, min(0.9, blended))
        return round(bounded, 4)

    def _calculate_close_ratio_scenarios(self) -> Tuple[float, float, float]:
        lookback_start, lookback_end = parse_time_window("12m")
        lookback_rows = self.pipeline_repository.list_stage_trends(lookback_start, lookback_end)
        by_period: Dict[date, Dict[str, float]] = defaultdict(
            lambda: {"quoted": 0.0, "confirmed": 0.0}
        )
        for row in lookback_rows:
            period = row.period_start
            if row.stage == "Quoted":
                by_period[period]["quoted"] += row.itinerary_count
            elif row.stage in ("Confirmed", "Traveling", "Traveled"):
                by_period[period]["confirmed"] += row.itinerary_count

        monthly_ratios: List[float] = []
        for period in sorted(by_period.keys()):
            quoted = by_period[period]["quoted"]
            confirmed = by_period[period]["confirmed"]
            denominator = quoted + confirmed
            if denominator > 0:
                monthly_ratios.append(confirmed / denominator)

        if not monthly_ratios:
            base = self._calculate_lookback_close_ratio()
            return base, base, base

        sorted_ratios = sorted(monthly_ratios)
        p25 = sorted_ratios[int((len(sorted_ratios) - 1) * 0.25)]
        p75 = sorted_ratios[int((len(sorted_ratios) - 1) * 0.75)]
        base = self._calculate_lookback_close_ratio()
        best = max(base, p75)
        worst = min(base, p25)
        return round(base, 4), round(best, 4), round(worst, 4)

    def _calculate_commission_income_yield(self) -> float:
        lookback_start, lookback_end = parse_time_window("12m")
        rows = self.revenue_repository.list_revenue_outlook(lookback_start, lookback_end, "monthly")
        closed_won = [row for row in rows if str(row.get("pipeline_bucket") or "").lower() == "closed_won"]
        gross_total = sum(float(row.get("gross_amount") or 0.0) for row in closed_won)
        income_total = sum(float(row.get("commission_income_amount") or 0.0) for row in closed_won)
        if gross_total <= 0:
            return 0.0
        return max(0.0, min(1.0, income_total / gross_total))

    @staticmethod
    def _build_trade_vs_direct_breakdown(
        consortia_rows: List[dict], first_year: int, current_year: int
    ) -> ItineraryTradeDirectBreakdown:
        by_period: Dict[date, Dict[str, float]] = defaultdict(
            lambda: {
                "period_end_ordinal": 0.0,
                "direct_itinerary_count": 0.0,
                "trade_itinerary_count": 0.0,
                "direct_pax_count": 0.0,
                "trade_pax_count": 0.0,
                "direct_gross_amount": 0.0,
                "trade_gross_amount": 0.0,
                "direct_commission_income_amount": 0.0,
                "trade_commission_income_amount": 0.0,
                "direct_margin_amount": 0.0,
                "trade_margin_amount": 0.0,
            }
        )

        for row in consortia_rows:
            period_start_value = row.get("period_start")
            period_end_value = row.get("period_end")
            period_start = (
                date.fromisoformat(period_start_value)
                if isinstance(period_start_value, str)
                else period_start_value
            )
            period_end = (
                date.fromisoformat(period_end_value)
                if isinstance(period_end_value, str)
                else period_end_value
            )
            if not period_start:
                continue

            bucket = by_period[period_start]
            if period_end:
                bucket["period_end_ordinal"] = period_end.toordinal()

            consortia_value = str(row.get("consortia") or "").strip().lower()
            category = (
                "direct"
                if consortia_value in {"not applicable", "n/a", "na", "not_applicable"}
                else "trade"
            )
            bucket[f"{category}_itinerary_count"] += float(row.get("itinerary_count") or 0.0)
            bucket[f"{category}_pax_count"] += float(row.get("pax_count") or 0.0)
            bucket[f"{category}_gross_amount"] += float(row.get("gross_amount") or 0.0)
            bucket[f"{category}_commission_income_amount"] += float(
                row.get("commission_income_amount") or 0.0
            )
            bucket[f"{category}_margin_amount"] += float(row.get("margin_amount") or 0.0)

        timeline: List[ItineraryTradeDirectMonthPoint] = []
        for year in range(first_year, current_year + 1):
            for month in range(1, 13):
                period_start = date(year, month, 1)
                default_period_end = (
                    date(year + 1, 1, 1) - timedelta(days=1)
                    if month == 12
                    else date(year, month + 1, 1) - timedelta(days=1)
                )
                values = by_period.get(period_start)
                if not values:
                    values = by_period[period_start]
                    values["period_end_ordinal"] = default_period_end.toordinal()

                period_end_ordinal = int(values["period_end_ordinal"]) if values["period_end_ordinal"] else 0
                period_end = (
                    date.fromordinal(period_end_ordinal)
                    if period_end_ordinal
                    else default_period_end
                )

                timeline.append(
                    ItineraryTradeDirectMonthPoint(
                        period_start=period_start,
                        period_end=period_end,
                        direct_itinerary_count=int(round(values["direct_itinerary_count"])),
                        trade_itinerary_count=int(round(values["trade_itinerary_count"])),
                        direct_pax_count=int(round(values["direct_pax_count"])),
                        trade_pax_count=int(round(values["trade_pax_count"])),
                        direct_gross_amount=values["direct_gross_amount"],
                        trade_gross_amount=values["trade_gross_amount"],
                        direct_commission_income_amount=values["direct_commission_income_amount"],
                        trade_commission_income_amount=values["trade_commission_income_amount"],
                        direct_margin_amount=values["direct_margin_amount"],
                        trade_margin_amount=values["trade_margin_amount"],
                    )
                )

        totals = ItineraryTradeDirectTotals(
            direct_itinerary_count=sum(point.direct_itinerary_count for point in timeline),
            trade_itinerary_count=sum(point.trade_itinerary_count for point in timeline),
            direct_pax_count=sum(point.direct_pax_count for point in timeline),
            trade_pax_count=sum(point.trade_pax_count for point in timeline),
            direct_gross_amount=sum(point.direct_gross_amount for point in timeline),
            trade_gross_amount=sum(point.trade_gross_amount for point in timeline),
            direct_commission_income_amount=sum(
                point.direct_commission_income_amount for point in timeline
            ),
            trade_commission_income_amount=sum(
                point.trade_commission_income_amount for point in timeline
            ),
            direct_margin_amount=sum(point.direct_margin_amount for point in timeline),
            trade_margin_amount=sum(point.trade_margin_amount for point in timeline),
        )
        return ItineraryTradeDirectBreakdown(timeline=timeline, totals=totals)
