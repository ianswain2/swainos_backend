from __future__ import annotations

from collections import defaultdict
from datetime import date, timedelta
import logging
from typing import Dict, List, Optional, Tuple

from src.analytics.booking_forecast import forecast_bookings
from src.analytics.cash_flow import calculate_cashflow_summary, calculate_cashflow_timeseries
from src.core.errors import AppError, NotFoundError
from src.models.revenue_bookings import BookingRecord
from src.repositories.revenue_bookings_repository import RevenueBookingsRepository
from src.schemas.revenue_bookings import (
    ApAging,
    ApPaymentCalendarPoint,
    ApSummary,
    BookingDetail,
    BookingForecastPoint,
    BookingSummary,
    CashFlowApSchedulePoint,
    CashFlowApMonthlyOutflowPoint,
    CashFlowForecastPoint,
    CashFlowForecastResponse,
    CashFlowSummary,
    CashFlowRiskDriver,
    CashFlowRiskOverview,
    CashFlowScenarioSummary,
    CashFlowTimeseriesPoint,
    DepositSummary,
    ItineraryTrendPoint,
    ItineraryLeadFlowPoint,
    ItineraryLeadFlowResponse,
    ItineraryLeadFlowSummary,
    ItineraryTrendsResponse,
    ItineraryTrendsSummary,
    PaymentOutSummary,
)
from src.schemas.common import Lineage


class RevenueBookingsService:
    DEFAULT_BUFFER_RATIO = 0.20
    DEFAULT_COVERAGE_THRESHOLD = 1.0

    def __init__(self, repository: RevenueBookingsRepository) -> None:
        self.repository = repository
        self.logger = logging.getLogger(__name__)

    def list_bookings(
        self,
        start_date: Optional[date],
        end_date: Optional[date],
        currency_code: Optional[str],
        page: int,
        page_size: int,
    ) -> Tuple[List[BookingSummary], int]:
        records, total = self.repository.list_bookings(
            start_date=start_date,
            end_date=end_date,
            currency_code=currency_code,
            page=page,
            page_size=page_size,
        )
        return [self._to_booking_summary(record) for record in records], total

    def get_booking(self, booking_id: str) -> BookingDetail:
        record = self.repository.get_booking_by_id(booking_id)
        if not record:
            raise NotFoundError("Booking not found")
        return self._to_booking_detail(record)

    def get_cashflow_summary(
        self, start_date: date, end_date: date, currency_code: Optional[str]
    ) -> List[CashFlowSummary]:
        payments_in = self.repository.list_customer_payments(
            start_date, end_date, currency_code
        )
        payments_out = self.repository.list_ap_payment_calendar(
            start_date, end_date, currency_code
        )
        return calculate_cashflow_summary(payments_in, payments_out)

    def get_cashflow_timeseries(
        self, start_date: date, end_date: date, currency_code: Optional[str]
    ) -> List[CashFlowTimeseriesPoint]:
        payments_in = self.repository.list_customer_payments(
            start_date, end_date, currency_code
        )
        payments_out = self.repository.list_ap_payment_calendar(
            start_date, end_date, currency_code
        )
        return calculate_cashflow_timeseries(payments_in, payments_out)

    def get_cashflow_risk_overview(
        self, start_date: date, end_date: date, currency_code: Optional[str], time_window: str
    ) -> List[CashFlowRiskOverview]:
        payments_in = self.repository.list_customer_payments(start_date, end_date, currency_code)
        payments_out = self.repository.list_ap_payment_calendar(start_date, end_date, currency_code)
        horizon_days = max((end_date - start_date).days + 1, 1)
        history_end = start_date - timedelta(days=1)
        history_start = history_end - timedelta(days=horizon_days - 1)
        historical_inflows = self.repository.list_customer_payments(history_start, history_end, currency_code)
        projected_daily_inflows = self._estimate_daily_inflows(historical_inflows, horizon_days)
        points_by_currency = self._build_sparse_cashflow_points(
            payments_in, payments_out, projected_daily_inflows, start_date, end_date
        )

        items: List[CashFlowRiskOverview] = []
        for currency, points in sorted(points_by_currency.items()):
            total_in = sum(point["cash_in"] for point in points)
            total_out = sum(point["cash_out"] for point in points)
            projected_ending_cash = points[-1]["projected_ending_cash"] if points else 0.0
            projected_min_cash = min((point["projected_ending_cash"] for point in points), default=0.0)
            coverage_ratio = (total_in / total_out) if total_out > 0 else 1.0
            cash_buffer_amount = total_out * self.DEFAULT_BUFFER_RATIO

            first_risk_date = None
            for point in points:
                if (
                    point["projected_ending_cash"] < 0
                    or point["projected_ending_cash"] < cash_buffer_amount
                    or point["coverage_ratio"] < self.DEFAULT_COVERAGE_THRESHOLD
                ):
                    first_risk_date = point["period_date"]
                    break

            risk_drivers: List[CashFlowRiskDriver] = []
            if projected_ending_cash < 0:
                risk_drivers.append(
                    CashFlowRiskDriver(
                        code="negative_cash",
                        message="Projected ending cash falls below zero in this horizon.",
                    )
                )
            if projected_min_cash < cash_buffer_amount:
                risk_drivers.append(
                    CashFlowRiskDriver(
                        code="buffer_breach",
                        message="Projected cash drops below the operating buffer threshold.",
                    )
                )
            if coverage_ratio < self.DEFAULT_COVERAGE_THRESHOLD:
                risk_drivers.append(
                    CashFlowRiskDriver(
                        code="weak_coverage",
                        message="Projected inflows do not fully cover projected outflows.",
                    )
                )
            if not risk_drivers:
                risk_drivers.append(
                    CashFlowRiskDriver(
                        code="stable",
                        message="No near-term cash stress signals found in this horizon.",
                    )
                )

            if first_risk_date is not None:
                risk_status = "at_risk"
            elif projected_min_cash < (cash_buffer_amount * 1.5):
                risk_status = "watch"
            else:
                risk_status = "healthy"

            time_to_risk_days = (
                (first_risk_date - start_date).days if first_risk_date is not None else None
            )
            items.append(
                CashFlowRiskOverview(
                    currency_code=currency,
                    risk_status=risk_status,
                    first_risk_date=first_risk_date,
                    time_to_risk_days=time_to_risk_days,
                    projected_ending_cash=projected_ending_cash,
                    projected_min_cash=projected_min_cash,
                    cash_buffer_amount=cash_buffer_amount,
                    coverage_ratio=coverage_ratio,
                    risk_drivers=risk_drivers[:3],
                )
            )
        return items

    def get_cashflow_forecast(
        self, start_date: date, end_date: date, currency_code: Optional[str], time_window: str
    ) -> List[CashFlowForecastResponse]:
        payments_in = self.repository.list_customer_payments(start_date, end_date, currency_code)
        payments_out = self.repository.list_ap_payment_calendar(start_date, end_date, currency_code)
        horizon_days = max((end_date - start_date).days + 1, 1)
        history_end = start_date - timedelta(days=1)
        history_start = history_end - timedelta(days=horizon_days - 1)
        historical_inflows = self.repository.list_customer_payments(history_start, history_end, currency_code)
        projected_daily_inflows = self._estimate_daily_inflows(historical_inflows, horizon_days)
        points_by_currency = self._build_sparse_cashflow_points(
            payments_in, payments_out, projected_daily_inflows, start_date, end_date
        )

        responses: List[CashFlowForecastResponse] = []
        for currency, points in sorted(points_by_currency.items()):
            monthly_points = self._to_monthly_forecast_points(currency, points)
            responses.append(
                CashFlowForecastResponse(
                    currency_code=currency,
                    time_window=time_window,
                    points=monthly_points,
                )
            )
        return responses

    def get_cashflow_ap_schedule(
        self, start_date: date, end_date: date, currency_code: Optional[str]
    ) -> List[CashFlowApSchedulePoint]:
        rows = self.repository.list_ap_payment_calendar(start_date, end_date, currency_code)
        return [
            CashFlowApSchedulePoint(
                payment_date=row.payment_date or start_date,
                currency_code=row.currency_code,
                amount_due=row.amount_due,
                line_count=row.line_count,
                supplier_count=row.supplier_count,
            )
            for row in rows
            if row.payment_date is not None
        ]

    def get_cashflow_ap_monthly_outflow(
        self, start_date: date, end_date: date, currency_code: Optional[str]
    ) -> List[CashFlowApMonthlyOutflowPoint]:
        rows = self.repository.list_ap_monthly_outflow(start_date, end_date, currency_code)
        return [
            CashFlowApMonthlyOutflowPoint(
                month_start=row.month_start,
                currency_code=row.currency_code,
                amount_due=row.amount_due,
                line_count=row.line_count,
                supplier_count=row.supplier_count,
            )
            for row in rows
        ]

    def get_cashflow_scenarios(
        self, start_date: date, end_date: date, currency_code: Optional[str], time_window: str
    ) -> List[CashFlowScenarioSummary]:
        baseline = self.get_cashflow_risk_overview(start_date, end_date, currency_code, time_window)
        scenarios: List[CashFlowScenarioSummary] = []
        for row in baseline:
            projected_ending_cash = row.projected_ending_cash + (row.cash_buffer_amount * 0.25)
            scenarios.append(
                CashFlowScenarioSummary(
                    scenario_name="Delay 10% of near-term AP by 30 days",
                    currency_code=row.currency_code,
                    description=(
                        "Read-only simulation showing a timing relief case for a portion "
                        "of upcoming AP obligations."
                    ),
                    projected_ending_cash=projected_ending_cash,
                    first_risk_date=row.first_risk_date,
                    risk_status="watch" if projected_ending_cash < row.cash_buffer_amount else "healthy",
                )
            )
        return scenarios

    def get_deposit_summary(
        self, start_date: date, end_date: date, currency_code: Optional[str]
    ) -> List[DepositSummary]:
        payments_in = self.repository.list_customer_payments(
            start_date, end_date, currency_code
        )
        ap_lines = self.repository.list_ap_open_liabilities(start_date, end_date, currency_code)
        totals = {}
        received_totals = {}
        ap_due_30d_totals = {}

        due_30d_cutoff = date.today() + timedelta(days=30)
        for line in ap_lines:
            if not line.currency_code or line.outstanding_amount is None:
                continue
            liability_due_date = line.effective_payment_date
            # Short-horizon AP pressure used for liquidity posture.
            if liability_due_date is None or liability_due_date <= due_30d_cutoff:
                ap_due_30d_totals[line.currency_code] = (
                    ap_due_30d_totals.get(line.currency_code, 0) + line.outstanding_amount
                )

        for payment in payments_in:
            if not payment.currency_code or payment.amount is None:
                continue
            current = totals.get(payment.currency_code, 0)
            totals[payment.currency_code] = current + payment.amount
            if (payment.payment_status or "").strip().lower() in {"processed", "received", "paid", "cleared"}:
                received_totals[payment.currency_code] = (
                    received_totals.get(payment.currency_code, 0) + payment.amount
                )
        return [
            DepositSummary(
                currency_code=currency,
                total_deposits=amount,
                received_deposits=received_totals.get(currency, 0),
                outstanding_deposits=amount - received_totals.get(currency, 0),
                available_cash_after_liability=received_totals.get(currency, 0)
                - ap_due_30d_totals.get(currency, 0),
            )
            for currency, amount in totals.items()
        ]

    def get_payments_out_summary(
        self, start_date: date, end_date: date, currency_code: Optional[str]
    ) -> List[PaymentOutSummary]:
        ap_lines = self.repository.list_ap_open_liabilities(
            start_date, end_date, currency_code
        )
        pressure_by_currency = self.repository.list_ap_pressure(currency_code)
        totals = {}
        counts = {}
        next_due_by_currency = {}
        for line in ap_lines:
            if not line.currency_code or line.outstanding_amount is None:
                continue
            totals[line.currency_code] = totals.get(line.currency_code, 0) + line.outstanding_amount
            counts[line.currency_code] = counts.get(line.currency_code, 0) + 1
            line_due_date = line.effective_payment_date
            if line_due_date is not None:
                current_next_due = next_due_by_currency.get(line.currency_code)
                if current_next_due is None or line_due_date < current_next_due:
                    next_due_by_currency[line.currency_code] = line_due_date

        summaries: List[PaymentOutSummary] = []
        for currency, total in totals.items():
            pressure = pressure_by_currency.get(currency, {})
            summaries.append(
                PaymentOutSummary(
                    currency_code=currency,
                    open_line_count=counts.get(currency, 0),
                    total_outstanding_amount=total,
                    due_30d_amount=self._coerce_float(pressure.get("due_30d_amount")),
                    next_due_date=next_due_by_currency.get(currency) or pressure.get("next_due_date"),
                )
            )
        return summaries

    def get_ap_summary(self, currency_code: Optional[str]) -> List[ApSummary]:
        summary_rows = self.repository.list_ap_summary(currency_code)
        pressure_by_currency = self.repository.list_ap_pressure(currency_code)
        items: List[ApSummary] = []
        for row in summary_rows:
            pressure = pressure_by_currency.get(row.currency_code, {})
            items.append(
                ApSummary(
                    currency_code=row.currency_code,
                    open_line_count=row.open_line_count,
                    open_booking_count=row.open_booking_count,
                    open_supplier_count=row.open_supplier_count,
                    total_outstanding_amount=row.total_outstanding_amount,
                    due_7d_amount=self._coerce_float(pressure.get("due_7d_amount")),
                    due_30d_amount=self._coerce_float(pressure.get("due_30d_amount")),
                    due_60d_amount=self._coerce_float(pressure.get("due_60d_amount")),
                    due_90d_amount=self._coerce_float(pressure.get("due_90d_amount")),
                    next_due_date=row.next_due_date,
                )
            )
        return items

    def get_ap_aging(self, currency_code: Optional[str]) -> List[ApAging]:
        rows = self.repository.list_ap_aging(currency_code)
        return [
            ApAging(
                currency_code=row.currency_code,
                open_line_count=row.open_line_count,
                total_outstanding_amount=row.total_outstanding_amount,
                current_not_due_amount=row.current_not_due_amount,
                overdue_1_30_amount=row.overdue_1_30_amount,
                overdue_31_60_amount=row.overdue_31_60_amount,
                overdue_61_90_amount=row.overdue_61_90_amount,
                overdue_90_plus_amount=row.overdue_90_plus_amount,
            )
            for row in rows
        ]

    def get_ap_payment_calendar(
        self,
        start_date: date,
        end_date: date,
        currency_code: Optional[str],
    ) -> List[ApPaymentCalendarPoint]:
        rows = self.repository.list_ap_payment_calendar(start_date, end_date, currency_code)
        return [
            ApPaymentCalendarPoint(
                payment_date=row.payment_date,
                currency_code=row.currency_code,
                line_count=row.line_count,
                supplier_count=row.supplier_count,
                amount_due=row.amount_due,
            )
            for row in rows
        ]

    @staticmethod
    def _coerce_float(value: object) -> float:
        if value is None:
            return 0.0
        try:
            return float(value)
        except (TypeError, ValueError):
            return 0.0

    def _build_sparse_cashflow_points(
        self,
        payments_in,
        payments_out,
        projected_daily_inflows: Dict[str, float],
        start_date: date,
        end_date: date,
    ) -> Dict[str, List[Dict[str, float | date]]]:
        per_currency = defaultdict(lambda: defaultdict(lambda: {"cash_in": 0.0, "cash_out": 0.0}))
        for payment in payments_in:
            if not payment.currency_code or payment.payment_date is None or payment.amount is None:
                continue
            entry = per_currency[payment.currency_code][payment.payment_date]
            entry["cash_in"] += float(payment.amount)
        for payment in payments_out:
            if not payment.currency_code or payment.payment_date is None or payment.amount_due is None:
                continue
            entry = per_currency[payment.currency_code][payment.payment_date]
            entry["cash_out"] += float(payment.amount_due)

        for currency, projected_inflow in projected_daily_inflows.items():
            if projected_inflow <= 0:
                continue
            period_date = start_date
            while period_date <= end_date:
                entry = per_currency[currency][period_date]
                entry["cash_in"] += projected_inflow
                period_date += timedelta(days=1)

        output: Dict[str, List[Dict[str, float | date]]] = {}
        for currency, by_date in per_currency.items():
            running_cash = 0.0
            running_in = 0.0
            running_out = 0.0
            points: List[Dict[str, float | date]] = []
            for period_date in sorted(by_date.keys()):
                cash_in = float(by_date[period_date]["cash_in"])
                cash_out = float(by_date[period_date]["cash_out"])
                net_cash = cash_in - cash_out
                running_cash += net_cash
                running_in += cash_in
                running_out += cash_out
                coverage_ratio = (running_in / running_out) if running_out > 0 else 1.0
                points.append(
                    {
                        "period_date": period_date,
                        "cash_in": cash_in,
                        "cash_out": cash_out,
                        "net_cash": net_cash,
                        "projected_ending_cash": running_cash,
                        "coverage_ratio": coverage_ratio,
                    }
                )
            output[currency] = points
        return output

    def _estimate_daily_inflows(self, historical_inflows, history_days: int) -> Dict[str, float]:
        totals: Dict[str, float] = defaultdict(float)
        for payment in historical_inflows:
            if not payment.currency_code or payment.amount is None:
                continue
            totals[payment.currency_code] += float(payment.amount)
        if history_days <= 0:
            return {currency: 0.0 for currency in totals}
        return {currency: amount / history_days for currency, amount in totals.items()}

    def _to_monthly_forecast_points(
        self, currency: str, points: List[Dict[str, float | date]]
    ) -> List[CashFlowForecastPoint]:
        month_buckets: Dict[str, Dict[str, float | date]] = {}
        for point in points:
            period_date = point["period_date"]
            if not isinstance(period_date, date):
                continue
            month_key = f"{period_date.year:04d}-{period_date.month:02d}"
            if month_key not in month_buckets:
                month_start = period_date.replace(day=1)
                month_buckets[month_key] = {
                    "period_start": month_start,
                    "period_end": period_date,
                    "cash_in": 0.0,
                    "cash_out": 0.0,
                    "net_cash": 0.0,
                    "projected_ending_cash": 0.0,
                    "coverage_ratio": 1.0,
                }
            bucket = month_buckets[month_key]
            bucket["period_end"] = period_date
            bucket["cash_in"] = float(bucket["cash_in"]) + float(point["cash_in"])
            bucket["cash_out"] = float(bucket["cash_out"]) + float(point["cash_out"])
            bucket["net_cash"] = float(bucket["net_cash"]) + float(point["net_cash"])
            bucket["projected_ending_cash"] = float(point["projected_ending_cash"])
            bucket["coverage_ratio"] = float(point["coverage_ratio"])

        total_out = sum(float(point["cash_out"]) for point in points)
        cash_buffer_amount = total_out * self.DEFAULT_BUFFER_RATIO
        forecast_points: List[CashFlowForecastPoint] = []
        for month_key in sorted(month_buckets.keys()):
            bucket = month_buckets[month_key]
            projected_ending_cash = float(bucket["projected_ending_cash"])
            coverage_ratio = float(bucket["coverage_ratio"])
            at_risk = (
                projected_ending_cash < 0
                or projected_ending_cash < cash_buffer_amount
                or coverage_ratio < self.DEFAULT_COVERAGE_THRESHOLD
            )
            period_start = bucket["period_start"]
            period_end = bucket["period_end"]
            if not isinstance(period_start, date) or not isinstance(period_end, date):
                continue
            forecast_points.append(
                CashFlowForecastPoint(
                    period_start=period_start,
                    period_end=period_end,
                    cash_in=float(bucket["cash_in"]),
                    cash_out=float(bucket["cash_out"]),
                    net_cash=float(bucket["net_cash"]),
                    projected_ending_cash=projected_ending_cash,
                    coverage_ratio=coverage_ratio,
                    at_risk=at_risk,
                )
            )
        return forecast_points

    def get_booking_forecasts(
        self, lookback_months: int, horizon_months: int
    ) -> List[BookingForecastPoint]:
        records, _ = self.repository.list_bookings(
            start_date=None,
            end_date=None,
            currency_code=None,
            page=1,
            page_size=1000,
            include_count=False,
        )
        return forecast_bookings(records, lookback_months, horizon_months)

    def get_itinerary_trends(self, start_date: date, end_date: date) -> ItineraryTrendsResponse:
        try:
            records = self.repository.list_itinerary_trends(start_date, end_date)
        except Exception as exc:
            self.logger.exception(
                "itinerary_trends_query_failed",
                extra={
                    "start_date": start_date.isoformat(),
                    "end_date": end_date.isoformat(),
                },
            )
            raise AppError(
                code="itinerary_trends_unavailable",
                message="Unable to load itinerary trends right now.",
                status_code=503,
            ) from exc

        timeline = [
            ItineraryTrendPoint(
                period_start=record.period_start,
                created_count=record.created_count,
                closed_count=record.closed_count,
                travel_start_count=record.travel_start_count,
                travel_end_count=record.travel_end_count,
            )
            for record in records
        ]

        created_total = sum(point.created_count for point in timeline)
        closed_total = sum(point.closed_count for point in timeline)
        travel_start_total = sum(point.travel_start_count for point in timeline)
        travel_end_total = sum(point.travel_end_count for point in timeline)

        summary = ItineraryTrendsSummary(
            created_itineraries=created_total,
            closed_itineraries=closed_total,
            travel_start_itineraries=travel_start_total,
            travel_end_itineraries=travel_end_total,
        )

        return ItineraryTrendsResponse(summary=summary, timeline=timeline)

    def get_itinerary_lead_flow(self, start_date: date, end_date: date) -> ItineraryLeadFlowResponse:
        try:
            records = self.repository.list_itinerary_lead_flow(start_date, end_date)
        except Exception as exc:
            self.logger.exception(
                "itinerary_lead_flow_query_failed",
                extra={
                    "start_date": start_date.isoformat(),
                    "end_date": end_date.isoformat(),
                },
            )
            raise AppError(
                code="itinerary_lead_flow_unavailable",
                message="Unable to load itinerary lead flow right now.",
                status_code=503,
            ) from exc

        timeline: List[ItineraryLeadFlowPoint] = []
        for record in records:
            denominator = record.closed_won_count + record.closed_lost_count
            conversion_rate = (record.closed_won_count / denominator) if denominator else 0.0
            timeline.append(
                ItineraryLeadFlowPoint(
                    period_start=record.period_start,
                    created_count=record.created_count,
                    closed_won_count=record.closed_won_count,
                    closed_lost_count=record.closed_lost_count,
                    conversion_rate=round(conversion_rate, 4),
                )
            )

        created_total = sum(point.created_count for point in timeline)
        closed_won_total = sum(point.closed_won_count for point in timeline)
        closed_lost_total = sum(point.closed_lost_count for point in timeline)
        total_closed = closed_won_total + closed_lost_total
        conversion_rate_total = (closed_won_total / total_closed) if total_closed else 0.0

        summary = ItineraryLeadFlowSummary(
            created_itineraries=created_total,
            closed_won_itineraries=closed_won_total,
            closed_lost_itineraries=closed_lost_total,
            conversion_rate=round(conversion_rate_total, 4),
        )

        return ItineraryLeadFlowResponse(summary=summary, timeline=timeline)

    def _to_booking_summary(self, record: BookingRecord) -> BookingSummary:
        return BookingSummary(
            id=record.id,
            booking_number=record.booking_number,
            service_start_date=record.service_start_date,
            service_end_date=record.service_end_date,
            gross_amount=record.gross_amount,
            net_amount=record.net_amount,
            currency_code=record.currency_code,
            itinerary_id=record.itinerary_id,
            lineage=Lineage(
                source_system="salesforce_kaptio",
                source_record_id=record.external_id,
                ingested_at=record.synced_at.isoformat() if record.synced_at else None,
            ),
        )

    def _to_booking_detail(self, record: BookingRecord) -> BookingDetail:
        return BookingDetail(
            **self._to_booking_summary(record).model_dump(),
            supplier_id=record.supplier_id,
            booking_type=record.booking_type,
            service_name=record.service_name,
            location_country=record.location_country,
            location_city=record.location_city,
            confirmation_number=record.confirmation_number,
        )


