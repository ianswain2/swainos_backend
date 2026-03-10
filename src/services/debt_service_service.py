from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal, ROUND_HALF_UP
from typing import Dict, List, Optional

from src.core.errors import BadRequestError, NotFoundError
from src.repositories.debt_service_repository import DebtServiceRepository
from src.schemas.debt_service import (
    DebtCovenantSnapshot,
    DebtFacility,
    DebtOverviewResponse,
    DebtPaymentCreateRequest,
    DebtPaymentCreateResponse,
    DebtPaymentRecord,
    DebtScenarioResult,
    DebtScenarioSummary,
    DebtScenarioRunRequest,
    DebtSchedulePoint,
)


MONEY_QUANT = Decimal("0.01")


@dataclass
class _AmortizationRow:
    due_date: date
    period_index: int
    opening_balance: Decimal
    payment_amount: Decimal
    principal_amount: Decimal
    interest_amount: Decimal
    remaining_balance: Decimal


class DebtServiceService:
    def __init__(self, repository: DebtServiceRepository) -> None:
        self.repository = repository

    def get_overview(self) -> DebtOverviewResponse:
        items = self.repository.list_overview_items()
        if not items:
            today = date.today()
            return DebtOverviewResponse(
                as_of_date=today,
                facility_count=0,
                outstanding_balance_amount=Decimal("0"),
                next_payment_date=None,
                next_payment_amount=Decimal("0"),
                principal_paid_ytd_amount=Decimal("0"),
                interest_paid_ytd_amount=Decimal("0"),
                scheduled_debt_service_30d_amount=Decimal("0"),
                scheduled_debt_service_60d_amount=Decimal("0"),
                scheduled_debt_service_90d_amount=Decimal("0"),
                dscr_value=None,
                covenant_status="pending_data",
                facilities=[],
            )

        as_of_date = max(item.as_of_date for item in items)
        next_payment_date = min((item.next_due_date for item in items if item.next_due_date), default=None)
        next_payment_amount = Decimal("0")
        if next_payment_date:
            next_payment_amount = sum(
                (item.next_due_amount or Decimal("0"))
                for item in items
                if item.next_due_date == next_payment_date
            )

        principal_ytd = Decimal("0")
        interest_ytd = Decimal("0")
        for item in items:
            if item.as_of_date.year == as_of_date.year:
                principal_ytd += item.principal_paid_to_date_amount
                interest_ytd += item.interest_paid_to_date_amount

        all_compliant = all(item.covenant_in_compliance is not False for item in items)
        status = "in_compliance" if all_compliant else "watch"

        return DebtOverviewResponse(
            as_of_date=as_of_date,
            facility_count=len(items),
            outstanding_balance_amount=sum(item.outstanding_balance_amount for item in items),
            next_payment_date=next_payment_date,
            next_payment_amount=next_payment_amount,
            principal_paid_ytd_amount=principal_ytd,
            interest_paid_ytd_amount=interest_ytd,
            scheduled_debt_service_30d_amount=sum(item.scheduled_debt_service_30d_amount for item in items),
            scheduled_debt_service_60d_amount=sum(item.scheduled_debt_service_60d_amount for item in items),
            scheduled_debt_service_90d_amount=sum(item.scheduled_debt_service_90d_amount for item in items),
            dscr_value=None,
            covenant_status=status,
            facilities=items,
        )

    def list_facilities(self) -> List[DebtFacility]:
        return self.repository.list_facilities()

    def get_schedule(
        self,
        facility_id: str,
        start_date: Optional[date],
        end_date: Optional[date],
    ) -> List[DebtSchedulePoint]:
        return self.repository.list_schedule(facility_id, start_date, end_date)

    def list_payments(self, facility_id: str) -> List[DebtPaymentRecord]:
        return self.repository.list_payments(facility_id)

    def create_payment(self, payload: DebtPaymentCreateRequest) -> DebtPaymentCreateResponse:
        facility = self.repository.get_facility(payload.facility_id)
        if not facility:
            raise NotFoundError("Debt facility not found")

        schedule = self.repository.list_schedule(payload.facility_id, None, None, limit=1)
        if not schedule:
            raise BadRequestError(
                "Debt schedule is not precomputed for this facility. "
                "Run the debt schedule precompute job before posting payments."
            )

        inserted = self.repository.insert_payment(
            payload={
                "facility_id": payload.facility_id,
                "payment_date": payload.payment_date.isoformat(),
                "principal_paid_amount": str(payload.principal_paid_amount),
                "interest_paid_amount": str(payload.interest_paid_amount),
                "extra_principal_amount": str(payload.extra_principal_amount),
                "fee_amount": str(payload.fee_amount),
                "source_account": payload.source_account,
                "reference": payload.reference,
                "notes": payload.notes,
            }
        )

        latest_snapshot = self.repository.get_latest_snapshot(payload.facility_id)
        if latest_snapshot and payload.payment_date < latest_snapshot.as_of_date:
            raise BadRequestError(
                "Backdated payments are not supported in the live posting path. "
                "Use a controlled replay workflow to rebuild snapshots."
            )
        opening_balance = (
            latest_snapshot.outstanding_balance_amount
            if latest_snapshot
            else facility.original_principal_amount
        )
        principal_rollforward = payload.principal_paid_amount + payload.extra_principal_amount
        remaining_balance = max(opening_balance - principal_rollforward, Decimal("0"))

        principal_to_date = (
            (latest_snapshot.principal_paid_to_date_amount if latest_snapshot else Decimal("0"))
            + principal_rollforward
        )
        interest_to_date = (
            (latest_snapshot.interest_paid_to_date_amount if latest_snapshot else Decimal("0"))
            + payload.interest_paid_amount
        )
        extra_principal_to_date = (
            (latest_snapshot.extra_principal_to_date_amount if latest_snapshot else Decimal("0"))
            + payload.extra_principal_amount
        )

        next_due = self.repository.get_next_schedule_after_date(
            facility_id=payload.facility_id,
            as_of_date=payload.payment_date,
        )
        snapshot = self.repository.upsert_snapshot(
            payload={
                "facility_id": payload.facility_id,
                "as_of_date": payload.payment_date.isoformat(),
                "outstanding_balance_amount": str(remaining_balance),
                "principal_paid_to_date_amount": str(principal_to_date),
                "interest_paid_to_date_amount": str(interest_to_date),
                "extra_principal_to_date_amount": str(extra_principal_to_date),
                "next_due_date": next_due.due_date.isoformat() if next_due else None,
                "next_due_amount": str(next_due.scheduled_payment_amount) if next_due else None,
            }
        )

        return DebtPaymentCreateResponse(
            payment_id=inserted.id,
            facility_id=payload.facility_id,
            payment_date=payload.payment_date,
            principal_paid_amount=payload.principal_paid_amount,
            interest_paid_amount=payload.interest_paid_amount,
            extra_principal_amount=payload.extra_principal_amount,
            remaining_balance_amount=snapshot.outstanding_balance_amount,
        )

    def run_scenario(self, payload: DebtScenarioRunRequest) -> DebtScenarioResult:
        baseline = self.get_schedule(payload.facility_id, payload.start_date, None)
        if not baseline:
            raise NotFoundError("Debt schedule not found")
        term = self.repository.get_term_for_date(payload.facility_id, payload.start_date)
        if not term:
            raise NotFoundError("Debt facility term not found for scenario start date")
        overview_items = self.repository.list_overview_items()
        facility_overview = next(
            (item for item in overview_items if item.facility_id == payload.facility_id),
            None,
        )
        scheduled_30d = (
            facility_overview.scheduled_debt_service_30d_amount
            if facility_overview
            else Decimal("0")
        )
        requested_extra = sum((event.extra_principal_amount for event in payload.events), Decimal("0"))
        liquidity_guardrail_passed = requested_extra <= (scheduled_30d * Decimal("0.5"))
        guardrail_reason = None
        if not liquidity_guardrail_passed:
            guardrail_reason = (
                "Requested extra principal exceeds 50% of next-30-day scheduled debt service. "
                "Run cash-flow review before applying accelerated payoff."
            )

        scenario_id = self.repository.create_scenario(
            facility_id=payload.facility_id,
            scenario_name=payload.scenario_name,
            start_date=payload.start_date,
        )
        events_by_date: Dict[date, Decimal] = {}
        event_payloads = []
        for event in payload.events:
            events_by_date[event.event_date] = events_by_date.get(event.event_date, Decimal("0")) + event.extra_principal_amount
            event_payloads.append(
                {
                    "event_date": event.event_date.isoformat(),
                    "extra_principal_amount": str(event.extra_principal_amount),
                    "notes": event.notes,
                }
            )
        self.repository.insert_scenario_events(scenario_id=scenario_id, events=event_payloads)

        baseline_payoff = self._find_payoff_date(baseline)
        baseline_interest = sum((point.scheduled_interest_amount for point in baseline), Decimal("0"))

        scenario_schedule = self._apply_extra_principal_events(
            baseline=baseline,
            events_by_date=events_by_date,
            annual_rate_decimal=self._normalize_annual_rate(term.annual_rate, term.rate_unit),
        )
        scenario_payoff = self._find_payoff_date(scenario_schedule)
        scenario_interest = sum((point.scheduled_interest_amount for point in scenario_schedule), Decimal("0"))
        interest_delta = baseline_interest - scenario_interest

        payoff_date_delta_days = None
        if baseline_payoff and scenario_payoff:
            payoff_date_delta_days = (baseline_payoff - scenario_payoff).days

        self.repository.update_scenario_result(
            scenario_id=scenario_id,
            payoff_date=scenario_payoff,
            total_interest_amount=self._money(scenario_interest),
            total_principal_amount=self._money(
                sum(
                    (point.scheduled_principal_amount + point.extra_principal_applied_amount for point in scenario_schedule),
                    Decimal("0"),
                )
            ),
            total_interest_delta_amount=self._money(interest_delta),
            payoff_date_delta_days=payoff_date_delta_days,
            metadata={"eventCount": len(payload.events)},
        )

        return DebtScenarioResult(
            scenario_id=scenario_id,
            facility_id=payload.facility_id,
            scenario_name=payload.scenario_name,
            start_date=payload.start_date,
            baseline_payoff_date=baseline_payoff,
            scenario_payoff_date=scenario_payoff,
            payoff_date_delta_days=payoff_date_delta_days,
            baseline_total_interest_amount=self._money(baseline_interest),
            scenario_total_interest_amount=self._money(scenario_interest),
            total_interest_delta_amount=self._money(interest_delta),
            liquidity_guardrail_passed=liquidity_guardrail_passed,
            guardrail_reason=guardrail_reason,
        )

    def list_scenarios(self, facility_id: str) -> List[DebtScenarioSummary]:
        return self.repository.list_scenarios(facility_id=facility_id)

    def list_covenant_snapshots(self) -> List[DebtCovenantSnapshot]:
        return self.repository.list_latest_covenant_snapshots()

    def precompute_all_schedules(self) -> dict[str, object]:
        facilities = self.repository.list_facilities()
        generated = 0
        skipped = 0
        for facility in facilities:
            existing = self.repository.list_schedule(facility.id, None, None, limit=1)
            if existing:
                skipped += 1
                continue
            rows = self._generate_and_store_schedule(facility.id)
            if rows:
                generated += 1
        return {
            "facilityCount": len(facilities),
            "generatedFacilityCount": generated,
            "skippedFacilityCount": skipped,
        }

    def _generate_and_store_schedule(self, facility_id: str) -> List[DebtSchedulePoint]:
        facility = self.repository.get_facility(facility_id)
        if not facility:
            raise NotFoundError("Debt facility not found")
        term = self.repository.get_latest_term(facility_id)
        if not term:
            return []

        rows = self._generate_amortization_rows(
            principal=facility.original_principal_amount,
            annual_rate=self._normalize_annual_rate(term.annual_rate, term.rate_unit),
            periods=term.amortization_months,
            first_due_date=facility.first_payment_date or term.effective_start_date,
        )
        insert_rows = [
            {
                "facility_id": facility.id,
                "term_id": term.id,
                "due_date": row.due_date.isoformat(),
                "period_index": row.period_index,
                "opening_balance_amount": str(self._money(row.opening_balance)),
                "scheduled_payment_amount": str(self._money(row.payment_amount)),
                "scheduled_principal_amount": str(self._money(row.principal_amount)),
                "scheduled_interest_amount": str(self._money(row.interest_amount)),
                "extra_principal_applied_amount": "0",
                "remaining_balance_amount": str(self._money(row.remaining_balance)),
                "generated_for_as_of_date": date.today().isoformat(),
            }
            for row in rows
        ]
        inserted = self.repository.upsert_schedule_rows(insert_rows)
        return sorted(inserted, key=lambda value: value.due_date)

    def _generate_amortization_rows(
        self,
        principal: Decimal,
        annual_rate: Decimal,
        periods: int,
        first_due_date: date,
    ) -> List[_AmortizationRow]:
        monthly_rate = annual_rate / Decimal("12")
        payment_amount = self._calculate_payment_amount(principal, monthly_rate, periods)

        rows: List[_AmortizationRow] = []
        balance = principal
        due_date = first_due_date
        for index in range(1, periods + 1):
            if balance <= Decimal("0"):
                break
            interest = self._money(balance * monthly_rate)
            principal_component = min(self._money(payment_amount - interest), balance)
            remaining = self._money(balance - principal_component)
            rows.append(
                _AmortizationRow(
                    due_date=due_date,
                    period_index=index,
                    opening_balance=balance,
                    payment_amount=self._money(principal_component + interest),
                    principal_amount=principal_component,
                    interest_amount=interest,
                    remaining_balance=remaining,
                )
            )
            balance = remaining
            due_date = self._add_month(due_date)
        return rows

    def _calculate_payment_amount(self, principal: Decimal, monthly_rate: Decimal, periods: int) -> Decimal:
        if monthly_rate == 0:
            return self._money(principal / Decimal(periods))
        numerator = principal * monthly_rate
        denominator = Decimal("1") - (Decimal("1") + monthly_rate) ** Decimal(-periods)
        return self._money(numerator / denominator)

    def _add_month(self, value: date) -> date:
        year = value.year + (value.month // 12)
        month = (value.month % 12) + 1
        day = min(value.day, self._days_in_month(year, month))
        return date(year, month, day)

    def _days_in_month(self, year: int, month: int) -> int:
        if month == 2:
            is_leap = year % 4 == 0 and (year % 100 != 0 or year % 400 == 0)
            return 29 if is_leap else 28
        if month in {4, 6, 9, 11}:
            return 30
        return 31

    def _find_payoff_date(self, schedule: List[DebtSchedulePoint]) -> Optional[date]:
        for point in reversed(schedule):
            if point.remaining_balance_amount <= Decimal("0"):
                return point.due_date
        return schedule[-1].due_date if schedule else None

    def _apply_extra_principal_events(
        self,
        baseline: List[DebtSchedulePoint],
        events_by_date: Dict[date, Decimal],
        annual_rate_decimal: Decimal,
    ) -> List[DebtSchedulePoint]:
        adjusted: List[DebtSchedulePoint] = []
        carrying_extra = Decimal("0")
        monthly_rate = annual_rate_decimal / Decimal("12")
        scheduled_payment_amount = baseline[0].scheduled_payment_amount if baseline else Decimal("0")
        opening_balance = baseline[0].opening_balance_amount if baseline else Decimal("0")
        for point in baseline:
            extra = events_by_date.get(point.due_date, Decimal("0")) + carrying_extra
            opening = opening_balance
            interest = self._money(opening * monthly_rate)
            principal_base = self._money(max(scheduled_payment_amount - interest, Decimal("0")))
            principal = principal_base + extra
            if principal > opening:
                carrying_extra = principal - opening
                principal = opening
            else:
                carrying_extra = Decimal("0")
            payment_amount = self._money(principal + interest)
            remaining = self._money(max(opening - principal, Decimal("0")))
            adjusted.append(
                point.model_copy(
                    update={
                        "opening_balance_amount": self._money(opening),
                        "scheduled_payment_amount": payment_amount,
                        "scheduled_principal_amount": self._money(principal),
                        "scheduled_interest_amount": interest,
                        "extra_principal_applied_amount": self._money(extra),
                        "remaining_balance_amount": remaining,
                    }
                )
            )
            if remaining <= Decimal("0"):
                break
            opening_balance = remaining
        return adjusted

    def _money(self, value: Decimal) -> Decimal:
        return value.quantize(MONEY_QUANT, rounding=ROUND_HALF_UP)

    def _normalize_annual_rate(self, annual_rate: Decimal, rate_unit: str) -> Decimal:
        if rate_unit == "percent":
            return annual_rate / Decimal("100")
        if annual_rate > Decimal("1"):
            raise BadRequestError("annual_rate must be decimal fraction (<=1) when rate_unit='decimal'")
        return annual_rate
