from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import List, Optional

from pydantic import Field

from src.shared.base import BaseSchema


class DebtFacility(BaseSchema):
    id: str
    external_id: Optional[str] = None
    lender_name: str
    facility_name: str
    facility_type: str
    original_principal_amount: Decimal
    currency_code: str
    origination_date: date
    first_payment_date: Optional[date] = None
    maturity_date: date
    payment_day_of_month: Optional[int] = None
    prepayment_penalty_mode: str
    status: str
    notes: Optional[str] = None
    created_at: str
    updated_at: str


class DebtFacilityTerm(BaseSchema):
    id: str
    facility_id: str
    effective_start_date: date
    effective_end_date: Optional[date] = None
    rate_mode: str
    rate_unit: str
    annual_rate: Decimal
    payment_frequency: str
    amortization_months: int
    scheduled_payment_amount: Optional[Decimal] = None
    recast_on_extra_principal: bool
    created_at: str
    updated_at: str


class DebtSchedulePoint(BaseSchema):
    id: str
    facility_id: str
    term_id: str
    due_date: date
    period_index: int
    opening_balance_amount: Decimal
    scheduled_payment_amount: Decimal
    scheduled_principal_amount: Decimal
    scheduled_interest_amount: Decimal
    extra_principal_applied_amount: Decimal
    remaining_balance_amount: Decimal
    generated_for_as_of_date: Optional[date] = None
    created_at: str
    updated_at: str


class DebtPaymentRecord(BaseSchema):
    id: str
    facility_id: str
    schedule_id: Optional[str] = None
    payment_date: date
    principal_paid_amount: Decimal
    interest_paid_amount: Decimal
    extra_principal_amount: Decimal
    fee_amount: Decimal
    source_account: Optional[str] = None
    reference: Optional[str] = None
    notes: Optional[str] = None
    entered_by: Optional[str] = None
    created_at: str
    updated_at: str


class DebtBalanceSnapshot(BaseSchema):
    id: str
    facility_id: str
    as_of_date: date
    outstanding_balance_amount: Decimal
    principal_paid_to_date_amount: Decimal
    interest_paid_to_date_amount: Decimal
    extra_principal_to_date_amount: Decimal
    next_due_date: Optional[date] = None
    next_due_amount: Optional[Decimal] = None
    created_at: str
    updated_at: str


class DebtOverviewItem(BaseSchema):
    facility_id: str
    facility_name: str
    currency_code: str
    as_of_date: date
    outstanding_balance_amount: Decimal
    principal_paid_to_date_amount: Decimal
    interest_paid_to_date_amount: Decimal
    extra_principal_to_date_amount: Decimal
    next_due_date: Optional[date] = None
    next_due_amount: Optional[Decimal] = None
    scheduled_debt_service_30d_amount: Decimal
    scheduled_debt_service_60d_amount: Decimal
    scheduled_debt_service_90d_amount: Decimal
    covenant_in_compliance: Optional[bool] = None


class DebtOverviewResponse(BaseSchema):
    as_of_date: date
    facility_count: int
    outstanding_balance_amount: Decimal
    next_payment_date: Optional[date] = None
    next_payment_amount: Decimal
    principal_paid_ytd_amount: Decimal
    interest_paid_ytd_amount: Decimal
    scheduled_debt_service_30d_amount: Decimal
    scheduled_debt_service_60d_amount: Decimal
    scheduled_debt_service_90d_amount: Decimal
    dscr_value: Optional[Decimal] = None
    covenant_status: str
    facilities: List[DebtOverviewItem]


class DebtScenarioEventInput(BaseSchema):
    event_date: date
    extra_principal_amount: Decimal = Field(ge=0)
    notes: Optional[str] = None


class DebtScenarioRunRequest(BaseSchema):
    facility_id: str
    scenario_name: str
    start_date: date
    events: List[DebtScenarioEventInput]


class DebtScenarioResult(BaseSchema):
    scenario_id: str
    facility_id: str
    scenario_name: str
    start_date: date
    baseline_payoff_date: Optional[date] = None
    scenario_payoff_date: Optional[date] = None
    payoff_date_delta_days: Optional[int] = None
    baseline_total_interest_amount: Decimal
    scenario_total_interest_amount: Decimal
    total_interest_delta_amount: Decimal
    liquidity_guardrail_passed: bool
    guardrail_reason: Optional[str] = None


class DebtScenarioSummary(BaseSchema):
    id: str
    facility_id: str
    scenario_name: str
    scenario_type: str
    start_date: date
    is_baseline: bool
    payoff_date: Optional[date] = None
    total_interest_amount: Optional[Decimal] = None
    total_principal_amount: Optional[Decimal] = None
    total_interest_delta_amount: Optional[Decimal] = None
    payoff_date_delta_days: Optional[int] = None
    created_at: str
    updated_at: str


class DebtCovenantSnapshot(BaseSchema):
    covenant_id: str
    facility_id: str
    covenant_code: str
    covenant_name: str
    metric_name: str
    threshold_value: Decimal
    comparison_operator: str
    as_of_date: date
    measured_value: Decimal
    is_in_compliance: bool
    note: Optional[str] = None


class DebtPaymentCreateRequest(BaseSchema):
    facility_id: str
    payment_date: date
    principal_paid_amount: Decimal = Field(ge=0)
    interest_paid_amount: Decimal = Field(ge=0)
    extra_principal_amount: Decimal = Field(default=Decimal("0"), ge=0)
    fee_amount: Decimal = Field(default=Decimal("0"), ge=0)
    source_account: Optional[str] = None
    reference: Optional[str] = None
    notes: Optional[str] = None


class DebtPaymentCreateResponse(BaseSchema):
    payment_id: str
    facility_id: str
    payment_date: date
    principal_paid_amount: Decimal
    interest_paid_amount: Decimal
    extra_principal_amount: Decimal
    remaining_balance_amount: Decimal
