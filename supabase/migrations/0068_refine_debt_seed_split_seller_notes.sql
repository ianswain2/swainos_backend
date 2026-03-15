-- Forward-only migration:
-- Refine debt seed to model three obligations:
-- 1) Citizens SBA bank loan
-- 2) Seller Note 1 (Ian Swain Sr.) with 2-year standby
-- 3) Seller Note 2 (equity-injection seller note) with full standby

insert into public.debt_facilities (
  external_id,
  lender_name,
  facility_name,
  facility_type,
  original_principal_amount,
  currency_code,
  origination_date,
  first_payment_date,
  maturity_date,
  payment_day_of_month,
  prepayment_penalty_mode,
  status,
  notes
)
values
  (
    'citizens_sba_7a_2026',
    'Citizens Bank',
    'SBA 7A Guaranteed Loan',
    'term_loan',
    4300000.00,
    'USD',
    '2026-05-01',
    '2026-06-01',
    '2036-05-01',
    1,
    'none',
    'active',
    'Bank term loan from Citizens. Fixed 7.00 percent, amortizing.'
  ),
  (
    'seller_note_1_ian_swain_sr_2026',
    'Ian Swain Sr.',
    'Seller Note 1',
    'seller_note',
    2142105.00,
    'USD',
    '2026-05-01',
    '2028-06-01',
    '2036-05-01',
    1,
    'unknown',
    'active',
    'Confirmed seller note: 10-year term, 4.65 percent fixed, 2-year standby (no payments first 24 months).'
  ),
  (
    'seller_note_2_equity_standby_2026',
    'Ian Swain Sr.',
    'Seller Note 2 (Equity Injection)',
    'seller_note',
    357895.00,
    'USD',
    '2026-05-01',
    null,
    '2036-05-01',
    null,
    'unknown',
    'active',
    'Confirmed seller note equity component: 10-year term, 4.65 percent fixed, full standby for full SBA life.'
  )
on conflict (external_id) do update
  set lender_name = excluded.lender_name,
      facility_name = excluded.facility_name,
      facility_type = excluded.facility_type,
      original_principal_amount = excluded.original_principal_amount,
      currency_code = excluded.currency_code,
      origination_date = excluded.origination_date,
      first_payment_date = excluded.first_payment_date,
      maturity_date = excluded.maturity_date,
      payment_day_of_month = excluded.payment_day_of_month,
      prepayment_penalty_mode = excluded.prepayment_penalty_mode,
      status = excluded.status,
      notes = excluded.notes,
      updated_at = now();

update public.debt_facilities
set status = 'inactive',
    notes = coalesce(notes, '') || ' Superseded by split notes: seller_note_1_ian_swain_sr_2026 and seller_note_2_equity_standby_2026.',
    updated_at = now()
where external_id = 'seller_note_2026';

insert into public.debt_facility_terms (
  facility_id,
  effective_start_date,
  effective_end_date,
  rate_mode,
  rate_unit,
  annual_rate,
  payment_frequency,
  amortization_months,
  recast_on_extra_principal
)
select
  f.id as facility_id,
  date '2026-05-01' as effective_start_date,
  null::date as effective_end_date,
  'fixed' as rate_mode,
  'percent' as rate_unit,
  case
    when f.external_id = 'citizens_sba_7a_2026' then 7.00
    when f.external_id = 'seller_note_1_ian_swain_sr_2026' then 4.65
    when f.external_id = 'seller_note_2_equity_standby_2026' then 4.65
    else 0.00
  end as annual_rate,
  'monthly' as payment_frequency,
  case
    when f.external_id = 'seller_note_1_ian_swain_sr_2026' then 96
    else 120
  end as amortization_months,
  false as recast_on_extra_principal
from public.debt_facilities f
where f.external_id in (
  'citizens_sba_7a_2026',
  'seller_note_1_ian_swain_sr_2026',
  'seller_note_2_equity_standby_2026'
)
on conflict (facility_id, effective_start_date) do update
  set effective_end_date = excluded.effective_end_date,
      rate_mode = excluded.rate_mode,
      rate_unit = excluded.rate_unit,
      annual_rate = excluded.annual_rate,
      payment_frequency = excluded.payment_frequency,
      amortization_months = excluded.amortization_months,
      recast_on_extra_principal = excluded.recast_on_extra_principal,
      updated_at = now();

insert into public.debt_balance_snapshots (
  facility_id,
  as_of_date,
  outstanding_balance_amount,
  principal_paid_to_date_amount,
  interest_paid_to_date_amount,
  extra_principal_to_date_amount,
  next_due_date,
  next_due_amount
)
select
  f.id as facility_id,
  date '2026-05-01' as as_of_date,
  f.original_principal_amount as outstanding_balance_amount,
  0 as principal_paid_to_date_amount,
  0 as interest_paid_to_date_amount,
  0 as extra_principal_to_date_amount,
  case
    when f.external_id = 'seller_note_2_equity_standby_2026' then null::date
    else f.first_payment_date
  end as next_due_date,
  null::numeric as next_due_amount
from public.debt_facilities f
where f.external_id in (
  'citizens_sba_7a_2026',
  'seller_note_1_ian_swain_sr_2026',
  'seller_note_2_equity_standby_2026'
)
on conflict (facility_id, as_of_date) do update
  set outstanding_balance_amount = excluded.outstanding_balance_amount,
      principal_paid_to_date_amount = excluded.principal_paid_to_date_amount,
      interest_paid_to_date_amount = excluded.interest_paid_to_date_amount,
      extra_principal_to_date_amount = excluded.extra_principal_to_date_amount,
      next_due_date = excluded.next_due_date,
      next_due_amount = excluded.next_due_amount,
      updated_at = now();

delete from public.debt_payment_schedule
where facility_id in (
  select id
  from public.debt_facilities
  where external_id in ('seller_note_2026', 'seller_note_2_equity_standby_2026')
);
