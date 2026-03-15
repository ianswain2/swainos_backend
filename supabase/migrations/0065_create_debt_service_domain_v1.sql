-- Forward-only migration:
-- Create normalized debt service domain tables for facility terms, projected schedules,
-- posted payments, balance snapshots, covenant tracking, and scenario simulation.

create table if not exists public.debt_facilities (
  id uuid primary key default gen_random_uuid(),
  external_id text unique,
  lender_name text not null,
  facility_name text not null,
  facility_type text not null default 'term_loan',
  original_principal_amount numeric not null check (original_principal_amount >= 0),
  currency_code text not null default 'USD',
  origination_date date not null,
  first_payment_date date,
  maturity_date date not null,
  payment_day_of_month integer check (payment_day_of_month between 1 and 31),
  prepayment_penalty_mode text not null default 'none',
  status text not null default 'active',
  notes text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create table if not exists public.debt_facility_terms (
  id uuid primary key default gen_random_uuid(),
  facility_id uuid not null references public.debt_facilities(id) on delete cascade,
  effective_start_date date not null,
  effective_end_date date,
  rate_mode text not null default 'fixed',
  rate_unit text not null default 'decimal' check (rate_unit in ('decimal', 'percent')),
  annual_rate numeric not null check (annual_rate >= 0),
  payment_frequency text not null default 'monthly',
  amortization_months integer not null check (amortization_months > 0),
  scheduled_payment_amount numeric check (scheduled_payment_amount is null or scheduled_payment_amount >= 0),
  recast_on_extra_principal boolean not null default false,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint debt_facility_terms_effective_range_chk
    check (effective_end_date is null or effective_end_date >= effective_start_date),
  constraint debt_facility_terms_annual_rate_unit_chk
    check (
      (rate_unit = 'decimal' and annual_rate <= 1)
      or (rate_unit = 'percent' and annual_rate <= 100)
    ),
  constraint debt_facility_terms_payment_frequency_chk
    check (payment_frequency in ('monthly'))
);

create unique index if not exists idx_debt_facility_terms_facility_start_unique
  on public.debt_facility_terms(facility_id, effective_start_date);

create extension if not exists btree_gist;

do $$
begin
  if not exists (
    select 1
    from pg_constraint
    where conname = 'debt_facility_terms_no_overlap_excl'
  ) then
    execute '
      alter table public.debt_facility_terms
      add constraint debt_facility_terms_no_overlap_excl
      exclude using gist (
        facility_id with =,
        daterange(
          effective_start_date,
          coalesce(effective_end_date, ''infinity''::date),
          ''[]''
        ) with &&
      )
    ';
  end if;
end $$;

create table if not exists public.debt_payment_schedule (
  id uuid primary key default gen_random_uuid(),
  facility_id uuid not null references public.debt_facilities(id) on delete cascade,
  term_id uuid not null references public.debt_facility_terms(id) on delete cascade,
  due_date date not null,
  period_index integer not null check (period_index >= 1),
  opening_balance_amount numeric not null check (opening_balance_amount >= 0),
  scheduled_payment_amount numeric not null check (scheduled_payment_amount >= 0),
  scheduled_principal_amount numeric not null check (scheduled_principal_amount >= 0),
  scheduled_interest_amount numeric not null check (scheduled_interest_amount >= 0),
  extra_principal_applied_amount numeric not null default 0 check (extra_principal_applied_amount >= 0),
  remaining_balance_amount numeric not null check (remaining_balance_amount >= 0),
  generated_for_as_of_date date,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint debt_payment_schedule_facility_due_unique unique (facility_id, due_date)
);

create table if not exists public.debt_payments_actual (
  id uuid primary key default gen_random_uuid(),
  facility_id uuid not null references public.debt_facilities(id) on delete cascade,
  schedule_id uuid references public.debt_payment_schedule(id) on delete set null,
  payment_date date not null,
  principal_paid_amount numeric not null check (principal_paid_amount >= 0),
  interest_paid_amount numeric not null check (interest_paid_amount >= 0),
  extra_principal_amount numeric not null default 0 check (extra_principal_amount >= 0),
  fee_amount numeric not null default 0 check (fee_amount >= 0),
  source_account text,
  reference text,
  notes text,
  entered_by uuid references public.app_users(id),
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create table if not exists public.debt_balance_snapshots (
  id uuid primary key default gen_random_uuid(),
  facility_id uuid not null references public.debt_facilities(id) on delete cascade,
  as_of_date date not null,
  outstanding_balance_amount numeric not null check (outstanding_balance_amount >= 0),
  principal_paid_to_date_amount numeric not null default 0 check (principal_paid_to_date_amount >= 0),
  interest_paid_to_date_amount numeric not null default 0 check (interest_paid_to_date_amount >= 0),
  extra_principal_to_date_amount numeric not null default 0 check (extra_principal_to_date_amount >= 0),
  next_due_date date,
  next_due_amount numeric check (next_due_amount is null or next_due_amount >= 0),
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint debt_balance_snapshots_facility_as_of_unique unique (facility_id, as_of_date)
);

create table if not exists public.debt_covenants (
  id uuid primary key default gen_random_uuid(),
  facility_id uuid not null references public.debt_facilities(id) on delete cascade,
  covenant_code text not null,
  covenant_name text not null,
  metric_name text not null,
  threshold_value numeric not null,
  comparison_operator text not null default 'gte',
  measurement_frequency text not null default 'monthly',
  is_active boolean not null default true,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create table if not exists public.debt_covenant_snapshots (
  id uuid primary key default gen_random_uuid(),
  covenant_id uuid not null references public.debt_covenants(id) on delete cascade,
  facility_id uuid not null references public.debt_facilities(id) on delete cascade,
  as_of_date date not null,
  measured_value numeric not null,
  threshold_value numeric not null,
  is_in_compliance boolean not null,
  note text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint debt_covenant_snapshots_covenant_as_of_unique unique (covenant_id, as_of_date)
);

create table if not exists public.debt_scenarios (
  id uuid primary key default gen_random_uuid(),
  facility_id uuid not null references public.debt_facilities(id) on delete cascade,
  scenario_name text not null,
  scenario_type text not null default 'manual_extra_principal',
  start_date date not null,
  is_baseline boolean not null default false,
  baseline_scenario_id uuid references public.debt_scenarios(id) on delete set null,
  payoff_date date,
  total_interest_amount numeric,
  total_principal_amount numeric,
  total_interest_delta_amount numeric,
  payoff_date_delta_days integer,
  metadata jsonb not null default '{}'::jsonb,
  created_by uuid references public.app_users(id),
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create table if not exists public.debt_scenario_events (
  id uuid primary key default gen_random_uuid(),
  scenario_id uuid not null references public.debt_scenarios(id) on delete cascade,
  event_date date not null,
  extra_principal_amount numeric not null check (extra_principal_amount >= 0),
  notes text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create index if not exists idx_debt_facility_terms_facility_effective_start
  on public.debt_facility_terms(facility_id, effective_start_date);

create index if not exists idx_debt_payment_schedule_facility_due_date
  on public.debt_payment_schedule(facility_id, due_date);

create index if not exists idx_debt_payments_actual_facility_payment_date
  on public.debt_payments_actual(facility_id, payment_date);

create index if not exists idx_debt_balance_snapshots_facility_as_of_date
  on public.debt_balance_snapshots(facility_id, as_of_date);

create index if not exists idx_debt_covenants_facility_active
  on public.debt_covenants(facility_id, is_active);

create index if not exists idx_debt_covenant_snapshots_facility_as_of
  on public.debt_covenant_snapshots(facility_id, as_of_date);

create index if not exists idx_debt_scenarios_facility_created_at
  on public.debt_scenarios(facility_id, created_at desc);

create index if not exists idx_debt_scenario_events_scenario_event_date
  on public.debt_scenario_events(scenario_id, event_date);

create or replace view public.v_debt_service_overview as
with latest_balance as (
  select distinct on (facility_id)
    facility_id,
    as_of_date,
    outstanding_balance_amount,
    principal_paid_to_date_amount,
    interest_paid_to_date_amount,
    extra_principal_to_date_amount,
    next_due_date,
    next_due_amount
  from public.debt_balance_snapshots
  order by facility_id, as_of_date desc
),
next_90 as (
  select
    facility_id,
    coalesce(sum(scheduled_payment_amount) filter (where due_date <= current_date + interval '30 days'), 0) as scheduled_debt_service_30d_amount,
    coalesce(sum(scheduled_payment_amount) filter (where due_date <= current_date + interval '60 days'), 0) as scheduled_debt_service_60d_amount,
    coalesce(sum(scheduled_payment_amount) filter (where due_date <= current_date + interval '90 days'), 0) as scheduled_debt_service_90d_amount
  from public.debt_payment_schedule
  where due_date >= current_date
  group by facility_id
),
latest_covenant as (
  select distinct on (facility_id)
    facility_id,
    as_of_date,
    is_in_compliance
  from public.debt_covenant_snapshots
  order by facility_id, as_of_date desc
)
select
  f.id as facility_id,
  f.facility_name,
  f.currency_code,
  coalesce(lb.as_of_date, current_date) as as_of_date,
  coalesce(lb.outstanding_balance_amount, f.original_principal_amount) as outstanding_balance_amount,
  coalesce(lb.principal_paid_to_date_amount, 0) as principal_paid_to_date_amount,
  coalesce(lb.interest_paid_to_date_amount, 0) as interest_paid_to_date_amount,
  coalesce(lb.extra_principal_to_date_amount, 0) as extra_principal_to_date_amount,
  lb.next_due_date,
  lb.next_due_amount,
  coalesce(n90.scheduled_debt_service_30d_amount, 0) as scheduled_debt_service_30d_amount,
  coalesce(n90.scheduled_debt_service_60d_amount, 0) as scheduled_debt_service_60d_amount,
  coalesce(n90.scheduled_debt_service_90d_amount, 0) as scheduled_debt_service_90d_amount,
  lc.is_in_compliance as covenant_in_compliance
from public.debt_facilities f
left join latest_balance lb on lb.facility_id = f.id
left join next_90 n90 on n90.facility_id = f.id
left join latest_covenant lc on lc.facility_id = f.id
where f.status = 'active';
