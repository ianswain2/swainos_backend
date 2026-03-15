-- Forward-only migration:
-- Create finance planning domain tables for budget versioning, actuals mapping, and sync runtime.

create table if not exists public.financial_categories (
  id uuid primary key default gen_random_uuid(),
  source_system text not null default 'quickbooks_online',
  source_account_id text,
  source_account_code text not null,
  category_name text not null,
  statement_section text not null check (
    statement_section in ('revenue', 'cogs', 'expense', 'other_income', 'other_expense', 'non_operating', 'uncategorized')
  ),
  parent_category_id uuid references public.financial_categories(id) on delete set null,
  sort_order integer not null default 0,
  is_header boolean not null default false,
  is_active boolean not null default true,
  metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (source_system, source_account_code)
);

create table if not exists public.budget_versions (
  id uuid primary key default gen_random_uuid(),
  version_name text not null,
  fiscal_year integer not null check (fiscal_year between 2000 and 2100),
  scenario_name text not null default 'baseline',
  status text not null default 'draft' check (status in ('draft', 'approved', 'locked', 'superseded')),
  based_on_version_id uuid references public.budget_versions(id) on delete set null,
  notes text,
  is_locked boolean not null default false,
  locked_at timestamptz,
  created_by uuid references public.app_users(id) on delete set null,
  approved_by uuid references public.app_users(id) on delete set null,
  approved_at timestamptz,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (fiscal_year, scenario_name, version_name),
  check (
    (status = 'locked' and is_locked = true and locked_at is not null)
    or (status <> 'locked' and is_locked = false and locked_at is null)
  ),
  check (
    (approved_by is null and approved_at is null)
    or (approved_by is not null and approved_at is not null)
  )
);

create table if not exists public.budget_lines (
  id uuid primary key default gen_random_uuid(),
  budget_version_id uuid not null references public.budget_versions(id) on delete cascade,
  month_start date not null,
  financial_category_id uuid not null references public.financial_categories(id) on delete restrict,
  currency_code text not null default 'USD',
  budget_amount numeric not null default 0,
  notes text,
  source_type text not null default 'manual' check (source_type in ('manual', 'formula', 'imported')),
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  check (month_start = date_trunc('month', month_start)::date),
  unique (budget_version_id, month_start, financial_category_id, currency_code)
);

create table if not exists public.budget_assumptions (
  id uuid primary key default gen_random_uuid(),
  budget_line_id uuid not null references public.budget_lines(id) on delete cascade,
  assumption_key text not null,
  assumption_value text,
  assumption_value_json jsonb,
  display_order integer not null default 0,
  created_by uuid references public.app_users(id) on delete set null,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (budget_line_id, assumption_key, display_order)
);

create table if not exists public.account_mapping (
  id uuid primary key default gen_random_uuid(),
  source_system text not null default 'quickbooks_online',
  source_account_id text,
  source_account_code text not null,
  source_account_name text,
  source_class_name text not null default '',
  source_location_name text not null default '',
  financial_category_id uuid not null references public.financial_categories(id) on delete restrict,
  allocation_pct numeric not null default 1 check (allocation_pct > 0 and allocation_pct <= 1),
  effective_from date not null default date '1900-01-01',
  effective_to date,
  is_active boolean not null default true,
  metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  check (effective_to is null or effective_to >= effective_from)
);

create extension if not exists btree_gist;

alter table public.account_mapping
  drop constraint if exists account_mapping_effective_window_no_overlap;

alter table public.account_mapping
  add constraint account_mapping_effective_window_no_overlap
  exclude using gist (
    source_system with =,
    source_account_code with =,
    source_class_name with =,
    source_location_name with =,
    financial_category_id with =,
    daterange(effective_from, coalesce(effective_to, date 'infinity'), '[]') with &&
  )
  where (is_active);

create table if not exists public.actuals_monthly (
  id uuid primary key default gen_random_uuid(),
  source_system text not null default 'quickbooks_online',
  source_account_id text,
  source_account_code text not null,
  source_class_name text not null default '',
  source_location_name text not null default '',
  month_start date not null,
  financial_category_id uuid not null references public.financial_categories(id) on delete restrict,
  currency_code text not null default 'USD',
  accounting_basis text not null default 'accrual' check (accounting_basis in ('accrual', 'cash')),
  actual_amount numeric not null default 0,
  as_of_date date not null default current_date,
  source_record_hash text,
  metadata jsonb not null default '{}'::jsonb,
  loaded_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  check (month_start = date_trunc('month', month_start)::date),
  unique (
    source_system,
    source_account_code,
    source_class_name,
    source_location_name,
    month_start,
    financial_category_id,
    currency_code,
    accounting_basis
  )
);

create table if not exists public.quickbooks_sync_cursors (
  cursor_key text primary key,
  cursor_value text,
  cursor_payload jsonb not null default '{}'::jsonb,
  updated_at timestamptz not null default now()
);

create table if not exists public.quickbooks_sync_runs (
  run_id uuid primary key,
  status text not null check (status in ('running', 'success', 'failed', 'skipped')),
  started_at timestamptz not null default now(),
  finished_at timestamptz,
  sync_scope text[] not null default '{}'::text[],
  records_processed integer not null default 0,
  records_created integer not null default 0,
  records_updated integer not null default 0,
  source_freshness_date date,
  run_metadata jsonb not null default '{}'::jsonb,
  warnings jsonb not null default '[]'::jsonb,
  error_message text
);

create index if not exists idx_financial_categories_source_code
  on public.financial_categories (source_system, source_account_code);

create index if not exists idx_financial_categories_section_sort
  on public.financial_categories (statement_section, sort_order, source_account_code);

create index if not exists idx_budget_versions_fiscal_status
  on public.budget_versions (fiscal_year, status, scenario_name, created_at desc);

create index if not exists idx_budget_versions_based_on
  on public.budget_versions (based_on_version_id);

create index if not exists idx_budget_lines_version_month
  on public.budget_lines (budget_version_id, month_start, currency_code);

create index if not exists idx_budget_lines_category_month
  on public.budget_lines (financial_category_id, month_start);

create index if not exists idx_budget_assumptions_line
  on public.budget_assumptions (budget_line_id, display_order);

create index if not exists idx_account_mapping_source_account
  on public.account_mapping (source_system, source_account_code, source_class_name, source_location_name);

create index if not exists idx_account_mapping_category
  on public.account_mapping (financial_category_id, effective_from, effective_to);

create index if not exists idx_actuals_monthly_month_category
  on public.actuals_monthly (month_start, financial_category_id, currency_code);

create index if not exists idx_actuals_monthly_source_account
  on public.actuals_monthly (source_system, source_account_code, source_class_name, source_location_name);

create index if not exists idx_actuals_monthly_as_of_date
  on public.actuals_monthly (as_of_date desc, loaded_at desc);

create unique index if not exists idx_actuals_monthly_source_record_hash_unique
  on public.actuals_monthly (source_system, source_record_hash)
  where source_record_hash is not null;

create index if not exists idx_quickbooks_sync_runs_started_at
  on public.quickbooks_sync_runs (started_at desc);
