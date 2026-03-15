-- Control plane runtime schema for data ingestion orchestration.

create extension if not exists pgcrypto;

do $$
begin
  if not exists (select 1 from pg_type where typname = 'data_job_schedule_mode') then
    create type public.data_job_schedule_mode as enum ('recurring', 'manual_only', 'backfill_only', 'system_managed');
  end if;
  if not exists (select 1 from pg_type where typname = 'data_job_run_status') then
    create type public.data_job_run_status as enum ('queued', 'running', 'success', 'failed', 'blocked', 'cancelled');
  end if;
  if not exists (select 1 from pg_type where typname = 'data_job_kind') then
    create type public.data_job_kind as enum ('source_ingestion', 'rollup_refresh', 'derived_compute', 'manual_import', 'maintenance');
  end if;
end $$;

create table if not exists public.data_jobs (
  id uuid primary key default gen_random_uuid(),
  job_key text not null unique,
  runner_key text not null,
  display_name text not null,
  job_kind public.data_job_kind not null,
  schedule_mode public.data_job_schedule_mode not null,
  enabled boolean not null default true,
  schedule_cron text null,
  schedule_timezone text not null default 'UTC',
  next_run_at timestamptz null,
  max_runtime_seconds integer not null default 3600,
  freshness_sla_minutes integer null,
  stale_after_minutes integer null,
  timeout_after_minutes integer null,
  owner text null,
  tags jsonb not null default '[]'::jsonb,
  config jsonb not null default '{}'::jsonb,
  deleted_at timestamptz null,
  created_at timestamptz not null default timezone('utc'::text, now()),
  updated_at timestamptz not null default timezone('utc'::text, now()),
  constraint data_jobs_schedule_cron_required check (
    (schedule_mode = 'recurring' and schedule_cron is not null)
    or (schedule_mode <> 'recurring')
  )
);

create index if not exists idx_data_jobs_due on public.data_jobs (enabled, next_run_at) where deleted_at is null;
create index if not exists idx_data_jobs_mode on public.data_jobs (schedule_mode) where deleted_at is null;

create table if not exists public.data_job_dependencies (
  id uuid primary key default gen_random_uuid(),
  job_id uuid not null references public.data_jobs(id) on delete cascade,
  depends_on_job_id uuid not null references public.data_jobs(id) on delete cascade,
  required boolean not null default true,
  allow_stale_dependency boolean not null default false,
  max_dependency_age_minutes integer null,
  created_at timestamptz not null default timezone('utc'::text, now()),
  unique (job_id, depends_on_job_id),
  constraint data_job_dependencies_not_self check (job_id <> depends_on_job_id)
);

create index if not exists idx_data_job_dependencies_job on public.data_job_dependencies (job_id);
create index if not exists idx_data_job_dependencies_depends_on on public.data_job_dependencies (depends_on_job_id);

create table if not exists public.data_job_runs (
  id uuid primary key default gen_random_uuid(),
  job_id uuid not null references public.data_jobs(id) on delete restrict,
  run_key text not null unique,
  run_status public.data_job_run_status not null default 'queued',
  trigger_type text not null,
  trigger_source text null,
  requested_by text null,
  requested_at timestamptz not null default timezone('utc'::text, now()),
  started_at timestamptz null,
  finished_at timestamptz null,
  blocked_reason text null,
  error_code text null,
  error_message text null,
  output jsonb not null default '{}'::jsonb,
  metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default timezone('utc'::text, now()),
  updated_at timestamptz not null default timezone('utc'::text, now())
);

create index if not exists idx_data_job_runs_job_created on public.data_job_runs (job_id, created_at desc);
create index if not exists idx_data_job_runs_status on public.data_job_runs (run_status, created_at desc);

create table if not exists public.data_job_run_steps (
  id uuid primary key default gen_random_uuid(),
  run_id uuid not null references public.data_job_runs(id) on delete cascade,
  step_key text not null,
  step_name text not null,
  step_order integer not null default 0,
  status public.data_job_run_status not null default 'queued',
  started_at timestamptz null,
  finished_at timestamptz null,
  error_message text null,
  output jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default timezone('utc'::text, now()),
  updated_at timestamptz not null default timezone('utc'::text, now()),
  unique (run_id, step_key)
);

create index if not exists idx_data_job_run_steps_run on public.data_job_run_steps (run_id, step_order asc);

create or replace view public.data_job_health_v1 as
with latest_runs as (
  select
    r.*,
    row_number() over (partition by r.job_id order by coalesce(r.started_at, r.created_at) desc) as rn
  from public.data_job_runs r
)
select
  j.id as job_id,
  j.job_key,
  j.display_name,
  j.job_kind,
  j.schedule_mode,
  j.enabled,
  j.schedule_cron,
  j.schedule_timezone,
  j.next_run_at,
  lr.id as last_run_id,
  lr.run_status as last_run_status,
  lr.started_at as last_started_at,
  lr.finished_at as last_finished_at,
  case
    when lr.started_at is not null and lr.finished_at is not null
      then extract(epoch from (lr.finished_at - lr.started_at))::integer
    else null
  end as last_duration_seconds,
  case
    when j.schedule_mode = 'recurring' and j.enabled and j.next_run_at is not null and j.next_run_at <= now() then true
    else false
  end as due_now
from public.data_jobs j
left join latest_runs lr on lr.job_id = j.id and lr.rn = 1
where j.deleted_at is null;

insert into public.data_jobs (
  job_key, runner_key, display_name, job_kind, schedule_mode, enabled, schedule_cron, schedule_timezone,
  owner, tags, config, freshness_sla_minutes, stale_after_minutes, next_run_at
)
values
  ('marketing-ga4-sync', 'marketing.ga4.sync', 'Marketing GA4 Sync', 'source_ingestion', 'recurring', true, '0 * * * *', 'UTC', 'marketing', '["marketing","ga4"]'::jsonb, '{}'::jsonb, 90, 180, timezone('utc'::text, now())),
  ('marketing-gsc-sync', 'marketing.gsc.sync', 'Marketing Search Console Sync', 'source_ingestion', 'recurring', true, '15 * * * *', 'UTC', 'marketing', '["marketing","gsc"]'::jsonb, '{}'::jsonb, 90, 180, timezone('utc'::text, now())),
  ('marketing-search-console-rollups-refresh', 'marketing.gsc.rollups.refresh', 'Marketing Search Console Rollups Refresh', 'rollup_refresh', 'system_managed', true, null, 'UTC', 'marketing', '["marketing","rollup"]'::jsonb, '{}'::jsonb, 120, 240, null),
  ('fx-rates-pull', 'fx.rates.pull', 'FX Rates Pull', 'source_ingestion', 'recurring', true, '*/15 * * * *', 'UTC', 'finance', '["fx","rates"]'::jsonb, '{}'::jsonb, 30, 60, timezone('utc'::text, now())),
  ('fx-exposure-refresh', 'fx.exposure.refresh', 'FX Exposure Refresh', 'rollup_refresh', 'system_managed', true, null, 'UTC', 'finance', '["fx","rollup"]'::jsonb, '{}'::jsonb, 45, 120, null),
  ('fx-signals-generate', 'fx.signals.generate', 'FX Signals Generate', 'derived_compute', 'recurring', true, '30 * * * *', 'UTC', 'finance', '["fx","signals"]'::jsonb, '{}'::jsonb, 120, 240, timezone('utc'::text, now())),
  ('fx-intelligence-generate', 'fx.intelligence.generate', 'FX Intelligence Generate', 'derived_compute', 'recurring', true, '0 */6 * * *', 'UTC', 'finance', '["fx","intelligence"]'::jsonb, '{}'::jsonb, 360, 720, timezone('utc'::text, now())),
  ('salesforce-readonly-sync', 'salesforce.readonly.sync', 'Salesforce Readonly Sync', 'source_ingestion', 'recurring', true, '0 */2 * * *', 'UTC', 'salesforce', '["salesforce","ingestion"]'::jsonb, '{}'::jsonb, 180, 360, timezone('utc'::text, now())),
  ('travel-trade-rollups-refresh', 'salesforce.travel_trade.rollups.refresh', 'Travel Trade Rollups Refresh', 'rollup_refresh', 'system_managed', true, null, 'UTC', 'salesforce', '["salesforce","rollup"]'::jsonb, '{}'::jsonb, 180, 360, null),
  ('consultant-ai-rollups-refresh', 'salesforce.consultant_ai.rollups.refresh', 'Consultant AI Rollups Refresh', 'rollup_refresh', 'system_managed', true, null, 'UTC', 'salesforce', '["salesforce","rollup"]'::jsonb, '{}'::jsonb, 180, 360, null),
  ('ai-insights-generate', 'ai.insights.generate', 'AI Insights Generate', 'derived_compute', 'recurring', true, '0 */6 * * *', 'UTC', 'ai', '["ai","insights"]'::jsonb, '{}'::jsonb, 360, 720, timezone('utc'::text, now())),
  ('bookings-import', 'imports.bookings.upsert', 'Bookings Import', 'manual_import', 'manual_only', true, null, 'UTC', 'ops', '["imports"]'::jsonb, '{}'::jsonb, null, null, null),
  ('customer-payments-import', 'imports.customer_payments.upsert', 'Customer Payments Import', 'manual_import', 'manual_only', true, null, 'UTC', 'ops', '["imports"]'::jsonb, '{}'::jsonb, null, null, null),
  ('supplier-invoices-import', 'imports.supplier_invoices.upsert', 'Supplier Invoices Import', 'manual_import', 'manual_only', true, null, 'UTC', 'ops', '["imports"]'::jsonb, '{}'::jsonb, null, null, null),
  ('supplier-invoice-bookings-import', 'imports.supplier_invoice_bookings.upsert', 'Supplier Invoice Bookings Import', 'manual_import', 'manual_only', true, null, 'UTC', 'ops', '["imports"]'::jsonb, '{}'::jsonb, null, null, null),
  ('supplier-invoice-lines-import', 'imports.supplier_invoice_lines.upsert', 'Supplier Invoice Lines Import', 'manual_import', 'manual_only', true, null, 'UTC', 'ops', '["imports"]'::jsonb, '{}'::jsonb, null, null, null),
  ('fx-rates-history-backfill', 'fx.rates.backfill', 'FX Rates History Backfill', 'maintenance', 'backfill_only', true, null, 'UTC', 'finance', '["fx","backfill"]'::jsonb, '{}'::jsonb, null, null, null),
  ('salesforce-permission-validate', 'salesforce.permissions.validate', 'Salesforce Permission Validate', 'maintenance', 'manual_only', true, null, 'UTC', 'salesforce', '["salesforce","maintenance"]'::jsonb, '{}'::jsonb, null, null, null),
  ('ai-insights-purge', 'ai.insights.purge', 'AI Insights Purge', 'maintenance', 'manual_only', true, null, 'UTC', 'ai', '["ai","maintenance"]'::jsonb, '{}'::jsonb, null, null, null),
  ('inactive-employees-cleanup', 'workforce.cleanup.inactive_employees', 'Inactive Employees Cleanup', 'maintenance', 'recurring', true, '0 3 * * 0', 'UTC', 'ops', '["maintenance"]'::jsonb, '{}'::jsonb, null, null, timezone('utc'::text, now())),
  ('debt-schedule-precompute', 'debt.schedule.precompute', 'Debt Schedule Precompute', 'maintenance', 'system_managed', true, null, 'UTC', 'finance', '["debt","maintenance"]'::jsonb, '{}'::jsonb, null, null, null)
on conflict (job_key) do update
set
  runner_key = excluded.runner_key,
  display_name = excluded.display_name,
  job_kind = excluded.job_kind,
  schedule_mode = excluded.schedule_mode,
  schedule_cron = excluded.schedule_cron,
  schedule_timezone = excluded.schedule_timezone,
  enabled = excluded.enabled,
  owner = excluded.owner,
  tags = excluded.tags,
  freshness_sla_minutes = excluded.freshness_sla_minutes,
  stale_after_minutes = excluded.stale_after_minutes,
  next_run_at = coalesce(public.data_jobs.next_run_at, excluded.next_run_at),
  updated_at = timezone('utc'::text, now());

update public.data_jobs
set next_run_at = timezone('utc'::text, now())
where deleted_at is null
  and enabled = true
  and schedule_mode = 'recurring'
  and next_run_at is null;

insert into public.data_job_dependencies (job_id, depends_on_job_id, required, allow_stale_dependency, max_dependency_age_minutes)
select child.id, parent.id, true, false, null
from public.data_jobs child
join public.data_jobs parent on parent.job_key = 'marketing-ga4-sync'
where child.job_key = 'marketing-search-console-rollups-refresh'
on conflict (job_id, depends_on_job_id) do nothing;

insert into public.data_job_dependencies (job_id, depends_on_job_id, required, allow_stale_dependency, max_dependency_age_minutes)
select child.id, parent.id, true, false, null
from public.data_jobs child
join public.data_jobs parent on parent.job_key = 'marketing-gsc-sync'
where child.job_key = 'marketing-search-console-rollups-refresh'
on conflict (job_id, depends_on_job_id) do nothing;

insert into public.data_job_dependencies (job_id, depends_on_job_id, required, allow_stale_dependency, max_dependency_age_minutes)
select child.id, parent.id, true, false, null
from public.data_jobs child
join public.data_jobs parent on parent.job_key = 'fx-rates-pull'
where child.job_key = 'fx-exposure-refresh'
on conflict (job_id, depends_on_job_id) do nothing;

insert into public.data_job_dependencies (job_id, depends_on_job_id, required, allow_stale_dependency, max_dependency_age_minutes)
select child.id, parent.id, true, false, null
from public.data_jobs child
join public.data_jobs parent on parent.job_key = 'fx-exposure-refresh'
where child.job_key = 'fx-signals-generate'
on conflict (job_id, depends_on_job_id) do nothing;

insert into public.data_job_dependencies (job_id, depends_on_job_id, required, allow_stale_dependency, max_dependency_age_minutes)
select child.id, parent.id, true, false, null
from public.data_jobs child
join public.data_jobs parent on parent.job_key = 'fx-rates-pull'
where child.job_key = 'fx-intelligence-generate'
on conflict (job_id, depends_on_job_id) do nothing;

insert into public.data_job_dependencies (job_id, depends_on_job_id, required, allow_stale_dependency, max_dependency_age_minutes)
select child.id, parent.id, true, false, null
from public.data_jobs child
join public.data_jobs parent on parent.job_key = 'salesforce-readonly-sync'
where child.job_key in ('travel-trade-rollups-refresh', 'consultant-ai-rollups-refresh')
on conflict (job_id, depends_on_job_id) do nothing;

insert into public.data_job_dependencies (job_id, depends_on_job_id, required, allow_stale_dependency, max_dependency_age_minutes)
select child.id, parent.id, true, false, null
from public.data_jobs child
join public.data_jobs parent on parent.job_key = 'consultant-ai-rollups-refresh'
where child.job_key = 'ai-insights-generate'
on conflict (job_id, depends_on_job_id) do nothing;

insert into public.data_job_dependencies (job_id, depends_on_job_id, required, allow_stale_dependency, max_dependency_age_minutes)
select child.id, parent.id, true, false, null
from public.data_jobs child
join public.data_jobs parent on parent.job_key = 'supplier-invoices-import'
where child.job_key in ('supplier-invoice-bookings-import', 'supplier-invoice-lines-import')
on conflict (job_id, depends_on_job_id) do nothing;
