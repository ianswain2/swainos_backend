-- Runtime cursor/run state for read-only Salesforce Bulk API ingestion.

create table if not exists public.salesforce_sync_cursors (
  object_name text primary key,
  last_systemmodstamp timestamptz,
  last_id text,
  updated_at timestamptz not null default now()
);

create table if not exists public.salesforce_sync_runs (
  run_id uuid primary key,
  status text not null check (status in ('running', 'success', 'failed')),
  started_at timestamptz not null default now(),
  finished_at timestamptz,
  upper_bound timestamptz not null,
  object_scope text[] not null,
  object_metrics jsonb not null default '{}'::jsonb,
  jobs_created integer not null default 0,
  polls_made integer not null default 0,
  result_pages_read integer not null default 0,
  error_message text
);

create index if not exists idx_salesforce_sync_runs_started_at
  on public.salesforce_sync_runs (started_at desc);

alter table public.salesforce_sync_cursors enable row level security;
alter table public.salesforce_sync_runs enable row level security;

create policy salesforce_sync_cursors_select_authenticated
on public.salesforce_sync_cursors for select
using (auth.role() = 'authenticated');

create policy salesforce_sync_cursors_insert_service
on public.salesforce_sync_cursors for insert
with check (auth.role() = 'service_role');

create policy salesforce_sync_cursors_update_service
on public.salesforce_sync_cursors for update
using (auth.role() = 'service_role')
with check (auth.role() = 'service_role');

create policy salesforce_sync_runs_select_authenticated
on public.salesforce_sync_runs for select
using (auth.role() = 'authenticated');

create policy salesforce_sync_runs_insert_service
on public.salesforce_sync_runs for insert
with check (auth.role() = 'service_role');

create policy salesforce_sync_runs_update_service
on public.salesforce_sync_runs for update
using (auth.role() = 'service_role')
with check (auth.role() = 'service_role');

