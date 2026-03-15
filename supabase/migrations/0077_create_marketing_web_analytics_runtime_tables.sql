-- GA4-first marketing web analytics snapshots and sync runtime state.

create table if not exists public.marketing_web_analytics_daily (
  snapshot_date date primary key,
  sessions numeric not null default 0,
  total_users numeric not null default 0,
  engaged_sessions numeric not null default 0,
  engagement_rate numeric not null default 0,
  key_events numeric not null default 0,
  source_medium text not null default 'all',
  default_channel_group text not null default 'all',
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create table if not exists public.marketing_web_analytics_landing_pages_daily (
  snapshot_date date not null,
  landing_page text not null,
  sessions numeric not null default 0,
  total_users numeric not null default 0,
  engagement_rate numeric not null default 0,
  key_events numeric not null default 0,
  avg_session_duration_seconds numeric,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  primary key (snapshot_date, landing_page)
);

create table if not exists public.marketing_web_analytics_events_daily (
  snapshot_date date not null,
  event_name text not null,
  event_count numeric not null default 0,
  total_users numeric not null default 0,
  event_value_amount numeric,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  primary key (snapshot_date, event_name)
);

create table if not exists public.marketing_web_analytics_sync_runs (
  id uuid primary key default gen_random_uuid(),
  source_system text not null default 'ga4',
  status text not null check (status in ('running', 'success', 'failed')),
  started_at timestamptz not null default now(),
  completed_at timestamptz,
  records_processed integer not null default 0,
  records_created integer not null default 0,
  data_window_start date,
  data_window_end date,
  error_message text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create index if not exists idx_marketing_web_analytics_daily_snapshot_date
  on public.marketing_web_analytics_daily (snapshot_date desc);

create index if not exists idx_marketing_web_analytics_pages_snapshot_date_sessions
  on public.marketing_web_analytics_landing_pages_daily (snapshot_date desc, sessions desc);

create index if not exists idx_marketing_web_analytics_events_snapshot_date_count
  on public.marketing_web_analytics_events_daily (snapshot_date desc, event_count desc);

create index if not exists idx_marketing_web_analytics_sync_runs_started_at
  on public.marketing_web_analytics_sync_runs (started_at desc);

alter table public.marketing_web_analytics_daily enable row level security;
alter table public.marketing_web_analytics_landing_pages_daily enable row level security;
alter table public.marketing_web_analytics_events_daily enable row level security;
alter table public.marketing_web_analytics_sync_runs enable row level security;

create policy marketing_web_analytics_daily_select_authenticated
on public.marketing_web_analytics_daily for select
using (auth.role() = 'authenticated');

create policy marketing_web_analytics_daily_insert_service
on public.marketing_web_analytics_daily for insert
with check (auth.role() = 'service_role');

create policy marketing_web_analytics_daily_update_service
on public.marketing_web_analytics_daily for update
using (auth.role() = 'service_role')
with check (auth.role() = 'service_role');

create policy marketing_web_analytics_pages_select_authenticated
on public.marketing_web_analytics_landing_pages_daily for select
using (auth.role() = 'authenticated');

create policy marketing_web_analytics_pages_insert_service
on public.marketing_web_analytics_landing_pages_daily for insert
with check (auth.role() = 'service_role');

create policy marketing_web_analytics_pages_update_service
on public.marketing_web_analytics_landing_pages_daily for update
using (auth.role() = 'service_role')
with check (auth.role() = 'service_role');

create policy marketing_web_analytics_events_select_authenticated
on public.marketing_web_analytics_events_daily for select
using (auth.role() = 'authenticated');

create policy marketing_web_analytics_events_insert_service
on public.marketing_web_analytics_events_daily for insert
with check (auth.role() = 'service_role');

create policy marketing_web_analytics_events_update_service
on public.marketing_web_analytics_events_daily for update
using (auth.role() = 'service_role')
with check (auth.role() = 'service_role');

create policy marketing_web_analytics_sync_runs_select_authenticated
on public.marketing_web_analytics_sync_runs for select
using (auth.role() = 'authenticated');

create policy marketing_web_analytics_sync_runs_insert_service
on public.marketing_web_analytics_sync_runs for insert
with check (auth.role() = 'service_role');

create policy marketing_web_analytics_sync_runs_update_service
on public.marketing_web_analytics_sync_runs for update
using (auth.role() = 'service_role')
with check (auth.role() = 'service_role');
