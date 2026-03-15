-- Harden marketing analytics canonical storage grains and idempotent upsert keys.

-- Channel totals are canonical at snapshot_date + default_channel_group.
alter table public.marketing_web_analytics_channels_daily
  alter column source_medium set default 'all';

-- Historical rows may contain multiple source_medium splits per date+channel from old model.
-- Reset to clean canonical storage; sync will fully repopulate exact facts.
truncate table public.marketing_web_analytics_channels_daily;

do $$
begin
  if exists (
    select 1
    from pg_constraint
    where conname = 'marketing_web_analytics_channels_daily_pkey'
      and conrelid = 'public.marketing_web_analytics_channels_daily'::regclass
  ) then
    alter table public.marketing_web_analytics_channels_daily
      drop constraint marketing_web_analytics_channels_daily_pkey;
  end if;
end $$;

alter table public.marketing_web_analytics_channels_daily
  add constraint marketing_web_analytics_channels_daily_pkey
  primary key (snapshot_date, default_channel_group);

-- Canonical country totals for top-country surface (no rollup from geo-detail rows).
create table if not exists public.marketing_web_analytics_country_daily (
  snapshot_date date not null,
  country text not null,
  sessions numeric not null default 0,
  total_users numeric not null default 0,
  engaged_sessions numeric not null default 0,
  key_events numeric not null default 0,
  engagement_rate numeric not null default 0,
  key_event_rate numeric not null default 0,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  primary key (snapshot_date, country)
);

create index if not exists idx_marketing_country_snapshot_sessions
  on public.marketing_web_analytics_country_daily (snapshot_date desc, sessions desc);

alter table public.marketing_web_analytics_country_daily enable row level security;

drop policy if exists marketing_web_analytics_country_select_authenticated
on public.marketing_web_analytics_country_daily;
create policy marketing_web_analytics_country_select_authenticated
on public.marketing_web_analytics_country_daily for select
using (auth.role() = 'authenticated');

drop policy if exists marketing_web_analytics_country_insert_service
on public.marketing_web_analytics_country_daily;
create policy marketing_web_analytics_country_insert_service
on public.marketing_web_analytics_country_daily for insert
with check (auth.role() = 'service_role');

drop policy if exists marketing_web_analytics_country_update_service
on public.marketing_web_analytics_country_daily;
create policy marketing_web_analytics_country_update_service
on public.marketing_web_analytics_country_daily for update
using (auth.role() = 'service_role')
with check (auth.role() = 'service_role');

-- Persist enrichment slices so 30d surfaces remain freshness-aligned.
create table if not exists public.marketing_web_analytics_demographics_daily (
  snapshot_date date not null,
  age_bracket text not null,
  gender text not null,
  sessions numeric not null default 0,
  total_users numeric not null default 0,
  engaged_sessions numeric not null default 0,
  key_events numeric not null default 0,
  engagement_rate numeric not null default 0,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  primary key (snapshot_date, age_bracket, gender)
);

create table if not exists public.marketing_web_analytics_devices_daily (
  snapshot_date date not null,
  device_category text not null,
  sessions numeric not null default 0,
  total_users numeric not null default 0,
  engaged_sessions numeric not null default 0,
  key_events numeric not null default 0,
  engagement_rate numeric not null default 0,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  primary key (snapshot_date, device_category)
);

create table if not exists public.marketing_web_analytics_internal_search_daily (
  snapshot_date date not null,
  search_term text not null,
  event_count numeric not null default 0,
  total_users numeric not null default 0,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  primary key (snapshot_date, search_term)
);

create index if not exists idx_marketing_demographics_snapshot_sessions
  on public.marketing_web_analytics_demographics_daily (snapshot_date desc, sessions desc);

create index if not exists idx_marketing_devices_snapshot_sessions
  on public.marketing_web_analytics_devices_daily (snapshot_date desc, sessions desc);

create index if not exists idx_marketing_internal_search_snapshot_event_count
  on public.marketing_web_analytics_internal_search_daily (snapshot_date desc, event_count desc);

alter table public.marketing_web_analytics_demographics_daily enable row level security;
alter table public.marketing_web_analytics_devices_daily enable row level security;
alter table public.marketing_web_analytics_internal_search_daily enable row level security;

drop policy if exists marketing_web_analytics_demographics_select_authenticated
on public.marketing_web_analytics_demographics_daily;
create policy marketing_web_analytics_demographics_select_authenticated
on public.marketing_web_analytics_demographics_daily for select
using (auth.role() = 'authenticated');

drop policy if exists marketing_web_analytics_demographics_insert_service
on public.marketing_web_analytics_demographics_daily;
create policy marketing_web_analytics_demographics_insert_service
on public.marketing_web_analytics_demographics_daily for insert
with check (auth.role() = 'service_role');

drop policy if exists marketing_web_analytics_demographics_update_service
on public.marketing_web_analytics_demographics_daily;
create policy marketing_web_analytics_demographics_update_service
on public.marketing_web_analytics_demographics_daily for update
using (auth.role() = 'service_role')
with check (auth.role() = 'service_role');

drop policy if exists marketing_web_analytics_devices_select_authenticated
on public.marketing_web_analytics_devices_daily;
create policy marketing_web_analytics_devices_select_authenticated
on public.marketing_web_analytics_devices_daily for select
using (auth.role() = 'authenticated');

drop policy if exists marketing_web_analytics_devices_insert_service
on public.marketing_web_analytics_devices_daily;
create policy marketing_web_analytics_devices_insert_service
on public.marketing_web_analytics_devices_daily for insert
with check (auth.role() = 'service_role');

drop policy if exists marketing_web_analytics_devices_update_service
on public.marketing_web_analytics_devices_daily;
create policy marketing_web_analytics_devices_update_service
on public.marketing_web_analytics_devices_daily for update
using (auth.role() = 'service_role')
with check (auth.role() = 'service_role');

drop policy if exists marketing_web_analytics_internal_search_select_authenticated
on public.marketing_web_analytics_internal_search_daily;
create policy marketing_web_analytics_internal_search_select_authenticated
on public.marketing_web_analytics_internal_search_daily for select
using (auth.role() = 'authenticated');

drop policy if exists marketing_web_analytics_internal_search_insert_service
on public.marketing_web_analytics_internal_search_daily;
create policy marketing_web_analytics_internal_search_insert_service
on public.marketing_web_analytics_internal_search_daily for insert
with check (auth.role() = 'service_role');

drop policy if exists marketing_web_analytics_internal_search_update_service
on public.marketing_web_analytics_internal_search_daily;
create policy marketing_web_analytics_internal_search_update_service
on public.marketing_web_analytics_internal_search_daily for update
using (auth.role() = 'service_role')
with check (auth.role() = 'service_role');

-- Persist exact period summaries used by overview KPI windows.
create table if not exists public.marketing_web_analytics_overview_period_summaries (
  as_of_date date not null,
  summary_key text not null,
  start_date date not null,
  end_date date not null,
  sessions numeric not null default 0,
  total_users numeric not null default 0,
  engaged_sessions numeric not null default 0,
  key_events numeric not null default 0,
  engagement_rate numeric not null default 0,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  primary key (as_of_date, summary_key)
);

create index if not exists idx_marketing_overview_period_summaries_as_of_date
  on public.marketing_web_analytics_overview_period_summaries (as_of_date desc);

alter table public.marketing_web_analytics_overview_period_summaries enable row level security;

drop policy if exists marketing_web_analytics_overview_period_summaries_select_authenticated
on public.marketing_web_analytics_overview_period_summaries;
create policy marketing_web_analytics_overview_period_summaries_select_authenticated
on public.marketing_web_analytics_overview_period_summaries for select
using (auth.role() = 'authenticated');

drop policy if exists marketing_web_analytics_overview_period_summaries_insert_service
on public.marketing_web_analytics_overview_period_summaries;
create policy marketing_web_analytics_overview_period_summaries_insert_service
on public.marketing_web_analytics_overview_period_summaries for insert
with check (auth.role() = 'service_role');

drop policy if exists marketing_web_analytics_overview_period_summaries_update_service
on public.marketing_web_analytics_overview_period_summaries;
create policy marketing_web_analytics_overview_period_summaries_update_service
on public.marketing_web_analytics_overview_period_summaries for update
using (auth.role() = 'service_role')
with check (auth.role() = 'service_role');
