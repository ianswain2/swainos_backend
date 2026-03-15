-- Add detailed page-activity and geography breakdown storage for marketing analytics.

create table if not exists public.marketing_web_analytics_page_activity_daily (
  snapshot_date date not null,
  page_path text not null,
  page_title text,
  screen_page_views numeric not null default 0,
  sessions numeric not null default 0,
  total_users numeric not null default 0,
  engaged_sessions numeric not null default 0,
  key_events numeric not null default 0,
  engagement_rate numeric not null default 0,
  key_event_rate numeric not null default 0,
  avg_session_duration_seconds numeric,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  primary key (snapshot_date, page_path)
);

create table if not exists public.marketing_web_analytics_geo_daily (
  snapshot_date date not null,
  country text not null,
  region text not null default '',
  city text not null default '',
  sessions numeric not null default 0,
  total_users numeric not null default 0,
  engaged_sessions numeric not null default 0,
  key_events numeric not null default 0,
  engagement_rate numeric not null default 0,
  key_event_rate numeric not null default 0,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  primary key (snapshot_date, country, region, city)
);

create index if not exists idx_marketing_page_activity_snapshot_sessions
  on public.marketing_web_analytics_page_activity_daily (snapshot_date desc, sessions desc);

create index if not exists idx_marketing_geo_snapshot_sessions
  on public.marketing_web_analytics_geo_daily (snapshot_date desc, sessions desc);

alter table public.marketing_web_analytics_page_activity_daily enable row level security;
alter table public.marketing_web_analytics_geo_daily enable row level security;

create policy marketing_web_analytics_page_activity_select_authenticated
on public.marketing_web_analytics_page_activity_daily for select
using (auth.role() = 'authenticated');

create policy marketing_web_analytics_page_activity_insert_service
on public.marketing_web_analytics_page_activity_daily for insert
with check (auth.role() = 'service_role');

create policy marketing_web_analytics_page_activity_update_service
on public.marketing_web_analytics_page_activity_daily for update
using (auth.role() = 'service_role')
with check (auth.role() = 'service_role');

create policy marketing_web_analytics_geo_select_authenticated
on public.marketing_web_analytics_geo_daily for select
using (auth.role() = 'authenticated');

create policy marketing_web_analytics_geo_insert_service
on public.marketing_web_analytics_geo_daily for insert
with check (auth.role() = 'service_role');

create policy marketing_web_analytics_geo_update_service
on public.marketing_web_analytics_geo_daily for update
using (auth.role() = 'service_role')
with check (auth.role() = 'service_role');
