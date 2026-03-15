-- Expand marketing analytics storage to persist channel/source-medium daily dimensions.

create table if not exists public.marketing_web_analytics_channels_daily (
  snapshot_date date not null,
  source_medium text not null,
  default_channel_group text not null,
  sessions numeric not null default 0,
  total_users numeric not null default 0,
  engaged_sessions numeric not null default 0,
  engagement_rate numeric not null default 0,
  key_events numeric not null default 0,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  primary key (snapshot_date, source_medium, default_channel_group)
);

create index if not exists idx_marketing_web_analytics_channels_snapshot_date_sessions
  on public.marketing_web_analytics_channels_daily (snapshot_date desc, sessions desc);

alter table public.marketing_web_analytics_channels_daily enable row level security;

create policy marketing_web_analytics_channels_select_authenticated
on public.marketing_web_analytics_channels_daily for select
using (auth.role() = 'authenticated');

create policy marketing_web_analytics_channels_insert_service
on public.marketing_web_analytics_channels_daily for insert
with check (auth.role() = 'service_role');

create policy marketing_web_analytics_channels_update_service
on public.marketing_web_analytics_channels_daily for update
using (auth.role() = 'service_role')
with check (auth.role() = 'service_role');
