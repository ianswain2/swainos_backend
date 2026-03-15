-- Search Console canonical analytics facts and query-serving marts.

create table if not exists public.marketing_search_console_daily (
  snapshot_date date not null,
  country_scope text not null default 'all',
  device_scope text not null default 'all',
  clicks numeric not null default 0,
  impressions numeric not null default 0,
  ctr numeric not null default 0,
  average_position numeric not null default 0,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  primary key (snapshot_date, country_scope, device_scope)
);

create table if not exists public.marketing_search_console_query_daily (
  snapshot_date date not null,
  query text not null,
  country_scope text not null default 'all',
  device_scope text not null default 'all',
  clicks numeric not null default 0,
  impressions numeric not null default 0,
  ctr numeric not null default 0,
  average_position numeric not null default 0,
  is_branded boolean not null default false,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  primary key (snapshot_date, query, country_scope, device_scope)
);

create table if not exists public.marketing_search_console_page_daily (
  snapshot_date date not null,
  page_path text not null,
  country_scope text not null default 'all',
  device_scope text not null default 'all',
  clicks numeric not null default 0,
  impressions numeric not null default 0,
  ctr numeric not null default 0,
  average_position numeric not null default 0,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  primary key (snapshot_date, page_path, country_scope, device_scope)
);

create table if not exists public.marketing_search_console_page_query_daily (
  snapshot_date date not null,
  page_path text not null,
  query text not null,
  country_scope text not null default 'all',
  device_scope text not null default 'all',
  clicks numeric not null default 0,
  impressions numeric not null default 0,
  ctr numeric not null default 0,
  average_position numeric not null default 0,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  primary key (snapshot_date, page_path, query, country_scope, device_scope)
);

create table if not exists public.marketing_search_console_country_daily (
  snapshot_date date not null,
  country text not null,
  clicks numeric not null default 0,
  impressions numeric not null default 0,
  ctr numeric not null default 0,
  average_position numeric not null default 0,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  primary key (snapshot_date, country)
);

create table if not exists public.marketing_search_console_device_daily (
  snapshot_date date not null,
  device text not null,
  clicks numeric not null default 0,
  impressions numeric not null default 0,
  ctr numeric not null default 0,
  average_position numeric not null default 0,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  primary key (snapshot_date, device)
);

create index if not exists idx_marketing_search_console_daily_snapshot
  on public.marketing_search_console_daily (snapshot_date desc, country_scope, device_scope);
create index if not exists idx_marketing_search_console_query_snapshot_impressions
  on public.marketing_search_console_query_daily (snapshot_date desc, country_scope, device_scope, impressions desc);
create index if not exists idx_marketing_search_console_page_snapshot_impressions
  on public.marketing_search_console_page_daily (snapshot_date desc, country_scope, device_scope, impressions desc);
create index if not exists idx_marketing_search_console_page_query_snapshot_impressions
  on public.marketing_search_console_page_query_daily (snapshot_date desc, country_scope, device_scope, impressions desc);
create index if not exists idx_marketing_search_console_country_snapshot_clicks
  on public.marketing_search_console_country_daily (snapshot_date desc, clicks desc);
create index if not exists idx_marketing_search_console_device_snapshot_clicks
  on public.marketing_search_console_device_daily (snapshot_date desc, clicks desc);

alter table public.marketing_search_console_daily enable row level security;
alter table public.marketing_search_console_query_daily enable row level security;
alter table public.marketing_search_console_page_daily enable row level security;
alter table public.marketing_search_console_page_query_daily enable row level security;
alter table public.marketing_search_console_country_daily enable row level security;
alter table public.marketing_search_console_device_daily enable row level security;

drop policy if exists marketing_search_console_daily_select_authenticated on public.marketing_search_console_daily;
create policy marketing_search_console_daily_select_authenticated
on public.marketing_search_console_daily for select
using (auth.role() = 'authenticated');

drop policy if exists marketing_search_console_daily_insert_service on public.marketing_search_console_daily;
create policy marketing_search_console_daily_insert_service
on public.marketing_search_console_daily for insert
with check (auth.role() = 'service_role');

drop policy if exists marketing_search_console_daily_update_service on public.marketing_search_console_daily;
create policy marketing_search_console_daily_update_service
on public.marketing_search_console_daily for update
using (auth.role() = 'service_role')
with check (auth.role() = 'service_role');

drop policy if exists marketing_search_console_query_select_authenticated on public.marketing_search_console_query_daily;
create policy marketing_search_console_query_select_authenticated
on public.marketing_search_console_query_daily for select
using (auth.role() = 'authenticated');

drop policy if exists marketing_search_console_query_insert_service on public.marketing_search_console_query_daily;
create policy marketing_search_console_query_insert_service
on public.marketing_search_console_query_daily for insert
with check (auth.role() = 'service_role');

drop policy if exists marketing_search_console_query_update_service on public.marketing_search_console_query_daily;
create policy marketing_search_console_query_update_service
on public.marketing_search_console_query_daily for update
using (auth.role() = 'service_role')
with check (auth.role() = 'service_role');

drop policy if exists marketing_search_console_page_select_authenticated on public.marketing_search_console_page_daily;
create policy marketing_search_console_page_select_authenticated
on public.marketing_search_console_page_daily for select
using (auth.role() = 'authenticated');

drop policy if exists marketing_search_console_page_insert_service on public.marketing_search_console_page_daily;
create policy marketing_search_console_page_insert_service
on public.marketing_search_console_page_daily for insert
with check (auth.role() = 'service_role');

drop policy if exists marketing_search_console_page_update_service on public.marketing_search_console_page_daily;
create policy marketing_search_console_page_update_service
on public.marketing_search_console_page_daily for update
using (auth.role() = 'service_role')
with check (auth.role() = 'service_role');

drop policy if exists marketing_search_console_page_query_select_authenticated on public.marketing_search_console_page_query_daily;
create policy marketing_search_console_page_query_select_authenticated
on public.marketing_search_console_page_query_daily for select
using (auth.role() = 'authenticated');

drop policy if exists marketing_search_console_page_query_insert_service on public.marketing_search_console_page_query_daily;
create policy marketing_search_console_page_query_insert_service
on public.marketing_search_console_page_query_daily for insert
with check (auth.role() = 'service_role');

drop policy if exists marketing_search_console_page_query_update_service on public.marketing_search_console_page_query_daily;
create policy marketing_search_console_page_query_update_service
on public.marketing_search_console_page_query_daily for update
using (auth.role() = 'service_role')
with check (auth.role() = 'service_role');

drop policy if exists marketing_search_console_country_select_authenticated on public.marketing_search_console_country_daily;
create policy marketing_search_console_country_select_authenticated
on public.marketing_search_console_country_daily for select
using (auth.role() = 'authenticated');

drop policy if exists marketing_search_console_country_insert_service on public.marketing_search_console_country_daily;
create policy marketing_search_console_country_insert_service
on public.marketing_search_console_country_daily for insert
with check (auth.role() = 'service_role');

drop policy if exists marketing_search_console_country_update_service on public.marketing_search_console_country_daily;
create policy marketing_search_console_country_update_service
on public.marketing_search_console_country_daily for update
using (auth.role() = 'service_role')
with check (auth.role() = 'service_role');

drop policy if exists marketing_search_console_device_select_authenticated on public.marketing_search_console_device_daily;
create policy marketing_search_console_device_select_authenticated
on public.marketing_search_console_device_daily for select
using (auth.role() = 'authenticated');

drop policy if exists marketing_search_console_device_insert_service on public.marketing_search_console_device_daily;
create policy marketing_search_console_device_insert_service
on public.marketing_search_console_device_daily for insert
with check (auth.role() = 'service_role');

drop policy if exists marketing_search_console_device_update_service on public.marketing_search_console_device_daily;
create policy marketing_search_console_device_update_service
on public.marketing_search_console_device_daily for update
using (auth.role() = 'service_role')
with check (auth.role() = 'service_role');
