create extension if not exists pg_trgm;

create table if not exists public.travel_agencies (
  id uuid primary key default gen_random_uuid(),
  external_id text not null unique,
  agency_name text not null,
  iata_code text null,
  host_identifier text null,
  created_at timestamptz not null default timezone('utc', now()),
  updated_at timestamptz not null default timezone('utc', now())
);

create table if not exists public.travel_agents (
  id uuid primary key default gen_random_uuid(),
  external_id text not null unique,
  agency_id uuid not null references public.travel_agencies(id),
  first_name text not null default '',
  last_name text not null default '',
  email text null,
  created_at timestamptz not null default timezone('utc', now()),
  updated_at timestamptz not null default timezone('utc', now())
);

create table if not exists public.travel_agent_agency_assignments (
  id uuid primary key default gen_random_uuid(),
  agent_id uuid not null references public.travel_agents(id),
  agency_id uuid not null references public.travel_agencies(id),
  effective_from date not null,
  effective_to date null,
  is_primary boolean not null default true,
  created_at timestamptz not null default timezone('utc', now()),
  updated_at timestamptz not null default timezone('utc', now())
);

create unique index if not exists idx_travel_agent_agency_assignments_unique_active
  on public.travel_agent_agency_assignments(agent_id, agency_id, effective_from);

create table if not exists public.travel_agent_monthly_rollup (
  period_start date not null,
  period_end date not null,
  agent_id uuid not null references public.travel_agents(id),
  agent_external_id text not null,
  agent_name text not null,
  agent_email text null,
  agency_id uuid not null references public.travel_agencies(id),
  agency_external_id text not null,
  agency_name text not null,
  leads_count integer not null default 0,
  converted_leads_count integer not null default 0,
  traveled_itineraries_count integer not null default 0,
  gross_amount numeric(14, 2) not null default 0,
  gross_profit_amount numeric(14, 2) not null default 0,
  created_at timestamptz not null default timezone('utc', now()),
  updated_at timestamptz not null default timezone('utc', now()),
  primary key (period_start, agent_id)
);

create table if not exists public.travel_agency_monthly_rollup (
  period_start date not null,
  period_end date not null,
  agency_id uuid not null references public.travel_agencies(id),
  agency_external_id text not null,
  agency_name text not null,
  leads_count integer not null default 0,
  converted_leads_count integer not null default 0,
  traveled_itineraries_count integer not null default 0,
  gross_amount numeric(14, 2) not null default 0,
  gross_profit_amount numeric(14, 2) not null default 0,
  active_agents_count integer not null default 0,
  created_at timestamptz not null default timezone('utc', now()),
  updated_at timestamptz not null default timezone('utc', now()),
  primary key (period_start, agency_id)
);

create table if not exists public.travel_agent_consultant_affinity_monthly_rollup (
  period_start date not null,
  period_end date not null,
  agent_id uuid not null references public.travel_agents(id),
  agent_external_id text not null,
  agent_name text not null,
  employee_id uuid not null references public.employees(id),
  employee_external_id text not null,
  employee_first_name text not null,
  employee_last_name text not null,
  converted_leads_count integer not null default 0,
  closed_won_itineraries_count integer not null default 0,
  created_at timestamptz not null default timezone('utc', now()),
  updated_at timestamptz not null default timezone('utc', now()),
  primary key (period_start, agent_id, employee_id)
);

create table if not exists public.travel_trade_search_index (
  entity_type text not null check (entity_type in ('agent', 'agency')),
  entity_id text not null,
  entity_external_id text not null,
  display_name text not null,
  email text null,
  agency_name text null,
  iata_code text null,
  host_identifier text null,
  search_text text not null,
  rank_score numeric(14, 4) not null default 0,
  updated_at timestamptz not null default timezone('utc', now()),
  primary key (entity_type, entity_id)
);

create index if not exists idx_travel_agent_monthly_rollup_period_gp
  on public.travel_agent_monthly_rollup(period_start desc, gross_profit_amount desc, agent_id);

create index if not exists idx_travel_agency_monthly_rollup_period_gp
  on public.travel_agency_monthly_rollup(period_start desc, gross_profit_amount desc, agency_id);

create index if not exists idx_travel_agent_affinity_period_agent
  on public.travel_agent_consultant_affinity_monthly_rollup(period_start desc, agent_id, converted_leads_count desc);

create index if not exists idx_travel_trade_search_index_tsv
  on public.travel_trade_search_index using gin (to_tsvector('simple', search_text));

create index if not exists idx_travel_trade_search_index_trgm
  on public.travel_trade_search_index using gin (search_text gin_trgm_ops);

create or replace function public.refresh_travel_trade_rollups_v1()
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
declare
  now_utc timestamptz := timezone('utc', now());
begin
  insert into public.travel_agencies (external_id, agency_name, iata_code, host_identifier, updated_at)
  select
    trim(a.external_id),
    coalesce(nullif(trim(a.agency_name), ''), 'Unnamed Agency'),
    nullif(trim(a.iata_number), ''),
    nullif(trim(a.host_agency_name), ''),
    now_utc
  from public.agencies a
  where nullif(trim(a.external_id), '') is not null
  on conflict (external_id) do update
  set
    agency_name = excluded.agency_name,
    iata_code = excluded.iata_code,
    host_identifier = excluded.host_identifier,
    updated_at = excluded.updated_at;

  insert into public.travel_agents (external_id, agency_id, first_name, last_name, email, updated_at)
  select
    trim(c.external_id) as external_id,
    ta.id as agency_id,
    coalesce(trim(c.first_name), '') as first_name,
    coalesce(trim(c.last_name), '') as last_name,
    nullif(trim(c.email), '') as email,
    now_utc
  from public.contacts c
  join public.agencies a
    on a.id = c.agency_id
  join public.travel_agencies ta
    on ta.external_id = trim(a.external_id)
  where nullif(trim(c.external_id), '') is not null
    and nullif(trim(a.external_id), '') is not null
    and c.agency_id is not null
  on conflict (external_id) do update
  set
    agency_id = excluded.agency_id,
    first_name = excluded.first_name,
    last_name = excluded.last_name,
    email = excluded.email,
    updated_at = excluded.updated_at;

  insert into public.travel_agent_agency_assignments (
    agent_id,
    agency_id,
    effective_from,
    effective_to,
    is_primary,
    updated_at
  )
  select
    t.id as agent_id,
    t.agency_id,
    current_date as effective_from,
    null as effective_to,
    true as is_primary,
    now_utc
  from public.travel_agents t
  on conflict (agent_id, agency_id, effective_from) do update
  set
    effective_to = excluded.effective_to,
    is_primary = excluded.is_primary,
    updated_at = excluded.updated_at;

  truncate table public.travel_agent_monthly_rollup;
  truncate table public.travel_agency_monthly_rollup;
  truncate table public.travel_agent_consultant_affinity_monthly_rollup;
  truncate table public.travel_trade_search_index;

  with trade_source as (
    select
      i.id as itinerary_id,
      i.employee_id,
      i.created_at,
      i.travel_end_date,
      coalesce(i.gross_amount, 0) as gross_amount,
      coalesce(i.gross_profit, 0) as gross_profit_amount,
      ta.id as agent_id,
      ta.external_id as agent_external_id,
      trim(concat(ta.first_name, ' ', ta.last_name)) as agent_name,
      ta.email as agent_email,
      tg.id as agency_id,
      tg.external_id as agency_external_id,
      tg.agency_name,
      coalesce(sr.pipeline_bucket, 'open') = 'closed_won' as is_closed_won
    from public.itineraries i
    join public.contacts c
      on c.id = i.primary_contact_id
    join public.travel_agents ta
      on ta.external_id = trim(c.external_id)
    join public.travel_agencies tg
      on tg.id = ta.agency_id
    left join public.itinerary_status_reference sr
      on sr.status_value = i.itinerary_status
    where coalesce(sr.is_filter_out, false) = false
      and nullif(trim(i.consortia), '') is null
      and c.agency_id is not null
      and nullif(trim(c.external_id), '') is not null
      and nullif(trim(tg.external_id), '') is not null
  ),
  lead_agg as (
    select
      date_trunc('month', ts.created_at)::date as period_start,
      (date_trunc('month', ts.created_at)::date + interval '1 month - 1 day')::date as period_end,
      ts.agent_id,
      ts.agent_external_id,
      ts.agent_name,
      ts.agent_email,
      ts.agency_id,
      ts.agency_external_id,
      ts.agency_name,
      count(*) as leads_count,
      sum(case when ts.is_closed_won then 1 else 0 end) as converted_leads_count
    from trade_source ts
    where ts.created_at is not null
    group by 1, 2, 3, 4, 5, 6, 7, 8, 9
  ),
  travel_agg as (
    select
      date_trunc('month', ts.travel_end_date)::date as period_start,
      (date_trunc('month', ts.travel_end_date)::date + interval '1 month - 1 day')::date as period_end,
      ts.agent_id,
      ts.agent_external_id,
      ts.agent_name,
      ts.agent_email,
      ts.agency_id,
      ts.agency_external_id,
      ts.agency_name,
      count(*) as traveled_itineraries_count,
      sum(ts.gross_amount)::numeric(14, 2) as gross_amount,
      sum(ts.gross_profit_amount)::numeric(14, 2) as gross_profit_amount
    from trade_source ts
    where ts.travel_end_date is not null
      and ts.is_closed_won = true
    group by 1, 2, 3, 4, 5, 6, 7, 8, 9
  )
  insert into public.travel_agent_monthly_rollup (
    period_start,
    period_end,
    agent_id,
    agent_external_id,
    agent_name,
    agent_email,
    agency_id,
    agency_external_id,
    agency_name,
    leads_count,
    converted_leads_count,
    traveled_itineraries_count,
    gross_amount,
    gross_profit_amount,
    updated_at
  )
  select
    coalesce(l.period_start, t.period_start) as period_start,
    coalesce(l.period_end, t.period_end) as period_end,
    coalesce(l.agent_id, t.agent_id) as agent_id,
    coalesce(l.agent_external_id, t.agent_external_id) as agent_external_id,
    coalesce(l.agent_name, t.agent_name) as agent_name,
    coalesce(l.agent_email, t.agent_email) as agent_email,
    coalesce(l.agency_id, t.agency_id) as agency_id,
    coalesce(l.agency_external_id, t.agency_external_id) as agency_external_id,
    coalesce(l.agency_name, t.agency_name) as agency_name,
    coalesce(l.leads_count, 0)::integer as leads_count,
    coalesce(l.converted_leads_count, 0)::integer as converted_leads_count,
    coalesce(t.traveled_itineraries_count, 0)::integer as traveled_itineraries_count,
    coalesce(t.gross_amount, 0)::numeric(14, 2) as gross_amount,
    coalesce(t.gross_profit_amount, 0)::numeric(14, 2) as gross_profit_amount,
    now_utc
  from lead_agg l
  full outer join travel_agg t
    on l.period_start = t.period_start
    and l.agent_id = t.agent_id;

  insert into public.travel_agency_monthly_rollup (
    period_start,
    period_end,
    agency_id,
    agency_external_id,
    agency_name,
    leads_count,
    converted_leads_count,
    traveled_itineraries_count,
    gross_amount,
    gross_profit_amount,
    active_agents_count,
    updated_at
  )
  select
    r.period_start,
    r.period_end,
    r.agency_id,
    r.agency_external_id,
    r.agency_name,
    sum(r.leads_count)::integer as leads_count,
    sum(r.converted_leads_count)::integer as converted_leads_count,
    sum(r.traveled_itineraries_count)::integer as traveled_itineraries_count,
    sum(r.gross_amount)::numeric(14, 2) as gross_amount,
    sum(r.gross_profit_amount)::numeric(14, 2) as gross_profit_amount,
    count(distinct r.agent_id)::integer as active_agents_count,
    now_utc
  from public.travel_agent_monthly_rollup r
  group by 1, 2, 3, 4, 5;

  insert into public.travel_agent_consultant_affinity_monthly_rollup (
    period_start,
    period_end,
    agent_id,
    agent_external_id,
    agent_name,
    employee_id,
    employee_external_id,
    employee_first_name,
    employee_last_name,
    converted_leads_count,
    closed_won_itineraries_count,
    updated_at
  )
  select
    date_trunc('month', ts.created_at)::date as period_start,
    (date_trunc('month', ts.created_at)::date + interval '1 month - 1 day')::date as period_end,
    ts.agent_id,
    ts.agent_external_id,
    ts.agent_name,
    e.id as employee_id,
    coalesce(e.external_id, '') as employee_external_id,
    coalesce(e.first_name, '') as employee_first_name,
    coalesce(e.last_name, '') as employee_last_name,
    sum(case when ts.is_closed_won then 1 else 0 end)::integer as converted_leads_count,
    sum(case when ts.is_closed_won and ts.travel_end_date is not null then 1 else 0 end)::integer as closed_won_itineraries_count,
    now_utc
  from trade_source ts
  join public.employees e
    on e.id = ts.employee_id
  where ts.created_at is not null
    and coalesce(e.analysis_disabled, false) = false
  group by 1, 2, 3, 4, 5, 6, 7, 8, 9;

  insert into public.travel_trade_search_index (
    entity_type,
    entity_id,
    entity_external_id,
    display_name,
    email,
    agency_name,
    iata_code,
    host_identifier,
    search_text,
    rank_score,
    updated_at
  )
  select
    'agent' as entity_type,
    ta.id::text as entity_id,
    ta.external_id as entity_external_id,
    trim(concat(ta.first_name, ' ', ta.last_name)) as display_name,
    ta.email,
    tg.agency_name,
    tg.iata_code,
    tg.host_identifier,
    trim(
      concat_ws(
        ' ',
        trim(concat(ta.first_name, ' ', ta.last_name)),
        coalesce(ta.email, ''),
        tg.agency_name,
        coalesce(tg.iata_code, ''),
        coalesce(tg.host_identifier, ''),
        ta.external_id,
        tg.external_id
      )
    ) as search_text,
    coalesce(max(ar.gross_profit_amount), 0) as rank_score,
    now_utc
  from public.travel_agents ta
  join public.travel_agencies tg
    on tg.id = ta.agency_id
  left join public.travel_agent_monthly_rollup ar
    on ar.agent_id = ta.id
  group by 1, 2, 3, 4, 5, 6, 7, 8, 10, 11
  union all
  select
    'agency' as entity_type,
    tg.id::text as entity_id,
    tg.external_id as entity_external_id,
    tg.agency_name as display_name,
    null as email,
    tg.agency_name,
    tg.iata_code,
    tg.host_identifier,
    trim(
      concat_ws(
        ' ',
        tg.agency_name,
        coalesce(tg.iata_code, ''),
        coalesce(tg.host_identifier, ''),
        tg.external_id
      )
    ) as search_text,
    coalesce(max(gr.gross_profit_amount), 0) as rank_score,
    now_utc
  from public.travel_agencies tg
  left join public.travel_agency_monthly_rollup gr
    on gr.agency_id = tg.id
  group by 1, 2, 3, 4, 5, 6, 7, 8, 10, 11;

  return jsonb_build_object(
    'status', 'ok',
    'refreshedAt', now_utc
  );
end;
$$;

revoke all on function public.refresh_travel_trade_rollups_v1() from public;
revoke all on function public.refresh_travel_trade_rollups_v1() from anon;
revoke all on function public.refresh_travel_trade_rollups_v1() from authenticated;
grant execute on function public.refresh_travel_trade_rollups_v1() to service_role;

grant select on public.travel_agent_monthly_rollup to authenticated;
grant select on public.travel_agency_monthly_rollup to authenticated;
grant select on public.travel_agent_consultant_affinity_monthly_rollup to authenticated;
grant select on public.travel_trade_search_index to authenticated;
grant all on public.travel_agencies to service_role;
grant all on public.travel_agents to service_role;
grant all on public.travel_agent_agency_assignments to service_role;
grant all on public.travel_agent_monthly_rollup to service_role;
grant all on public.travel_agency_monthly_rollup to service_role;
grant all on public.travel_agent_consultant_affinity_monthly_rollup to service_role;
grant all on public.travel_trade_search_index to service_role;

alter table public.travel_agencies enable row level security;
alter table public.travel_agents enable row level security;
alter table public.travel_agent_agency_assignments enable row level security;
alter table public.travel_agent_monthly_rollup enable row level security;
alter table public.travel_agency_monthly_rollup enable row level security;
alter table public.travel_agent_consultant_affinity_monthly_rollup enable row level security;
alter table public.travel_trade_search_index enable row level security;

drop policy if exists travel_agencies_select_authenticated on public.travel_agencies;
drop policy if exists travel_agencies_service_write on public.travel_agencies;
drop policy if exists travel_agents_select_authenticated on public.travel_agents;
drop policy if exists travel_agents_service_write on public.travel_agents;
drop policy if exists travel_agent_agency_assignments_select_authenticated on public.travel_agent_agency_assignments;
drop policy if exists travel_agent_agency_assignments_service_write on public.travel_agent_agency_assignments;
drop policy if exists travel_agent_monthly_rollup_select_authenticated on public.travel_agent_monthly_rollup;
drop policy if exists travel_agent_monthly_rollup_service_write on public.travel_agent_monthly_rollup;
drop policy if exists travel_agency_monthly_rollup_select_authenticated on public.travel_agency_monthly_rollup;
drop policy if exists travel_agency_monthly_rollup_service_write on public.travel_agency_monthly_rollup;
drop policy if exists travel_agent_consultant_affinity_monthly_rollup_select_authenticated on public.travel_agent_consultant_affinity_monthly_rollup;
drop policy if exists travel_agent_consultant_affinity_monthly_rollup_service_write on public.travel_agent_consultant_affinity_monthly_rollup;
drop policy if exists travel_trade_search_index_select_authenticated on public.travel_trade_search_index;
drop policy if exists travel_trade_search_index_service_write on public.travel_trade_search_index;

create policy travel_agencies_select_authenticated
on public.travel_agencies for select
using (auth.role() = 'authenticated');

create policy travel_agencies_service_write
on public.travel_agencies for all
using (auth.role() = 'service_role')
with check (auth.role() = 'service_role');

create policy travel_agents_select_authenticated
on public.travel_agents for select
using (auth.role() = 'authenticated');

create policy travel_agents_service_write
on public.travel_agents for all
using (auth.role() = 'service_role')
with check (auth.role() = 'service_role');

create policy travel_agent_agency_assignments_select_authenticated
on public.travel_agent_agency_assignments for select
using (auth.role() = 'authenticated');

create policy travel_agent_agency_assignments_service_write
on public.travel_agent_agency_assignments for all
using (auth.role() = 'service_role')
with check (auth.role() = 'service_role');

create policy travel_agent_monthly_rollup_select_authenticated
on public.travel_agent_monthly_rollup for select
using (auth.role() = 'authenticated');

create policy travel_agent_monthly_rollup_service_write
on public.travel_agent_monthly_rollup for all
using (auth.role() = 'service_role')
with check (auth.role() = 'service_role');

create policy travel_agency_monthly_rollup_select_authenticated
on public.travel_agency_monthly_rollup for select
using (auth.role() = 'authenticated');

create policy travel_agency_monthly_rollup_service_write
on public.travel_agency_monthly_rollup for all
using (auth.role() = 'service_role')
with check (auth.role() = 'service_role');

create policy travel_agent_consultant_affinity_monthly_rollup_select_authenticated
on public.travel_agent_consultant_affinity_monthly_rollup for select
using (auth.role() = 'authenticated');

create policy travel_agent_consultant_affinity_monthly_rollup_service_write
on public.travel_agent_consultant_affinity_monthly_rollup for all
using (auth.role() = 'service_role')
with check (auth.role() = 'service_role');

create policy travel_trade_search_index_select_authenticated
on public.travel_trade_search_index for select
using (auth.role() = 'authenticated');

create policy travel_trade_search_index_service_write
on public.travel_trade_search_index for all
using (auth.role() = 'service_role')
with check (auth.role() = 'service_role');
