create table if not exists public.travel_trade_lead_monthly_rollup (
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
  created_at timestamptz not null default timezone('utc', now()),
  updated_at timestamptz not null default timezone('utc', now()),
  primary key (period_start, agent_id)
);

create table if not exists public.travel_trade_booked_itinerary_monthly_rollup (
  period_start date not null,
  period_end date not null,
  agent_id uuid not null references public.travel_agents(id),
  agent_external_id text not null,
  agent_name text not null,
  agent_email text null,
  agency_id uuid not null references public.travel_agencies(id),
  agency_external_id text not null,
  agency_name text not null,
  booked_itineraries_count integer not null default 0,
  gross_amount numeric(14, 2) not null default 0,
  gross_profit_amount numeric(14, 2) not null default 0,
  created_at timestamptz not null default timezone('utc', now()),
  updated_at timestamptz not null default timezone('utc', now()),
  primary key (period_start, agent_id)
);

create index if not exists idx_trade_lead_rollup_period_agent
  on public.travel_trade_lead_monthly_rollup(period_start desc, agent_id);

create index if not exists idx_trade_booked_rollup_period_agent
  on public.travel_trade_booked_itinerary_monthly_rollup(period_start desc, agent_id);

grant select on public.travel_trade_lead_monthly_rollup to authenticated;
grant select on public.travel_trade_booked_itinerary_monthly_rollup to authenticated;
grant all on public.travel_trade_lead_monthly_rollup to service_role;
grant all on public.travel_trade_booked_itinerary_monthly_rollup to service_role;

alter table public.travel_trade_lead_monthly_rollup enable row level security;
alter table public.travel_trade_booked_itinerary_monthly_rollup enable row level security;

drop policy if exists travel_trade_lead_rollup_select_authenticated on public.travel_trade_lead_monthly_rollup;
drop policy if exists travel_trade_lead_rollup_service_write on public.travel_trade_lead_monthly_rollup;
drop policy if exists travel_trade_booked_rollup_select_authenticated on public.travel_trade_booked_itinerary_monthly_rollup;
drop policy if exists travel_trade_booked_rollup_service_write on public.travel_trade_booked_itinerary_monthly_rollup;

create policy travel_trade_lead_rollup_select_authenticated
on public.travel_trade_lead_monthly_rollup for select
using (auth.role() = 'authenticated');

create policy travel_trade_lead_rollup_service_write
on public.travel_trade_lead_monthly_rollup for all
using (auth.role() = 'service_role')
with check (auth.role() = 'service_role');

create policy travel_trade_booked_rollup_select_authenticated
on public.travel_trade_booked_itinerary_monthly_rollup for select
using (auth.role() = 'authenticated');

create policy travel_trade_booked_rollup_service_write
on public.travel_trade_booked_itinerary_monthly_rollup for all
using (auth.role() = 'service_role')
with check (auth.role() = 'service_role');

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
    nullif(
      trim(
        coalesce(
          to_jsonb(a)->>'iata_code',
          to_jsonb(a)->>'iata_number',
          to_jsonb(a)->>'agency_code',
          ''
        )
      ),
      ''
    ),
    nullif(
      trim(
        coalesce(
          to_jsonb(a)->>'host_identifier',
          to_jsonb(a)->>'host_agency_name',
          to_jsonb(a)->>'consortia',
          ''
        )
      ),
      ''
    ),
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
    coalesce(
      nullif(trim(coalesce(to_jsonb(c)->>'first_name', '')), ''),
      nullif(split_part(trim(coalesce(c.full_name, '')), ' ', 1), ''),
      ''
    ) as first_name,
    coalesce(
      nullif(trim(coalesce(to_jsonb(c)->>'last_name', '')), ''),
      nullif(trim(regexp_replace(trim(coalesce(c.full_name, '')), '^\S+\s*', '')), ''),
      ''
    ) as last_name,
    nullif(
      trim(
        coalesce(
          to_jsonb(c)->>'email',
          to_jsonb(c)->>'contact_email',
          ''
        )
      ),
      ''
    ) as email,
    now_utc
  from public.contacts c
  join public.agencies a
    on a.id = c.agency_id
  join public.travel_agencies ta
    on lower(trim(ta.external_id)) = lower(trim(a.external_id))
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

  update public.travel_agent_agency_assignments assignments
  set
    effective_to = current_date - 1,
    is_primary = false,
    updated_at = now_utc
  from public.travel_agents agents
  where assignments.agent_id = agents.id
    and assignments.effective_to is null
    and assignments.is_primary = true
    and assignments.agency_id <> agents.agency_id;

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
  where not exists (
    select 1
    from public.travel_agent_agency_assignments a
    where a.agent_id = t.id
      and a.agency_id = t.agency_id
      and a.effective_to is null
      and a.is_primary = true
  );

  truncate table public.travel_trade_lead_monthly_rollup;
  truncate table public.travel_trade_booked_itinerary_monthly_rollup;
  truncate table public.travel_agent_monthly_rollup;
  truncate table public.travel_agency_monthly_rollup;
  truncate table public.travel_agent_consultant_affinity_monthly_rollup;
  truncate table public.travel_trade_search_index;

  drop table if exists _travel_trade_source;
  create temporary table _travel_trade_source on commit drop as
  select
    i.id as itinerary_id,
    i.employee_id,
    i.created_at,
    i.travel_start_date,
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
    on lower(trim(ta.external_id)) = lower(trim(c.external_id))
  join public.travel_agencies tg
    on tg.id = ta.agency_id
  left join public.itinerary_status_reference sr
    on sr.status_value = i.itinerary_status
  where coalesce(sr.is_filter_out, false) = false
    and nullif(trim(i.consortia), '') is not null
    and lower(trim(i.consortia)) not in ('not applicable', 'n/a', 'na', 'none', 'null')
    and c.agency_id is not null
    and nullif(trim(c.external_id), '') is not null
    and nullif(trim(tg.external_id), '') is not null;

  insert into public.travel_trade_lead_monthly_rollup (
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
    updated_at
  )
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
    count(*)::integer as leads_count,
    sum(case when ts.is_closed_won then 1 else 0 end)::integer as converted_leads_count,
    now_utc
  from _travel_trade_source ts
  where ts.created_at is not null
  group by 1, 2, 3, 4, 5, 6, 7, 8, 9;

  insert into public.travel_trade_booked_itinerary_monthly_rollup (
    period_start,
    period_end,
    agent_id,
    agent_external_id,
    agent_name,
    agent_email,
    agency_id,
    agency_external_id,
    agency_name,
    booked_itineraries_count,
    gross_amount,
    gross_profit_amount,
    updated_at
  )
  select
    date_trunc('month', coalesce(ts.travel_start_date, ts.travel_end_date))::date as period_start,
    (date_trunc('month', coalesce(ts.travel_start_date, ts.travel_end_date))::date + interval '1 month - 1 day')::date as period_end,
    ts.agent_id,
    ts.agent_external_id,
    ts.agent_name,
    ts.agent_email,
    ts.agency_id,
    ts.agency_external_id,
    ts.agency_name,
    count(*)::integer as booked_itineraries_count,
    sum(ts.gross_amount)::numeric(14, 2) as gross_amount,
    sum(ts.gross_profit_amount)::numeric(14, 2) as gross_profit_amount,
    now_utc
  from _travel_trade_source ts
  where coalesce(ts.travel_start_date, ts.travel_end_date) is not null
    and ts.is_closed_won = true
  group by 1, 2, 3, 4, 5, 6, 7, 8, 9;

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
    coalesce(l.period_start, b.period_start) as period_start,
    coalesce(l.period_end, b.period_end) as period_end,
    coalesce(l.agent_id, b.agent_id) as agent_id,
    coalesce(l.agent_external_id, b.agent_external_id) as agent_external_id,
    coalesce(l.agent_name, b.agent_name) as agent_name,
    coalesce(l.agent_email, b.agent_email) as agent_email,
    coalesce(l.agency_id, b.agency_id) as agency_id,
    coalesce(l.agency_external_id, b.agency_external_id) as agency_external_id,
    coalesce(l.agency_name, b.agency_name) as agency_name,
    coalesce(l.leads_count, 0)::integer as leads_count,
    coalesce(l.converted_leads_count, 0)::integer as converted_leads_count,
    coalesce(b.booked_itineraries_count, 0)::integer as traveled_itineraries_count,
    coalesce(b.gross_amount, 0)::numeric(14, 2) as gross_amount,
    coalesce(b.gross_profit_amount, 0)::numeric(14, 2) as gross_profit_amount,
    now_utc
  from public.travel_trade_lead_monthly_rollup l
  full outer join public.travel_trade_booked_itinerary_monthly_rollup b
    on l.period_start = b.period_start
    and l.agent_id = b.agent_id;

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
    sum(case when ts.is_closed_won and coalesce(ts.travel_start_date, ts.travel_end_date) is not null then 1 else 0 end)::integer as closed_won_itineraries_count,
    now_utc
  from _travel_trade_source ts
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
  group by 1, 2, 3, 4, 5, 6, 7, 8, 9
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
  group by 1, 2, 3, 4, 5, 6, 7, 8, 9;

  return jsonb_build_object(
    'status', 'ok',
    'refreshedAt', now_utc
  );
end;
$$;

alter function public.refresh_travel_trade_rollups_v1()
  set statement_timeout = '15min';

select public.refresh_travel_trade_rollups_v1();
