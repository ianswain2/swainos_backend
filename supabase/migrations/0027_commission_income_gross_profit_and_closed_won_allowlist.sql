-- Canonical commission-income and status allowlist alignment.
-- New business rules:
-- 1) commission_income_amount is sourced from itineraries.gross_profit
-- 2) Closed-Won statuses are strictly allowlisted:
--    Deposited/Confirming, Amendment in Progress, Pre-Departure, eDocs Sent,
--    Traveling, Traveled, Cancel Fees
-- 3) Any status currently classified as closed_won and not in the allowlist
--    is reclassified to closed_lost.

insert into public.itinerary_status_reference (
  status_value,
  pipeline_category,
  pipeline_bucket,
  definition,
  is_filter_out,
  is_active
)
values (
  'Amendment in Progress',
  'Closed - Won',
  'closed_won',
  'Amendment currently being processed and retained as closed-won for reporting.',
  false,
  true
)
on conflict (status_value) do update set
  pipeline_category = excluded.pipeline_category,
  pipeline_bucket = excluded.pipeline_bucket,
  definition = excluded.definition,
  is_filter_out = excluded.is_filter_out,
  is_active = true,
  updated_at = now();

update public.itinerary_status_reference
set
  pipeline_category = 'Closed - Won',
  pipeline_bucket = 'closed_won',
  is_filter_out = false,
  is_active = true,
  updated_at = now()
where status_value in (
  'Deposited/Confirming',
  'Amendment in Progress',
  'Pre-Departure',
  'eDocs Sent',
  'Traveling',
  'Traveled',
  'Cancel Fees'
);

update public.itinerary_status_reference
set
  pipeline_category = 'Closed - Lost',
  pipeline_bucket = 'closed_lost',
  is_filter_out = false,
  is_active = true,
  updated_at = now()
where pipeline_bucket = 'closed_won'
  and status_value not in (
    'Deposited/Confirming',
    'Amendment in Progress',
    'Pre-Departure',
    'eDocs Sent',
    'Traveling',
    'Traveled',
    'Cancel Fees'
  );

drop materialized view if exists public.mv_itinerary_revenue_monthly;
create materialized view public.mv_itinerary_revenue_monthly as
select
  date_trunc('month', i.travel_end_date)::date as period_start,
  (date_trunc('month', i.travel_end_date)::date + interval '1 month - 1 day')::date as period_end,
  coalesce(sr.pipeline_category, 'unmapped') as pipeline_category,
  coalesce(sr.pipeline_bucket, 'unmapped') as pipeline_bucket,
  count(*) as itinerary_count,
  sum(coalesce(i.pax_count, 0)) as pax_count,
  sum(coalesce(i.gross_amount, 0)) as gross_amount,
  sum(coalesce(i.net_amount, 0)) as net_amount,
  sum(coalesce(i.gross_profit, 0)) as commission_income_amount,
  sum(coalesce(i.gross_amount, 0) - coalesce(i.net_amount, 0)) as margin_amount,
  case
    when sum(coalesce(i.gross_amount, 0)) > 0
      then sum(coalesce(i.gross_amount, 0) - coalesce(i.net_amount, 0)) / sum(coalesce(i.gross_amount, 0))
    else 0
  end as margin_pct,
  sum(coalesce(i.gross_profit, 0)) as gross_profit,
  sum(coalesce(i.cost_amount, 0)) as cost_amount,
  sum(coalesce(i.commission_amount, 0)) as commission_amount,
  sum(coalesce(i.trade_commission_amount, 0)) as trade_commission_amount,
  avg(coalesce(i.gross_amount, 0)) as avg_gross_per_itinerary,
  avg(coalesce(i.net_amount, 0)) as avg_net_per_itinerary,
  avg(coalesce(i.gross_profit, 0)) as avg_commission_income_per_itinerary,
  case
    when sum(coalesce(i.pax_count, 0)) > 0
      then sum(coalesce(i.gross_amount, 0)) / sum(coalesce(i.pax_count, 0))
    else 0
  end as avg_gross_per_pax,
  case
    when sum(coalesce(i.pax_count, 0)) > 0
      then sum(coalesce(i.net_amount, 0)) / sum(coalesce(i.pax_count, 0))
    else 0
  end as avg_net_per_pax,
  case
    when sum(coalesce(i.pax_count, 0)) > 0
      then sum(coalesce(i.gross_profit, 0)) / sum(coalesce(i.pax_count, 0))
    else 0
  end as avg_commission_income_per_pax,
  avg(coalesce(i.number_of_days, 0)) as avg_number_of_days,
  avg(coalesce(i.number_of_nights, 0)) as avg_number_of_nights
from public.itineraries i
left join public.itinerary_status_reference sr
  on sr.status_value = i.itinerary_status
where i.travel_end_date is not null
  and coalesce(sr.is_filter_out, false) = false
group by 1, 2, 3, 4;

create unique index if not exists idx_mv_itinerary_revenue_monthly_unique
  on public.mv_itinerary_revenue_monthly(period_start, pipeline_category, pipeline_bucket);

drop materialized view if exists public.mv_itinerary_revenue_weekly;
create materialized view public.mv_itinerary_revenue_weekly as
select
  date_trunc('week', i.travel_end_date)::date as period_start,
  (date_trunc('week', i.travel_end_date)::date + interval '6 day')::date as period_end,
  coalesce(sr.pipeline_category, 'unmapped') as pipeline_category,
  coalesce(sr.pipeline_bucket, 'unmapped') as pipeline_bucket,
  count(*) as itinerary_count,
  sum(coalesce(i.pax_count, 0)) as pax_count,
  sum(coalesce(i.gross_amount, 0)) as gross_amount,
  sum(coalesce(i.net_amount, 0)) as net_amount,
  sum(coalesce(i.gross_profit, 0)) as commission_income_amount,
  sum(coalesce(i.gross_amount, 0) - coalesce(i.net_amount, 0)) as margin_amount,
  sum(coalesce(i.commission_amount, 0)) as commission_amount,
  sum(coalesce(i.trade_commission_amount, 0)) as trade_commission_amount
from public.itineraries i
left join public.itinerary_status_reference sr
  on sr.status_value = i.itinerary_status
where i.travel_end_date is not null
  and coalesce(sr.is_filter_out, false) = false
group by 1, 2, 3, 4;

create unique index if not exists idx_mv_itinerary_revenue_weekly_unique
  on public.mv_itinerary_revenue_weekly(period_start, pipeline_category, pipeline_bucket);

drop materialized view if exists public.mv_itinerary_consortia_monthly;
create materialized view public.mv_itinerary_consortia_monthly as
select
  date_trunc('month', i.travel_start_date)::date as period_start,
  (date_trunc('month', i.travel_start_date)::date + interval '1 month - 1 day')::date as period_end,
  coalesce(nullif(trim(i.consortia), ''), 'Unassigned') as consortia,
  count(*) as itinerary_count,
  sum(coalesce(i.pax_count, 0)) as pax_count,
  sum(coalesce(i.gross_amount, 0)) as gross_amount,
  sum(coalesce(i.net_amount, 0)) as net_amount,
  sum(coalesce(i.gross_profit, 0)) as commission_income_amount,
  sum(coalesce(i.gross_amount, 0) - coalesce(i.net_amount, 0)) as margin_amount
from public.itineraries i
left join public.itinerary_status_reference sr
  on sr.status_value = i.itinerary_status
where i.travel_start_date is not null
  and coalesce(sr.is_filter_out, false) = false
group by 1, 2, 3;

create unique index if not exists idx_mv_itinerary_consortia_monthly_unique
  on public.mv_itinerary_consortia_monthly(period_start, consortia);

drop materialized view if exists public.mv_itinerary_trade_agency_monthly;
create materialized view public.mv_itinerary_trade_agency_monthly as
select
  date_trunc('month', i.travel_start_date)::date as period_start,
  (date_trunc('month', i.travel_start_date)::date + interval '1 month - 1 day')::date as period_end,
  coalesce(a_contact.id::text, a_itinerary.id::text, a_external.id::text, 'unassigned') as agency_id,
  coalesce(
    nullif(trim(a_contact.agency_name), ''),
    nullif(trim(a_itinerary.agency_name), ''),
    nullif(trim(a_external.agency_name), ''),
    'Unassigned Agency'
  ) as agency_name,
  count(*) as itinerary_count,
  sum(coalesce(i.pax_count, 0)) as pax_count,
  sum(coalesce(i.gross_amount, 0)) as gross_amount,
  sum(coalesce(i.net_amount, 0)) as net_amount,
  sum(coalesce(i.gross_profit, 0)) as commission_income_amount,
  sum(coalesce(i.gross_amount, 0) - coalesce(i.net_amount, 0)) as margin_amount,
  sum(coalesce(i.trade_commission_amount, 0)) as trade_commission_amount
from public.itineraries i
left join public.contacts c
  on c.id = i.primary_contact_id
left join public.agencies a_contact
  on a_contact.id = c.agency_id
left join public.agencies a_itinerary
  on a_itinerary.id = i.agency_id
left join public.agencies a_external
  on a_external.external_id = i.agency_external_id
left join public.itinerary_status_reference sr
  on sr.status_value = i.itinerary_status
where i.travel_start_date is not null
  and coalesce(sr.is_filter_out, false) = false
  and (
    lower(coalesce(i.primary_contact_type, '')) in ('trade', 'agent')
    or coalesce(i.trade_commission_amount, 0) > 0
  )
group by 1, 2, 3, 4;

create unique index if not exists idx_mv_itinerary_trade_agency_monthly_unique
  on public.mv_itinerary_trade_agency_monthly(period_start, agency_id);

drop materialized view if exists public.mv_itinerary_consortia_actuals_monthly;
create materialized view public.mv_itinerary_consortia_actuals_monthly as
select
  date_trunc('month', i.travel_end_date)::date as period_start,
  (date_trunc('month', i.travel_end_date)::date + interval '1 month - 1 day')::date as period_end,
  coalesce(nullif(trim(i.consortia), ''), 'Unassigned') as consortia,
  count(*) as itinerary_count,
  sum(coalesce(i.pax_count, 0)) as pax_count,
  sum(coalesce(i.gross_amount, 0)) as gross_amount,
  sum(coalesce(i.net_amount, 0)) as net_amount,
  sum(coalesce(i.gross_profit, 0)) as commission_income_amount,
  sum(coalesce(i.gross_amount, 0) - coalesce(i.net_amount, 0)) as margin_amount
from public.itineraries i
left join public.itinerary_status_reference sr
  on sr.status_value = i.itinerary_status
where i.travel_end_date is not null
  and coalesce(sr.is_filter_out, false) = false
  and coalesce(sr.pipeline_bucket, 'open') = 'closed_won'
group by 1, 2, 3;

create unique index if not exists idx_mv_itinerary_consortia_actuals_monthly_unique
  on public.mv_itinerary_consortia_actuals_monthly(period_start, consortia);

drop materialized view if exists public.mv_itinerary_trade_agency_actuals_monthly;
create materialized view public.mv_itinerary_trade_agency_actuals_monthly as
select
  date_trunc('month', i.travel_end_date)::date as period_start,
  (date_trunc('month', i.travel_end_date)::date + interval '1 month - 1 day')::date as period_end,
  coalesce(a_contact.id::text, a_itinerary.id::text, a_external.id::text, 'unassigned') as agency_id,
  coalesce(
    nullif(trim(a_contact.agency_name), ''),
    nullif(trim(a_itinerary.agency_name), ''),
    nullif(trim(a_external.agency_name), ''),
    'Unassigned Agency'
  ) as agency_name,
  count(*) as itinerary_count,
  sum(coalesce(i.pax_count, 0)) as pax_count,
  sum(coalesce(i.gross_amount, 0)) as gross_amount,
  sum(coalesce(i.net_amount, 0)) as net_amount,
  sum(coalesce(i.gross_profit, 0)) as commission_income_amount,
  sum(coalesce(i.gross_amount, 0) - coalesce(i.net_amount, 0)) as margin_amount,
  sum(coalesce(i.trade_commission_amount, 0)) as trade_commission_amount
from public.itineraries i
left join public.contacts c
  on c.id = i.primary_contact_id
left join public.agencies a_contact
  on a_contact.id = c.agency_id
left join public.agencies a_itinerary
  on a_itinerary.id = i.agency_id
left join public.agencies a_external
  on a_external.external_id = i.agency_external_id
left join public.itinerary_status_reference sr
  on sr.status_value = i.itinerary_status
where i.travel_end_date is not null
  and coalesce(sr.is_filter_out, false) = false
  and coalesce(sr.pipeline_bucket, 'open') = 'closed_won'
  and (
    lower(coalesce(i.primary_contact_type, '')) in ('trade', 'agent')
    or coalesce(i.trade_commission_amount, 0) > 0
  )
group by 1, 2, 3, 4;

create unique index if not exists idx_mv_itinerary_trade_agency_actuals_monthly_unique
  on public.mv_itinerary_trade_agency_actuals_monthly(period_start, agency_id);

drop materialized view if exists public.mv_itinerary_pipeline_stages;
create materialized view public.mv_itinerary_pipeline_stages as
select
  date_trunc('month', i.travel_start_date)::date as period_start,
  (date_trunc('month', i.travel_start_date)::date + interval '1 month - 1 day')::date as period_end,
  case
    when coalesce(sr.pipeline_bucket, 'open') = 'closed_lost' then 'Lost'
    when i.itinerary_status = 'Proposal Sent' then 'Quoted'
    when i.itinerary_status in ('Deposited/Confirming', 'Amendment in Progress', 'Pre-Departure', 'eDocs Sent') then 'Confirmed'
    when i.itinerary_status = 'Traveling' then 'Traveling'
    when i.itinerary_status = 'Traveled' then 'Traveled'
    else 'Unknown'
  end as stage,
  count(*) as itinerary_count,
  sum(coalesce(i.gross_amount, 0)) as gross_amount,
  sum(coalesce(i.net_amount, 0)) as net_amount,
  sum(coalesce(i.pax_count, 0)) as pax_count
from public.itineraries i
left join public.itinerary_status_reference sr
  on sr.status_value = i.itinerary_status
where i.travel_start_date is not null
  and coalesce(sr.is_filter_out, false) = false
group by 1, 2, 3;
