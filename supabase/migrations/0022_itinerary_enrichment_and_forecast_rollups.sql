-- Enrich itineraries for forecasting and add analytics rollups

alter table public.itineraries
  add column if not exists close_date date,
  add column if not exists trade_commission_due_date date,
  add column if not exists trade_commission_status text,
  add column if not exists consortia text,
  add column if not exists final_payment_date date,
  add column if not exists gross_profit numeric,
  add column if not exists cost_amount numeric,
  add column if not exists number_of_days integer,
  add column if not exists number_of_nights integer,
  add column if not exists trade_commission_amount numeric,
  add column if not exists outstanding_balance numeric,
  add column if not exists agency_external_id text,
  add column if not exists primary_contact_external_id text,
  add column if not exists owner_external_id text,
  add column if not exists lost_date date,
  add column if not exists lost_comments text;

create index if not exists idx_itineraries_close_date on public.itineraries(close_date);
create index if not exists idx_itineraries_final_payment_date on public.itineraries(final_payment_date);
create index if not exists idx_itineraries_trade_commission_due_date on public.itineraries(trade_commission_due_date);
create index if not exists idx_itineraries_lost_date on public.itineraries(lost_date);
create index if not exists idx_itineraries_consortia on public.itineraries(consortia);
create index if not exists idx_itineraries_agency_external_id on public.itineraries(agency_external_id);
create index if not exists idx_itineraries_primary_contact_external_id on public.itineraries(primary_contact_external_id);
create index if not exists idx_itineraries_owner_external_id on public.itineraries(owner_external_id);

create table if not exists public.itinerary_status_reference (
  status_value text primary key,
  pipeline_category text not null,
  pipeline_bucket text not null,
  definition text,
  is_filter_out boolean not null default false,
  is_active boolean not null default true,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

alter table public.itinerary_status_reference
  add column if not exists pipeline_bucket text;

insert into public.itinerary_status_reference (
  status_value,
  pipeline_category,
  pipeline_bucket,
  definition,
  is_filter_out
)
values
  ('Lost', 'Closed - Lost', 'closed_lost', 'Booking opportunity was lost — client did not proceed.', false),
  ('Traveled', 'Closed - Won', 'closed_won', 'Trip has been completed and passengers have returned.', false),
  ('Cancelled', 'Closed - Won', 'closed_won', 'Itinerary was booked but cancelled before travel.', false),
  ('Duplicate Itinerary', 'Filter Out', 'filter_out', 'Duplicate itinerary record.', true),
  ('Test Itinerary', 'Filter Out', 'filter_out', 'Test itinerary record.', true),
  ('Rejected', 'Lost', 'closed_lost', 'Itinerary was rejected.', false),
  ('Sample Itinerary', 'Filter Out', 'filter_out', 'Sample itinerary record.', true),
  ('Pre-Departure', 'Closed - Won', 'closed_won', 'Client has confirmed and is preparing to travel.', false),
  ('Proposal Sent', 'Open', 'open', 'Proposal/quote sent and awaiting response.', false),
  ('Cancel Fees', 'Closed - Won', 'closed_won', 'Cancellation fees captured as settled outcome.', false),
  ('Deposited/Confirming', 'Closed - Won', 'closed_won', 'Deposit paid and itinerary confirming.', false),
  ('Holding', 'Holding', 'holding', 'Itinerary is on hold and awaiting movement.', false),
  ('Traveling', 'Closed - Won', 'closed_won', 'Passengers are currently traveling.', false),
  ('eDocs Sent', 'Closed - Won', 'closed_won', 'Travel documents issued to the client.', false),
  ('Amendment Merged', 'Closed - Won', 'closed_won', 'Amendment merged into the active itinerary.', false),
  ('Snapshot Booking', 'Filter Out', 'filter_out', 'Snapshot/system booking status.', true),
  ('Amendment Rejected', 'Closed - Lost', 'closed_lost', 'Amendment was rejected.', false),
  ('Assigned', 'Open', 'open', 'Assigned to consultant and actively worked on.', false),
  ('Invoiced', 'Closed - Won', 'closed_won', 'Final invoice issued to client/agency.', false),
  ('Booked', 'Closed - Won', 'closed_won', 'Itinerary booked with suppliers.', false),
  ('Confirmed', 'Closed - Won', 'closed_won', 'All services confirmed for itinerary.', false),
  ('Closed', 'Closed - Lost', 'closed_lost', 'Closed after settlement and processing.', false),
  ('Draft', 'Open', 'open', 'Draft itinerary not yet shared.', false),
  ('Pending', 'Open', 'open', 'Awaiting action or approval.', false)
on conflict (status_value) do update set
  pipeline_category = excluded.pipeline_category,
  pipeline_bucket = excluded.pipeline_bucket,
  definition = excluded.definition,
  is_filter_out = excluded.is_filter_out,
  is_active = true,
  updated_at = now();

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

drop materialized view if exists public.mv_itinerary_deposit_monthly;
create materialized view public.mv_itinerary_deposit_monthly as
select
  date_trunc('month', i.close_date)::date as period_start,
  (date_trunc('month', i.close_date)::date + interval '1 month - 1 day')::date as period_end,
  count(*) as closed_itinerary_count,
  sum(coalesce(i.gross_amount, 0)) as closed_gross_amount,
  sum(coalesce(i.deposit_received, 0)) as deposit_received_amount,
  sum(coalesce(i.gross_amount, 0) * 0.25) as target_deposit_amount,
  sum(coalesce(i.deposit_received, 0)) - sum(coalesce(i.gross_amount, 0) * 0.25) as deposit_gap_amount,
  case
    when sum(coalesce(i.gross_amount, 0) * 0.25) > 0
      then sum(coalesce(i.deposit_received, 0)) / sum(coalesce(i.gross_amount, 0) * 0.25)
    else 0
  end as deposit_coverage_ratio
from public.itineraries i
left join public.itinerary_status_reference sr
  on sr.status_value = i.itinerary_status
where i.close_date is not null
  and coalesce(sr.is_filter_out, false) = false
  and coalesce(sr.pipeline_bucket, 'open') = 'closed_won'
group by 1, 2;

create unique index if not exists idx_mv_itinerary_deposit_monthly_unique
  on public.mv_itinerary_deposit_monthly(period_start);

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
  coalesce(a.id::text, 'unassigned') as agency_id,
  coalesce(nullif(trim(a.agency_name), ''), 'Unassigned Agency') as agency_name,
  count(*) as itinerary_count,
  sum(coalesce(i.pax_count, 0)) as pax_count,
  sum(coalesce(i.gross_amount, 0)) as gross_amount,
  sum(coalesce(i.net_amount, 0)) as net_amount,
  sum(coalesce(i.trade_commission_amount, 0)) as trade_commission_amount
from public.itineraries i
left join public.contacts c
  on c.id = i.primary_contact_id
left join public.agencies a
  on a.id = c.agency_id
left join public.itinerary_status_reference sr
  on sr.status_value = i.itinerary_status
where i.travel_start_date is not null
  and coalesce(sr.is_filter_out, false) = false
  and lower(coalesce(i.primary_contact_type, '')) = 'trade'
group by 1, 2, 3, 4;

create unique index if not exists idx_mv_itinerary_trade_agency_monthly_unique
  on public.mv_itinerary_trade_agency_monthly(period_start, agency_id);
