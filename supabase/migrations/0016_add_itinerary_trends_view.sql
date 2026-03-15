-- Aggregate itinerary lifecycle trends for dashboards
create materialized view if not exists public.mv_itinerary_trends as
with created as (
  select
    date_trunc('month', created_at)::date as period_start,
    count(*) as created_count
  from public.itineraries
  group by 1
),
closed as (
  select
    date_trunc('month', updated_at)::date as period_start,
    count(*) as closed_count
  from public.itineraries
  where itinerary_status in (
    'Lost',
    'Rejected',
    'Cancelled',
    'Cancel Fees',
    'Amendment Merged',
    'Amendment Rejected'
  )
  group by 1
),
travel_start as (
  select
    date_trunc('month', travel_start_date)::date as period_start,
    count(*) as travel_start_count
  from public.itineraries
  where travel_start_date is not null
  group by 1
),
travel_end as (
  select
    date_trunc('month', travel_end_date)::date as period_start,
    count(*) as travel_end_count
  from public.itineraries
  where travel_end_date is not null
  group by 1
)
select
  coalesce(
    created.period_start,
    closed.period_start,
    travel_start.period_start,
    travel_end.period_start
  ) as period_start,
  coalesce(created.created_count, 0) as created_count,
  coalesce(closed.closed_count, 0) as closed_count,
  coalesce(travel_start.travel_start_count, 0) as travel_start_count,
  coalesce(travel_end.travel_end_count, 0) as travel_end_count
from created
full outer join closed on closed.period_start = created.period_start
full outer join travel_start on travel_start.period_start = coalesce(
  created.period_start,
  closed.period_start
)
full outer join travel_end on travel_end.period_start = coalesce(
  created.period_start,
  closed.period_start,
  travel_start.period_start
);
