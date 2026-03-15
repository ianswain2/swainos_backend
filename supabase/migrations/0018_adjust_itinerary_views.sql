-- Adjust itinerary trend views to remove closed_count usage
drop materialized view if exists public.mv_itinerary_trends;

create materialized view if not exists public.mv_itinerary_trends as
with created as (
  select
    date_trunc('month', created_at)::date as period_start,
    count(*) as created_count
  from public.itineraries
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
    travel_start.period_start,
    travel_end.period_start
  ) as period_start,
  coalesce(created.created_count, 0) as created_count,
  0::int as closed_count,
  coalesce(travel_start.travel_start_count, 0) as travel_start_count,
  coalesce(travel_end.travel_end_count, 0) as travel_end_count
from created
full outer join travel_start on travel_start.period_start = created.period_start
full outer join travel_end on travel_end.period_start = coalesce(
  created.period_start,
  travel_start.period_start
);

-- Add period_end for status trends to support month ranges
drop materialized view if exists public.mv_itinerary_status_trends;

create materialized view if not exists public.mv_itinerary_status_trends as
select
  date_trunc('month', created_at)::date as period_start,
  (date_trunc('month', created_at)::date + interval '1 month - 1 day')::date as period_end,
  itinerary_status,
  count(*) as itinerary_count
from public.itineraries
where coalesce(lower(itinerary_status), '') not in (
  'duplicate itinerary',
  'test itinerary',
  'sample itinerary',
  'snapshot booking'
)
group by 1, 2, 3;
