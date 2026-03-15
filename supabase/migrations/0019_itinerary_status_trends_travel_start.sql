-- Align pipeline trend periods to travel_start_date
drop materialized view if exists public.mv_itinerary_status_trends;

create materialized view if not exists public.mv_itinerary_status_trends as
select
  date_trunc('month', travel_start_date)::date as period_start,
  (date_trunc('month', travel_start_date)::date + interval '1 month - 1 day')::date as period_end,
  itinerary_status,
  count(*) as itinerary_count
from public.itineraries
where travel_start_date is not null
  and coalesce(lower(itinerary_status), '') not in (
    'duplicate itinerary',
    'test itinerary',
    'sample itinerary',
    'snapshot booking'
  )
group by 1, 2, 3;
