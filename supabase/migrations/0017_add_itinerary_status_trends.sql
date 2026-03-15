-- Monthly itinerary status rollups for pipeline analytics
create materialized view if not exists public.mv_itinerary_status_trends as
select
  date_trunc('month', created_at)::date as period_start,
  itinerary_status,
  count(*) as itinerary_count
from public.itineraries
where coalesce(lower(itinerary_status), '') not in (
  'duplicate itinerary',
  'test itinerary',
  'sample itinerary',
  'snapshot booking'
)
group by 1, 2;
