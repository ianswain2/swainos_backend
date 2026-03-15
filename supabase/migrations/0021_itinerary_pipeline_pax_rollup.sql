-- Add passenger rollups to stage-based itinerary pipeline materialized view
drop materialized view if exists public.mv_itinerary_pipeline_stages;

create materialized view if not exists public.mv_itinerary_pipeline_stages as
select
  date_trunc('month', travel_start_date)::date as period_start,
  (date_trunc('month', travel_start_date)::date + interval '1 month - 1 day')::date as period_end,
  case
    when itinerary_status in ('Lost', 'Rejected', 'Cancelled') then 'Lost'
    when itinerary_status = 'Proposal Sent' then 'Quoted'
    when itinerary_status in ('Deposited/Confirming', 'Pre-Departure', 'eDocs Sent') then 'Confirmed'
    when itinerary_status = 'Traveling' then 'Traveling'
    when itinerary_status = 'Traveled' then 'Traveled'
    else 'Unknown'
  end as stage,
  count(*) as itinerary_count,
  sum(coalesce(gross_amount, 0)) as gross_amount,
  sum(coalesce(net_amount, 0)) as net_amount,
  sum(coalesce(pax_count, 0)) as pax_count
from public.itineraries
where travel_start_date is not null
  and coalesce(lower(itinerary_status), '') not in (
    'duplicate itinerary',
    'test itinerary',
    'sample itinerary',
    'snapshot booking'
  )
group by 1, 2, 3;
