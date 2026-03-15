drop materialized view if exists public.mv_itinerary_lead_flow_monthly;

create materialized view public.mv_itinerary_lead_flow_monthly as
select
  date_trunc('month', i.created_at)::date as period_start,
  count(*)::int as created_count,
  sum(
    case
      when coalesce(lower(trim(s.pipeline_bucket)), '') = 'closed_won' then 1
      else 0
    end
  )::int as closed_won_count,
  sum(
    case
      when coalesce(lower(trim(s.pipeline_bucket)), '') = 'closed_lost' then 1
      else 0
    end
  )::int as closed_lost_count
from public.itineraries i
left join public.itinerary_status_reference s
  on lower(trim(coalesce(s.status_value, ''))) = lower(trim(coalesce(i.itinerary_status, '')))
where i.created_at is not null
  and coalesce(lower(trim(i.itinerary_status)), '') not in (
    'duplicate itinerary',
    'test itinerary',
    'sample itinerary',
    'snapshot booking'
  )
group by 1;

create index if not exists idx_mv_itinerary_lead_flow_monthly_period_start
  on public.mv_itinerary_lead_flow_monthly(period_start);
