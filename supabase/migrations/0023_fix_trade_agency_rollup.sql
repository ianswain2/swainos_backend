-- Fix trade agency rollup classification and agency resolution.
-- Includes Agent contact type and falls back to itinerary agency links.

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
