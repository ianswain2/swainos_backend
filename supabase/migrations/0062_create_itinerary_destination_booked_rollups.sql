-- Forward-only migration:
-- Create destination booked rollups from itinerary_items using item service start dates
-- and confirmed itineraries for Sales Destination analytics.

drop materialized view if exists public.mv_itinerary_destination_booked_monthly;

create materialized view public.mv_itinerary_destination_booked_monthly as
with scoped_items as (
  select
    date_trunc('month', ii.service_start_date)::date as period_start,
    (date_trunc('month', ii.service_start_date)::date + interval '1 month - 1 day')::date as period_end,
    coalesce(nullif(trim(ii.location_country), ''), 'Unknown') as location_country,
    coalesce(nullif(trim(ii.location_city), ''), 'Unspecified') as location_city,
    ii.itinerary_id,
    ii.quantity,
    ii.total_cost,
    ii.total_price,
    ii.gross_margin,
    ii.profit_margin_percent,
    coalesce(ii.is_cancelled, false) as is_cancelled,
    coalesce(ii.is_deleted, false) as is_deleted
  from public.itinerary_items ii
  inner join public.itineraries i
    on i.id = ii.itinerary_id
  where ii.service_start_date is not null
    and lower(trim(coalesce(i.itinerary_status, ''))) = 'confirmed'
),
rolled as (
  select
    period_start,
    period_end,
    location_country,
    location_city,
    count(*)::bigint as total_item_count,
    count(*) filter (where is_cancelled) ::bigint as cancelled_item_count,
    count(*) filter (where is_deleted) ::bigint as deleted_item_count,
    count(*) filter (where not is_cancelled and not is_deleted) ::bigint as active_item_count,
    count(distinct itinerary_id) filter (where not is_cancelled and not is_deleted) ::bigint as booked_itinerary_count,
    coalesce(sum(coalesce(quantity, 0)) filter (where not is_cancelled and not is_deleted), 0) as booked_quantity,
    coalesce(sum(coalesce(total_cost, 0)) filter (where not is_cancelled and not is_deleted), 0) as booked_total_cost,
    coalesce(sum(coalesce(total_price, 0)) filter (where not is_cancelled and not is_deleted), 0) as booked_total_price,
    coalesce(
      sum(
        coalesce(gross_margin, coalesce(total_price, 0) - coalesce(total_cost, 0))
      ) filter (where not is_cancelled and not is_deleted),
      0
    ) as booked_gross_margin,
    coalesce(
      avg(profit_margin_percent) filter (where not is_cancelled and not is_deleted),
      0
    ) as booked_avg_profit_margin_percent
  from scoped_items
  group by 1, 2, 3, 4
)
select
  period_start,
  period_end,
  location_country,
  location_city,
  total_item_count,
  cancelled_item_count,
  deleted_item_count,
  active_item_count,
  booked_itinerary_count,
  booked_quantity,
  booked_total_cost,
  booked_total_price,
  booked_gross_margin,
  booked_avg_profit_margin_percent,
  case
    when booked_total_price > 0
      then booked_gross_margin / booked_total_price
    else 0
  end as booked_margin_pct
from rolled;

create unique index if not exists idx_mv_itinerary_destination_booked_monthly_unique
  on public.mv_itinerary_destination_booked_monthly(period_start, location_country, location_city);

create index if not exists idx_mv_itinerary_destination_booked_monthly_country_period
  on public.mv_itinerary_destination_booked_monthly(location_country, period_start);

create index if not exists idx_mv_itinerary_destination_booked_monthly_city_period
  on public.mv_itinerary_destination_booked_monthly(location_city, period_start);

create or replace function public.refresh_consultant_ai_rollups_v1()
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
begin
  refresh materialized view public.mv_itinerary_revenue_monthly;
  refresh materialized view public.mv_itinerary_revenue_weekly;
  refresh materialized view public.mv_itinerary_lead_flow_monthly;
  refresh materialized view public.mv_itinerary_destination_booked_monthly;
  refresh materialized view public.mv_travel_consultant_profile_monthly;
  refresh materialized view public.mv_travel_consultant_funnel_monthly;
  refresh materialized view public.mv_travel_consultant_leaderboard_monthly;
  refresh materialized view public.mv_travel_consultant_compensation_monthly;

  refresh materialized view public.ai_context_travel_consultant_v1;
  refresh materialized view public.ai_context_itinerary_health_v1;
  refresh materialized view public.ai_context_command_center_v1;
  refresh materialized view public.ai_context_consultant_benchmarks_v1;
  refresh materialized view public.ai_context_company_metrics_v1;

  return jsonb_build_object(
    'status', 'ok',
    'refreshedAt', now(),
    'views', jsonb_build_array(
      'mv_itinerary_revenue_monthly',
      'mv_itinerary_revenue_weekly',
      'mv_itinerary_lead_flow_monthly',
      'mv_itinerary_destination_booked_monthly',
      'mv_travel_consultant_profile_monthly',
      'mv_travel_consultant_funnel_monthly',
      'mv_travel_consultant_leaderboard_monthly',
      'mv_travel_consultant_compensation_monthly',
      'ai_context_travel_consultant_v1',
      'ai_context_itinerary_health_v1',
      'ai_context_command_center_v1',
      'ai_context_consultant_benchmarks_v1',
      'ai_context_company_metrics_v1'
    )
  );
end;
$$;

revoke all on function public.refresh_consultant_ai_rollups_v1() from public;
grant execute on function public.refresh_consultant_ai_rollups_v1() to service_role;
