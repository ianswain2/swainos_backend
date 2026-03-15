-- Forward-only migration:
-- Shift itinerary revenue/actuals rollups from travel_end_date to travel_start_date
-- to align dashboard "booked" analytics with Travel Start Date reporting.

drop materialized view if exists public.ai_context_company_metrics_v1;
drop materialized view if exists public.ai_context_command_center_v1;
drop materialized view if exists public.ai_context_itinerary_health_v1;
drop materialized view if exists public.mv_itinerary_lead_flow_monthly;

drop materialized view if exists public.mv_itinerary_trade_agency_actuals_monthly;
drop materialized view if exists public.mv_itinerary_consortia_actuals_monthly;
drop materialized view if exists public.mv_itinerary_revenue_weekly;
drop materialized view if exists public.mv_itinerary_revenue_monthly;

create materialized view public.mv_itinerary_revenue_monthly as
select
  date_trunc('month', i.travel_start_date)::date as period_start,
  (date_trunc('month', i.travel_start_date)::date + interval '1 month - 1 day')::date as period_end,
  coalesce(sr.pipeline_category, 'unmapped') as pipeline_category,
  coalesce(sr.pipeline_bucket, 'unmapped') as pipeline_bucket,
  count(*) as itinerary_count,
  sum(coalesce(i.pax_count, 0)) as pax_count,
  sum(coalesce(i.gross_amount, 0)) as gross_amount,
  sum(coalesce(i.net_amount, 0)) as net_amount,
  sum(coalesce(i.gross_profit, 0)) as gross_profit_amount,
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
  avg(coalesce(i.gross_profit, 0)) as avg_gross_profit_per_itinerary,
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
  end as avg_gross_profit_per_pax,
  avg(coalesce(i.number_of_days, 0)) as avg_number_of_days,
  avg(coalesce(i.number_of_nights, 0)) as avg_number_of_nights
from public.itineraries i
left join public.itinerary_status_reference sr
  on sr.status_value = i.itinerary_status
where i.travel_start_date is not null
  and coalesce(sr.is_filter_out, false) = false
group by 1, 2, 3, 4;

create unique index if not exists idx_mv_itinerary_revenue_monthly_unique
  on public.mv_itinerary_revenue_monthly(period_start, pipeline_category, pipeline_bucket);

create materialized view public.mv_itinerary_revenue_weekly as
select
  date_trunc('week', i.travel_start_date)::date as period_start,
  (date_trunc('week', i.travel_start_date)::date + interval '6 day')::date as period_end,
  coalesce(sr.pipeline_category, 'unmapped') as pipeline_category,
  coalesce(sr.pipeline_bucket, 'unmapped') as pipeline_bucket,
  count(*) as itinerary_count,
  sum(coalesce(i.pax_count, 0)) as pax_count,
  sum(coalesce(i.gross_amount, 0)) as gross_amount,
  sum(coalesce(i.net_amount, 0)) as net_amount,
  sum(coalesce(i.gross_profit, 0)) as gross_profit_amount,
  sum(coalesce(i.gross_amount, 0) - coalesce(i.net_amount, 0)) as margin_amount,
  sum(coalesce(i.commission_amount, 0)) as commission_amount,
  sum(coalesce(i.trade_commission_amount, 0)) as trade_commission_amount
from public.itineraries i
left join public.itinerary_status_reference sr
  on sr.status_value = i.itinerary_status
where i.travel_start_date is not null
  and coalesce(sr.is_filter_out, false) = false
group by 1, 2, 3, 4;

create unique index if not exists idx_mv_itinerary_revenue_weekly_unique
  on public.mv_itinerary_revenue_weekly(period_start, pipeline_category, pipeline_bucket);

create materialized view public.mv_itinerary_consortia_actuals_monthly as
select
  date_trunc('month', i.travel_start_date)::date as period_start,
  (date_trunc('month', i.travel_start_date)::date + interval '1 month - 1 day')::date as period_end,
  coalesce(nullif(trim(i.consortia), ''), 'Unassigned') as consortia,
  count(*) as itinerary_count,
  sum(coalesce(i.pax_count, 0)) as pax_count,
  sum(coalesce(i.gross_amount, 0)) as gross_amount,
  sum(coalesce(i.net_amount, 0)) as net_amount,
  sum(coalesce(i.gross_profit, 0)) as gross_profit_amount,
  sum(coalesce(i.gross_amount, 0) - coalesce(i.net_amount, 0)) as margin_amount
from public.itineraries i
left join public.itinerary_status_reference sr
  on sr.status_value = i.itinerary_status
where i.travel_start_date is not null
  and coalesce(sr.is_filter_out, false) = false
  and coalesce(sr.pipeline_bucket, 'open') = 'closed_won'
group by 1, 2, 3;

create unique index if not exists idx_mv_itinerary_consortia_actuals_monthly_unique
  on public.mv_itinerary_consortia_actuals_monthly(period_start, consortia);

create materialized view public.mv_itinerary_trade_agency_actuals_monthly as
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
  sum(coalesce(i.gross_profit, 0)) as gross_profit_amount,
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
  and coalesce(sr.pipeline_bucket, 'open') = 'closed_won'
  and (
    lower(coalesce(i.primary_contact_type, '')) in ('trade', 'agent')
    or coalesce(i.trade_commission_amount, 0) > 0
  )
group by 1, 2, 3, 4;

create unique index if not exists idx_mv_itinerary_trade_agency_actuals_monthly_unique
  on public.mv_itinerary_trade_agency_actuals_monthly(period_start, agency_id);

create materialized view public.mv_itinerary_lead_flow_monthly as
with status_map as (
  select
    lower(trim(coalesce(status_value, ''))) as status_key,
    lower(trim(coalesce(pipeline_bucket, ''))) as pipeline_bucket,
    coalesce(is_filter_out, false) as is_filter_out
  from public.itinerary_status_reference
),
created_by_month as (
  select
    date_trunc('month', i.created_at)::date as period_start,
    count(*)::int as created_count
  from public.itineraries i
  left join status_map sm
    on sm.status_key = lower(trim(coalesce(i.itinerary_status, '')))
  where i.created_at is not null
    and coalesce(sm.is_filter_out, false) = false
    and coalesce(lower(trim(i.itinerary_status)), '') not in (
      'duplicate itinerary',
      'test itinerary',
      'sample itinerary',
      'snapshot booking'
    )
  group by 1
),
closed_won_by_month as (
  select
    date_trunc('month', i.close_date)::date as period_start,
    count(*)::int as closed_won_count
  from public.itineraries i
  left join status_map sm
    on sm.status_key = lower(trim(coalesce(i.itinerary_status, '')))
  where i.close_date is not null
    and sm.pipeline_bucket = 'closed_won'
    and coalesce(sm.is_filter_out, false) = false
    and coalesce(lower(trim(i.itinerary_status)), '') not in (
      'duplicate itinerary',
      'test itinerary',
      'sample itinerary',
      'snapshot booking'
    )
  group by 1
),
closed_lost_by_month as (
  select
    date_trunc('month', coalesce(i.lost_date, i.close_date))::date as period_start,
    count(*)::int as closed_lost_count
  from public.itineraries i
  left join status_map sm
    on sm.status_key = lower(trim(coalesce(i.itinerary_status, '')))
  where coalesce(i.lost_date, i.close_date) is not null
    and sm.pipeline_bucket = 'closed_lost'
    and coalesce(sm.is_filter_out, false) = false
    and coalesce(lower(trim(i.itinerary_status)), '') not in (
      'duplicate itinerary',
      'test itinerary',
      'sample itinerary',
      'snapshot booking'
    )
  group by 1
),
all_months as (
  select period_start from created_by_month
  union
  select period_start from closed_won_by_month
  union
  select period_start from closed_lost_by_month
)
select
  m.period_start,
  coalesce(c.created_count, 0)::int as created_count,
  coalesce(cw.closed_won_count, 0)::int as closed_won_count,
  coalesce(cl.closed_lost_count, 0)::int as closed_lost_count
from all_months m
left join created_by_month c
  on c.period_start = m.period_start
left join closed_won_by_month cw
  on cw.period_start = m.period_start
left join closed_lost_by_month cl
  on cl.period_start = m.period_start
order by m.period_start asc;

create unique index if not exists idx_mv_itinerary_lead_flow_monthly_period_start
  on public.mv_itinerary_lead_flow_monthly(period_start);

create materialized view public.ai_context_itinerary_health_v1 as
with lead_window as (
  select
    period_start,
    coalesce(created_count, 0) as created_count,
    coalesce(closed_won_count, 0) as closed_won_count,
    coalesce(closed_lost_count, 0) as closed_lost_count
  from public.mv_itinerary_lead_flow_monthly
  where period_start >= (date_trunc('month', current_date)::date - interval '11 months')::date
),
revenue_window as (
  select
    period_start,
    sum(coalesce(gross_profit_amount, 0)) filter (where pipeline_bucket = 'closed_won') as closed_won_gross_profit_amount
  from public.mv_itinerary_revenue_monthly
  where period_start >= (date_trunc('month', current_date)::date - interval '11 months')::date
  group by period_start
),
deposit_window as (
  select
    period_start,
    coalesce(deposit_coverage_ratio, 0) as deposit_coverage_ratio,
    coalesce(deposit_gap_amount, 0) as deposit_gap_amount
  from public.mv_itinerary_deposit_monthly
  where period_start >= (date_trunc('month', current_date)::date - interval '11 months')::date
)
select
  lw.period_start,
  coalesce(lw.created_count, 0) as created_count,
  coalesce(lw.closed_won_count, 0) as closed_won_count,
  coalesce(lw.closed_lost_count, 0) as closed_lost_count,
  case
    when coalesce(lw.created_count, 0) > 0 then coalesce(lw.closed_won_count, 0)::numeric / lw.created_count
    else 0
  end as conversion_rate,
  coalesce(rw.closed_won_gross_profit_amount, 0) as closed_won_gross_profit_amount,
  coalesce(dw.deposit_coverage_ratio, 0) as deposit_coverage_ratio,
  coalesce(dw.deposit_gap_amount, 0) as deposit_gap_amount
from lead_window lw
left join revenue_window rw
  on rw.period_start = lw.period_start
left join deposit_window dw
  on dw.period_start = lw.period_start;

create unique index if not exists idx_ai_context_itinerary_health_v1_unique
  on public.ai_context_itinerary_health_v1(period_start);

create materialized view public.ai_context_command_center_v1 as
with cash_window as (
  select
    sum(case when forecast_date <= current_date + interval '30 days' then coalesce(net_cash_flow, 0) else 0 end) as net_cash_flow_30d,
    sum(case when forecast_date <= current_date + interval '60 days' then coalesce(net_cash_flow, 0) else 0 end) as net_cash_flow_60d,
    sum(case when forecast_date <= current_date + interval '90 days' then coalesce(net_cash_flow, 0) else 0 end) as net_cash_flow_90d
  from public.mv_cash_flow_forecast
),
deposit_window as (
  select
    avg(coalesce(deposit_coverage_ratio, 0)) as avg_deposit_coverage_ratio_6m,
    sum(coalesce(deposit_gap_amount, 0)) as total_deposit_gap_amount_6m
  from public.mv_itinerary_deposit_monthly
  where period_start >= (date_trunc('month', current_date)::date - interval '5 months')::date
),
lead_window as (
  select
    sum(coalesce(created_count, 0)) as created_count_12m,
    sum(coalesce(closed_won_count, 0)) as closed_won_count_12m,
    sum(coalesce(closed_lost_count, 0)) as closed_lost_count_12m
  from public.mv_itinerary_lead_flow_monthly
  where period_start >= (date_trunc('month', current_date)::date - interval '11 months')::date
)
select
  current_date as as_of_date,
  coalesce(c.net_cash_flow_30d, 0) as net_cash_flow_30d,
  coalesce(c.net_cash_flow_60d, 0) as net_cash_flow_60d,
  coalesce(c.net_cash_flow_90d, 0) as net_cash_flow_90d,
  coalesce(d.avg_deposit_coverage_ratio_6m, 0) as avg_deposit_coverage_ratio_6m,
  coalesce(d.total_deposit_gap_amount_6m, 0) as total_deposit_gap_amount_6m,
  coalesce(l.created_count_12m, 0) as lead_created_count_12m,
  coalesce(l.closed_won_count_12m, 0) as lead_closed_won_count_12m,
  coalesce(l.closed_lost_count_12m, 0) as lead_closed_lost_count_12m,
  case
    when coalesce(l.created_count_12m, 0) > 0 then coalesce(l.closed_won_count_12m, 0)::numeric / l.created_count_12m
    else 0
  end as lead_conversion_rate_12m
from cash_window c
cross join deposit_window d
cross join lead_window l;

create unique index if not exists idx_ai_context_command_center_v1_unique
  on public.ai_context_command_center_v1(as_of_date);

create materialized view public.ai_context_company_metrics_v1 as
with benchmark as (
  select *
  from public.ai_context_consultant_benchmarks_v1
),
command_center as (
  select *
  from public.ai_context_command_center_v1
  order by as_of_date desc
  limit 1
)
select
  current_date as as_of_date,
  b.period_type,
  b.domain,
  b.consultant_count,
  b.total_itinerary_count,
  b.total_lead_count,
  b.total_closed_won_count,
  b.total_closed_lost_count,
  b.total_booked_revenue_amount,
  b.total_gross_profit_amount,
  b.total_margin_amount,
  b.weighted_margin_pct,
  b.weighted_conversion_rate,
  b.weighted_close_rate,
  b.team_avg_conversion_rate,
  b.team_avg_margin_pct,
  b.team_avg_close_rate,
  b.team_avg_speed_to_book_days,
  b.team_top_conversion_rate,
  b.team_top_margin_pct,
  b.team_top_close_rate,
  b.team_low_conversion_rate,
  b.team_low_margin_pct,
  b.team_low_close_rate,
  b.team_p20_conversion_rate,
  b.team_p20_margin_pct,
  b.team_p20_close_rate,
  b.team_median_conversion_rate,
  b.team_median_margin_pct,
  b.team_median_close_rate,
  b.team_p80_conversion_rate,
  b.team_p80_margin_pct,
  b.team_p80_close_rate,
  b.target_conversion_rate,
  b.target_margin_pct,
  b.target_growth_pct,
  b.strategic_target_conversion_rate,
  b.strategic_target_margin_pct,
  b.strategic_target_growth_pct,
  case
    when b.consultant_count > 0 then b.total_booked_revenue_amount / b.consultant_count
    else 0
  end as avg_revenue_per_consultant_amount,
  case
    when b.consultant_count > 0 then b.total_itinerary_count::numeric / b.consultant_count
    else 0
  end as avg_itineraries_per_consultant,
  case
    when b.consultant_count > 0 then b.total_lead_count::numeric / b.consultant_count
    else 0
  end as avg_leads_per_consultant,
  c.net_cash_flow_30d,
  c.net_cash_flow_60d,
  c.net_cash_flow_90d,
  c.avg_deposit_coverage_ratio_6m,
  c.total_deposit_gap_amount_6m,
  c.lead_created_count_12m,
  c.lead_closed_won_count_12m,
  c.lead_closed_lost_count_12m,
  c.lead_conversion_rate_12m
from benchmark b
cross join command_center c;

create unique index if not exists idx_ai_context_company_metrics_v1_unique
  on public.ai_context_company_metrics_v1(period_type, domain);

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
