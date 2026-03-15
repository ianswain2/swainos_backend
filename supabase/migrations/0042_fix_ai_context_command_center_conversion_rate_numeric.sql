drop materialized view if exists public.ai_context_command_center_v1;

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

