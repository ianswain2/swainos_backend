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
    when coalesce(l.created_count_12m, 0) > 0 then coalesce(l.closed_won_count_12m, 0) / l.created_count_12m
    else 0
  end as lead_conversion_rate_12m
from cash_window c
cross join deposit_window d
cross join lead_window l;

create unique index if not exists idx_ai_context_command_center_v1_unique
  on public.ai_context_command_center_v1(as_of_date);

drop materialized view if exists public.ai_context_travel_consultant_v1;

create materialized view public.ai_context_travel_consultant_v1 as
with latest_profile as (
  select
    p.*,
    row_number() over (partition by p.employee_id order by p.period_start desc) as row_num
  from public.mv_travel_consultant_profile_monthly p
),
latest_funnel as (
  select
    f.*,
    row_number() over (partition by f.employee_id order by f.period_start desc) as row_num
  from public.mv_travel_consultant_funnel_monthly f
),
latest_comp as (
  select
    c.*,
    row_number() over (partition by c.employee_id order by c.period_start desc) as row_num
  from public.mv_travel_consultant_compensation_monthly c
),
baseline_profile as (
  select
    p.employee_id,
    avg(coalesce(p.booked_revenue_amount, 0)) as avg_booked_revenue_12m
  from public.mv_travel_consultant_profile_monthly p
  where p.period_start >= (date_trunc('month', current_date)::date - interval '11 months')::date
  group by p.employee_id
)
select
  lp.period_start as as_of_period_start,
  lp.period_end as as_of_period_end,
  lp.employee_id,
  lp.employee_external_id,
  lp.first_name,
  lp.last_name,
  lp.email,
  coalesce(lp.itinerary_count, 0) as itinerary_count,
  coalesce(lp.booked_revenue_amount, 0) as booked_revenue_amount,
  coalesce(lp.commission_income_amount, 0) as commission_income_amount,
  coalesce(lp.margin_pct, 0) as margin_pct,
  coalesce(lf.lead_count, 0) as lead_count,
  coalesce(lf.closed_won_count, 0) as closed_won_count,
  coalesce(lf.closed_lost_count, 0) as closed_lost_count,
  case
    when coalesce(lf.lead_count, 0) > 0 then coalesce(lf.closed_won_count, 0)::numeric / lf.lead_count
    else 0
  end as conversion_rate,
  case
    when (coalesce(lf.closed_won_count, 0) + coalesce(lf.closed_lost_count, 0)) > 0
      then coalesce(lf.closed_won_count, 0)::numeric / (coalesce(lf.closed_won_count, 0) + coalesce(lf.closed_lost_count, 0))
    else 0
  end as close_rate,
  coalesce(lf.median_speed_to_book_days, 0) as avg_speed_to_book_days,
  coalesce(lc.salary_monthly_amount, 0) as salary_monthly_amount,
  coalesce(lc.estimated_total_pay_amount, 0) as estimated_total_pay_amount,
  coalesce(bp.avg_booked_revenue_12m, 0) as avg_booked_revenue_12m,
  case
    when coalesce(bp.avg_booked_revenue_12m, 0) > 0
      then (coalesce(lp.booked_revenue_amount, 0) - (bp.avg_booked_revenue_12m * 1.12)) / (bp.avg_booked_revenue_12m * 1.12)
    else 0
  end as growth_target_variance_pct
from latest_profile lp
left join latest_funnel lf
  on lf.employee_id = lp.employee_id and lf.row_num = 1
left join latest_comp lc
  on lc.employee_id = lp.employee_id and lc.row_num = 1
left join baseline_profile bp
  on bp.employee_id = lp.employee_id
where lp.row_num = 1;

create unique index if not exists idx_ai_context_travel_consultant_v1_unique
  on public.ai_context_travel_consultant_v1(employee_id);

create index if not exists idx_ai_context_travel_consultant_v1_period
  on public.ai_context_travel_consultant_v1(as_of_period_start);

drop materialized view if exists public.ai_context_itinerary_health_v1;

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
    sum(coalesce(commission_income_amount, 0)) filter (where pipeline_bucket = 'closed_won') as closed_won_commission_income_amount
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
  coalesce(rw.closed_won_commission_income_amount, 0) as closed_won_commission_income_amount,
  coalesce(dw.deposit_coverage_ratio, 0) as deposit_coverage_ratio,
  coalesce(dw.deposit_gap_amount, 0) as deposit_gap_amount
from lead_window lw
left join revenue_window rw
  on rw.period_start = lw.period_start
left join deposit_window dw
  on dw.period_start = lw.period_start;

create unique index if not exists idx_ai_context_itinerary_health_v1_unique
  on public.ai_context_itinerary_health_v1(period_start);

