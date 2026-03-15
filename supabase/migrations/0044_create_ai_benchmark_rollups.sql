drop materialized view if exists public.ai_context_consultant_benchmarks_v1;
drop materialized view if exists public.ai_context_company_metrics_v1;

create materialized view public.ai_context_consultant_benchmarks_v1 as
with window_defs as (
  select
    'monthly'::text as period_type,
    date_trunc('month', current_date)::date as period_start,
    (date_trunc('month', current_date)::date + interval '1 month - 1 day')::date as period_end
  union all
  select
    'year'::text,
    date_trunc('year', current_date)::date,
    (date_trunc('year', current_date)::date + interval '1 year - 1 day')::date
  union all
  select
    'rolling12'::text,
    (date_trunc('month', current_date)::date - interval '11 months')::date,
    (date_trunc('month', current_date)::date + interval '1 month - 1 day')::date
),
travel_by_consultant as (
  select
    wd.period_type,
    l.employee_id,
    sum(coalesce(l.itinerary_count, 0))::bigint as itinerary_count,
    sum(coalesce(l.booked_revenue_amount, 0))::numeric as travel_booked_revenue_amount,
    sum(coalesce(l.commission_income_amount, 0))::numeric as commission_income_amount,
    sum(coalesce(l.margin_amount, 0))::numeric as margin_amount
  from window_defs wd
  join public.mv_travel_consultant_leaderboard_monthly l
    on l.period_start >= wd.period_start
   and l.period_start <= wd.period_end
  group by wd.period_type, l.employee_id
),
funnel_by_consultant as (
  select
    wd.period_type,
    f.employee_id,
    sum(coalesce(f.lead_count, 0))::bigint as lead_count,
    sum(coalesce(f.closed_won_count, 0))::bigint as closed_won_count,
    sum(coalesce(f.closed_lost_count, 0))::bigint as closed_lost_count,
    sum(coalesce(f.booked_revenue_amount, 0))::numeric as funnel_booked_revenue_amount,
    avg(nullif(f.median_speed_to_book_days, 0))::numeric as avg_speed_to_book_days
  from window_defs wd
  join public.mv_travel_consultant_funnel_monthly f
    on f.period_start >= wd.period_start
   and f.period_start <= wd.period_end
  group by wd.period_type, f.employee_id
),
consultant_scope as (
  select
    coalesce(t.period_type, f.period_type) as period_type,
    coalesce(t.employee_id, f.employee_id) as employee_id,
    coalesce(t.itinerary_count, 0)::bigint as itinerary_count,
    coalesce(f.lead_count, 0)::bigint as lead_count,
    coalesce(f.closed_won_count, 0)::bigint as closed_won_count,
    coalesce(f.closed_lost_count, 0)::bigint as closed_lost_count,
    coalesce(t.travel_booked_revenue_amount, 0)::numeric as travel_booked_revenue_amount,
    coalesce(f.funnel_booked_revenue_amount, 0)::numeric as funnel_booked_revenue_amount,
    coalesce(t.commission_income_amount, 0)::numeric as commission_income_amount,
    coalesce(t.margin_amount, 0)::numeric as margin_amount,
    case
      when coalesce(t.travel_booked_revenue_amount, 0) > 0
        then coalesce(t.margin_amount, 0) / t.travel_booked_revenue_amount
      else 0
    end as margin_pct,
    case
      when coalesce(f.lead_count, 0) > 0
        then coalesce(f.closed_won_count, 0)::numeric / f.lead_count
      else 0
    end as conversion_rate,
    case
      when coalesce(f.closed_won_count, 0) + coalesce(f.closed_lost_count, 0) > 0
        then coalesce(f.closed_won_count, 0)::numeric / (coalesce(f.closed_won_count, 0) + coalesce(f.closed_lost_count, 0))
      else 0
    end as close_rate,
    coalesce(f.avg_speed_to_book_days, 0)::numeric as avg_speed_to_book_days
  from travel_by_consultant t
  full outer join funnel_by_consultant f
    on t.period_type = f.period_type
   and t.employee_id = f.employee_id
),
consultant_domain_rows as (
  select
    period_type,
    'travel'::text as domain,
    employee_id,
    itinerary_count,
    lead_count,
    closed_won_count,
    closed_lost_count,
    travel_booked_revenue_amount as booked_revenue_amount,
    travel_booked_revenue_amount as margin_revenue_basis_amount,
    commission_income_amount,
    margin_amount,
    margin_pct,
    conversion_rate,
    close_rate,
    avg_speed_to_book_days
  from consultant_scope
  union all
  select
    period_type,
    'funnel'::text as domain,
    employee_id,
    itinerary_count,
    lead_count,
    closed_won_count,
    closed_lost_count,
    funnel_booked_revenue_amount as booked_revenue_amount,
    travel_booked_revenue_amount as margin_revenue_basis_amount,
    commission_income_amount,
    margin_amount,
    margin_pct,
    conversion_rate,
    close_rate,
    avg_speed_to_book_days
  from consultant_scope
)
select
  current_date as as_of_date,
  period_type,
  domain,
  count(*)::int as consultant_count,
  avg(conversion_rate)::numeric as team_avg_conversion_rate,
  avg(margin_pct)::numeric as team_avg_margin_pct,
  avg(close_rate)::numeric as team_avg_close_rate,
  avg(avg_speed_to_book_days)::numeric as team_avg_speed_to_book_days,
  max(conversion_rate)::numeric as team_top_conversion_rate,
  max(margin_pct)::numeric as team_top_margin_pct,
  max(close_rate)::numeric as team_top_close_rate,
  min(conversion_rate)::numeric as team_low_conversion_rate,
  min(margin_pct)::numeric as team_low_margin_pct,
  min(close_rate)::numeric as team_low_close_rate,
  percentile_cont(0.5) within group (order by conversion_rate)::numeric as team_median_conversion_rate,
  percentile_cont(0.5) within group (order by margin_pct)::numeric as team_median_margin_pct,
  percentile_cont(0.5) within group (order by close_rate)::numeric as team_median_close_rate,
  percentile_cont(0.8) within group (order by conversion_rate)::numeric as team_p80_conversion_rate,
  percentile_cont(0.8) within group (order by margin_pct)::numeric as team_p80_margin_pct,
  percentile_cont(0.8) within group (order by close_rate)::numeric as team_p80_close_rate,
  percentile_cont(0.2) within group (order by conversion_rate)::numeric as team_p20_conversion_rate,
  percentile_cont(0.2) within group (order by margin_pct)::numeric as team_p20_margin_pct,
  percentile_cont(0.2) within group (order by close_rate)::numeric as team_p20_close_rate,
  sum(itinerary_count)::bigint as total_itinerary_count,
  sum(lead_count)::bigint as total_lead_count,
  sum(closed_won_count)::bigint as total_closed_won_count,
  sum(closed_lost_count)::bigint as total_closed_lost_count,
  sum(booked_revenue_amount)::numeric as total_booked_revenue_amount,
  sum(commission_income_amount)::numeric as total_commission_income_amount,
  sum(margin_amount)::numeric as total_margin_amount,
  case
    when sum(margin_revenue_basis_amount) > 0 then sum(margin_amount) / sum(margin_revenue_basis_amount)
    else 0
  end as weighted_margin_pct,
  case
    when sum(lead_count) > 0 then sum(closed_won_count)::numeric / sum(lead_count)
    else 0
  end as weighted_conversion_rate,
  case
    when sum(closed_won_count) + sum(closed_lost_count) > 0
      then sum(closed_won_count)::numeric / (sum(closed_won_count) + sum(closed_lost_count))
    else 0
  end as weighted_close_rate,
  greatest(0.35::numeric, avg(conversion_rate) * 0.85) as target_conversion_rate,
  greatest(0.08::numeric, avg(margin_pct) * 0.85) as target_margin_pct,
  0.12::numeric as target_growth_pct,
  0.35::numeric as strategic_target_conversion_rate,
  0.20::numeric as strategic_target_margin_pct,
  0.12::numeric as strategic_target_growth_pct
from consultant_domain_rows
group by period_type, domain;

create unique index if not exists idx_ai_context_consultant_benchmarks_v1_unique
  on public.ai_context_consultant_benchmarks_v1(period_type, domain);

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
  b.total_commission_income_amount,
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
