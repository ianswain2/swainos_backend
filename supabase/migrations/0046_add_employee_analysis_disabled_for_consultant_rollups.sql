-- Add employee analysis toggle scoped to consultant analytics only.
-- IMPORTANT: This flag excludes employees only from consultant-focused rollups and
-- downstream consultant AI context. It does NOT change global itinerary revenue,
-- forecast, deposit, or company-level financial rollups.

alter table public.employees
  add column if not exists analysis_disabled boolean not null default false;

create index if not exists idx_employees_analysis_disabled
  on public.employees(analysis_disabled);

drop materialized view if exists public.ai_context_company_metrics_v1;
drop materialized view if exists public.ai_context_consultant_benchmarks_v1;
drop materialized view if exists public.ai_context_travel_consultant_v1;

drop materialized view if exists public.mv_travel_consultant_compensation_monthly;
drop materialized view if exists public.mv_travel_consultant_funnel_monthly;
drop materialized view if exists public.mv_travel_consultant_profile_monthly;
drop materialized view if exists public.mv_travel_consultant_leaderboard_monthly;

create materialized view public.mv_travel_consultant_leaderboard_monthly as
select
  date_trunc('month', i.travel_end_date)::date as period_start,
  (date_trunc('month', i.travel_end_date)::date + interval '1 month - 1 day')::date as period_end,
  i.employee_id,
  e.external_id as employee_external_id,
  e.first_name,
  e.last_name,
  e.email,
  count(*) as itinerary_count,
  sum(coalesce(i.pax_count, 0)) as pax_count,
  sum(coalesce(i.gross_amount, 0)) as booked_revenue_amount,
  sum(coalesce(i.gross_profit, 0)) as commission_income_amount,
  sum(coalesce(i.gross_amount, 0) - coalesce(i.net_amount, 0)) as margin_amount,
  case
    when sum(coalesce(i.gross_amount, 0)) > 0
      then sum(coalesce(i.gross_amount, 0) - coalesce(i.net_amount, 0)) / sum(coalesce(i.gross_amount, 0))
    else 0
  end as margin_pct,
  case
    when count(*) > 0 then sum(coalesce(i.gross_amount, 0)) / count(*)
    else 0
  end as avg_booking_value_amount
from public.itineraries i
join public.employees e
  on e.id = i.employee_id
left join public.itinerary_status_reference sr
  on sr.status_value = i.itinerary_status
where i.travel_end_date is not null
  and coalesce(sr.is_filter_out, false) = false
  and coalesce(sr.pipeline_bucket, 'open') = 'closed_won'
  and coalesce(e.analysis_disabled, false) = false
group by 1, 2, 3, 4, 5, 6, 7;

create unique index if not exists idx_mv_travel_consultant_leaderboard_monthly_unique
  on public.mv_travel_consultant_leaderboard_monthly(period_start, employee_id);

create index if not exists idx_mv_travel_consultant_leaderboard_monthly_employee
  on public.mv_travel_consultant_leaderboard_monthly(employee_id);

create materialized view public.mv_travel_consultant_profile_monthly as
select
  date_trunc('month', i.travel_end_date)::date as period_start,
  (date_trunc('month', i.travel_end_date)::date + interval '1 month - 1 day')::date as period_end,
  i.employee_id,
  e.external_id as employee_external_id,
  e.first_name,
  e.last_name,
  e.email,
  count(*) as itinerary_count,
  sum(coalesce(i.pax_count, 0)) as pax_count,
  sum(coalesce(i.gross_amount, 0)) as booked_revenue_amount,
  sum(coalesce(i.net_amount, 0)) as net_amount,
  sum(coalesce(i.gross_profit, 0)) as commission_income_amount,
  sum(coalesce(i.gross_amount, 0) - coalesce(i.net_amount, 0)) as margin_amount,
  case
    when sum(coalesce(i.gross_amount, 0)) > 0
      then sum(coalesce(i.gross_amount, 0) - coalesce(i.net_amount, 0)) / sum(coalesce(i.gross_amount, 0))
    else 0
  end as margin_pct,
  avg(coalesce(i.number_of_days, 0)) as avg_number_of_days,
  avg(coalesce(i.number_of_nights, 0)) as avg_number_of_nights
from public.itineraries i
join public.employees e
  on e.id = i.employee_id
left join public.itinerary_status_reference sr
  on sr.status_value = i.itinerary_status
where i.travel_end_date is not null
  and coalesce(sr.is_filter_out, false) = false
  and coalesce(sr.pipeline_bucket, 'open') = 'closed_won'
  and coalesce(e.analysis_disabled, false) = false
group by 1, 2, 3, 4, 5, 6, 7;

create unique index if not exists idx_mv_travel_consultant_profile_monthly_unique
  on public.mv_travel_consultant_profile_monthly(period_start, employee_id);

create index if not exists idx_mv_travel_consultant_profile_monthly_employee
  on public.mv_travel_consultant_profile_monthly(employee_id);

create materialized view public.mv_travel_consultant_funnel_monthly as
select
  date_trunc('month', i.created_at)::date as period_start,
  (date_trunc('month', i.created_at)::date + interval '1 month - 1 day')::date as period_end,
  i.employee_id,
  e.external_id as employee_external_id,
  e.first_name,
  e.last_name,
  e.email,
  count(*)::int as lead_count,
  sum(
    case
      when coalesce(sr.pipeline_bucket, 'open') = 'closed_won' then 1
      else 0
    end
  )::int as closed_won_count,
  sum(
    case
      when coalesce(sr.pipeline_bucket, 'open') = 'closed_lost' then 1
      else 0
    end
  )::int as closed_lost_count,
  sum(
    case
      when coalesce(sr.pipeline_bucket, 'open') = 'closed_won' then coalesce(i.gross_amount, 0)
      else 0
    end
  ) as booked_revenue_amount,
  percentile_cont(0.5) within group (
    order by greatest(0, i.close_date - i.created_at::date)::numeric
  ) filter (
    where coalesce(sr.pipeline_bucket, 'open') = 'closed_won'
      and i.close_date is not null
      and i.created_at is not null
  ) as median_speed_to_book_days
from public.itineraries i
join public.employees e
  on e.id = i.employee_id
left join public.itinerary_status_reference sr
  on sr.status_value = i.itinerary_status
where i.created_at is not null
  and coalesce(sr.is_filter_out, false) = false
  and coalesce(e.analysis_disabled, false) = false
group by 1, 2, 3, 4, 5, 6, 7;

create unique index if not exists idx_mv_travel_consultant_funnel_monthly_unique
  on public.mv_travel_consultant_funnel_monthly(period_start, employee_id);

create index if not exists idx_mv_travel_consultant_funnel_monthly_employee
  on public.mv_travel_consultant_funnel_monthly(employee_id);

create materialized view public.mv_travel_consultant_compensation_monthly as
select
  p.period_start,
  p.period_end,
  p.employee_id,
  p.employee_external_id,
  p.first_name,
  p.last_name,
  p.email,
  coalesce(e.salary, 0) as salary_annual_amount,
  coalesce(e.salary, 0) / 12.0 as salary_monthly_amount,
  e.commission_rate as commission_rate,
  coalesce(p.commission_income_amount, 0) as commission_income_amount,
  coalesce(p.commission_income_amount, 0) * e.commission_rate as estimated_commission_amount,
  (coalesce(e.salary, 0) / 12.0) + (coalesce(p.commission_income_amount, 0) * e.commission_rate) as estimated_total_pay_amount
from public.mv_travel_consultant_profile_monthly p
join public.employees e
  on e.id = p.employee_id
where coalesce(e.analysis_disabled, false) = false;

create unique index if not exists idx_mv_travel_consultant_compensation_monthly_unique
  on public.mv_travel_consultant_compensation_monthly(period_start, employee_id);

create index if not exists idx_mv_travel_consultant_compensation_monthly_employee
  on public.mv_travel_consultant_compensation_monthly(employee_id);

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

create or replace function public.refresh_consultant_ai_rollups_v1()
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
begin
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
