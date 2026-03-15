-- Travel consultant leaderboard rollup by realized travel month.

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
group by 1, 2, 3, 4, 5, 6, 7;

create unique index if not exists idx_mv_travel_consultant_leaderboard_monthly_unique
  on public.mv_travel_consultant_leaderboard_monthly(period_start, employee_id);

create index if not exists idx_mv_travel_consultant_leaderboard_monthly_employee
  on public.mv_travel_consultant_leaderboard_monthly(employee_id);
