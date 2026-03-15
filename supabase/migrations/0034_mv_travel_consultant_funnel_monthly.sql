-- Travel consultant funnel rollup by lead-created month.

drop materialized view if exists public.mv_travel_consultant_funnel_monthly;

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
group by 1, 2, 3, 4, 5, 6, 7;

create unique index if not exists idx_mv_travel_consultant_funnel_monthly_unique
  on public.mv_travel_consultant_funnel_monthly(period_start, employee_id);

create index if not exists idx_mv_travel_consultant_funnel_monthly_employee
  on public.mv_travel_consultant_funnel_monthly(employee_id);
