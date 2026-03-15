-- Travel consultant compensation rollup by realized travel month.

drop materialized view if exists public.mv_travel_consultant_compensation_monthly;

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
  coalesce(p.commission_income_amount, 0) * e.commission_rate
    as estimated_commission_amount,
  (coalesce(e.salary, 0) / 12.0)
    + (coalesce(p.commission_income_amount, 0) * e.commission_rate)
    as estimated_total_pay_amount
from public.mv_travel_consultant_profile_monthly p
join public.employees e
  on e.id = p.employee_id;

create unique index if not exists idx_mv_travel_consultant_compensation_monthly_unique
  on public.mv_travel_consultant_compensation_monthly(period_start, employee_id);

create index if not exists idx_mv_travel_consultant_compensation_monthly_employee
  on public.mv_travel_consultant_compensation_monthly(employee_id);
