-- Forward-only migration:
-- Create finance variance and AI context views for budget-vs-actual analysis.

create or replace view public.variance_monthly_v1 as
with actuals_rollup as (
  select
    a.month_start,
    a.financial_category_id,
    a.currency_code,
    sum(a.actual_amount) as actual_amount,
    max(a.as_of_date) as as_of_date
  from public.actuals_monthly a
  group by
    a.month_start,
    a.financial_category_id,
    a.currency_code
)
select
  bl.budget_version_id,
  bv.version_name,
  bv.fiscal_year,
  bv.scenario_name,
  bv.status as version_status,
  bl.month_start,
  bl.financial_category_id,
  fc.source_system,
  fc.source_account_code,
  fc.category_name,
  fc.statement_section,
  bl.currency_code,
  bl.budget_amount,
  coalesce(ar.actual_amount, 0)::numeric as actual_amount,
  (coalesce(ar.actual_amount, 0) - bl.budget_amount)::numeric as variance_amount,
  case
    when abs(bl.budget_amount) < 0.0000001 then null
    else ((coalesce(ar.actual_amount, 0) - bl.budget_amount) / abs(bl.budget_amount))
  end as variance_pct,
  ar.as_of_date,
  bl.created_at,
  bl.updated_at
from public.budget_lines bl
join public.budget_versions bv on bv.id = bl.budget_version_id
join public.financial_categories fc on fc.id = bl.financial_category_id
left join actuals_rollup ar
  on ar.month_start = bl.month_start
  and ar.financial_category_id = bl.financial_category_id
  and ar.currency_code = bl.currency_code;

create or replace view public.ai_budget_context_v1 as
select
  v.budget_version_id,
  v.version_name,
  v.fiscal_year,
  v.scenario_name,
  v.version_status,
  v.month_start,
  v.financial_category_id,
  v.source_account_code,
  v.category_name,
  v.statement_section,
  v.currency_code,
  v.budget_amount,
  v.actual_amount,
  v.variance_amount,
  v.variance_pct,
  abs(v.variance_amount) as absolute_variance_amount,
  abs(coalesce(v.variance_pct, 0)) as absolute_variance_pct,
  v.as_of_date
from public.variance_monthly_v1 v;

create or replace view public.ai_budget_changes_v1 as
select
  current_version.id as budget_version_id,
  current_version.version_name,
  current_version.fiscal_year,
  current_version.scenario_name,
  current_line.month_start,
  current_line.financial_category_id,
  current_line.currency_code,
  current_line.budget_amount as current_budget_amount,
  coalesce(base_line.budget_amount, 0)::numeric as baseline_budget_amount,
  (current_line.budget_amount - coalesce(base_line.budget_amount, 0))::numeric as budget_delta_amount
from public.budget_versions current_version
join public.budget_lines current_line
  on current_line.budget_version_id = current_version.id
left join public.budget_lines base_line
  on base_line.budget_version_id = current_version.based_on_version_id
  and base_line.month_start = current_line.month_start
  and base_line.financial_category_id = current_line.financial_category_id
  and base_line.currency_code = current_line.currency_code
where current_version.based_on_version_id is not null;

create or replace view public.ai_budget_alerts_v1 as
select
  v.budget_version_id,
  v.version_name,
  v.fiscal_year,
  v.scenario_name,
  v.month_start,
  v.financial_category_id,
  v.source_account_code,
  v.category_name,
  v.statement_section,
  v.currency_code,
  v.budget_amount,
  v.actual_amount,
  v.variance_amount,
  v.variance_pct,
  abs(v.variance_amount) as absolute_variance_amount,
  abs(coalesce(v.variance_pct, 0)) as absolute_variance_pct,
  case
    when abs(v.variance_amount) >= 100000 or abs(coalesce(v.variance_pct, 0)) >= 0.20 then 'critical'
    when abs(v.variance_amount) >= 50000 or abs(coalesce(v.variance_pct, 0)) >= 0.15 then 'high'
    when abs(v.variance_amount) >= 25000 or abs(coalesce(v.variance_pct, 0)) >= 0.10 then 'medium'
    else 'low'
  end as alert_severity,
  v.as_of_date
from public.variance_monthly_v1 v
where abs(v.variance_amount) >= 25000
   or abs(coalesce(v.variance_pct, 0)) >= 0.10;
