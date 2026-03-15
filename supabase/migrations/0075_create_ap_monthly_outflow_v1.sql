create or replace view public.ap_monthly_outflow_v1 as
select
  date_trunc('month', effective_payment_date)::date as month_start,
  currency_code,
  count(*)::integer as line_count,
  count(distinct supplier_id)::integer as supplier_count,
  sum(outstanding_amount)::numeric as amount_due
from public.ap_open_liability_v1
group by date_trunc('month', effective_payment_date)::date, currency_code;

grant select on public.ap_monthly_outflow_v1 to authenticated;
grant select on public.ap_monthly_outflow_v1 to service_role;
