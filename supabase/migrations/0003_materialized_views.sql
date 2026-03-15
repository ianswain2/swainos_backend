-- Materialized views for AI analysis and dashboard performance

create materialized view if not exists public.mv_monthly_revenue as
select
  date_trunc('month', travel_start_date)::date as year_month,
  primary_country as destination_country,
  agency_id,
  count(*) as itinerary_count,
  sum(coalesce(pax_count, 0)) as pax_count,
  sum(coalesce(gross_amount, 0)) as gross_revenue,
  sum(coalesce(net_amount, 0)) as net_revenue,
  sum(coalesce(commission_amount, 0)) as commission_earned,
  avg(coalesce(gross_amount, 0)) as avg_trip_value
from public.itineraries
group by 1, 2, 3;

create materialized view if not exists public.mv_rolling_metrics as
select
  date_trunc('month', travel_start_date)::date as metric_date,
  avg(coalesce(gross_amount, 0)) over (
    order by date_trunc('month', travel_start_date)::date
    rows between 2 preceding and current row
  ) as revenue_3mo_avg,
  avg(coalesce(gross_amount, 0)) over (
    order by date_trunc('month', travel_start_date)::date
    rows between 11 preceding and current row
  ) as revenue_12mo_avg,
  avg(coalesce(pax_count, 0)) over (
    order by date_trunc('month', travel_start_date)::date
    rows between 2 preceding and current row
  ) as bookings_3mo_avg,
  avg(coalesce(commission_amount, 0)) over (
    order by date_trunc('month', travel_start_date)::date
    rows between 2 preceding and current row
  ) as margin_3mo_avg,
  0::numeric as yoy_revenue_change,
  0::numeric as yoy_booking_change
from public.itineraries;

create materialized view if not exists public.mv_fx_exposure as
with confirmed as (
  select
    currency_code,
    sum(case when due_date <= current_date + interval '30 days' then coalesce(total_amount, 0) else 0 end) as confirmed_30d,
    sum(case when due_date <= current_date + interval '60 days' then coalesce(total_amount, 0) else 0 end) as confirmed_60d,
    sum(case when due_date <= current_date + interval '90 days' then coalesce(total_amount, 0) else 0 end) as confirmed_90d
  from public.supplier_invoices
  where coalesce(payment_status, '') <> 'Paid'
  group by currency_code
),
estimated as (
  select
    currency_code,
    sum(
      case
        when travel_start_date <= current_date + interval '30 days'
          then coalesce(net_amount, 0) * 0.70 * status_weight * 1.10
        else 0
      end
    ) as estimated_30d,
    sum(
      case
        when travel_start_date <= current_date + interval '60 days'
          then coalesce(net_amount, 0) * 0.70 * status_weight * 1.10
        else 0
      end
    ) as estimated_60d,
    sum(
      case
        when travel_start_date <= current_date + interval '90 days'
          then coalesce(net_amount, 0) * 0.70 * status_weight * 1.10
        else 0
      end
    ) as estimated_90d
  from (
    select
      currency_code,
      travel_start_date,
      net_amount,
      case
        when itinerary_status in ('Deposited/Confirming', 'Pre-Departure', 'eDocs Sent') then 0.95
        when itinerary_status = 'Proposal Sent' then 0.40
        when itinerary_status = 'Assigned' then 0.20
        else 0
      end as status_weight
    from public.itineraries
    where travel_start_date >= current_date
  ) i
  group by currency_code
),
holdings as (
  select currency_code, balance_amount
  from public.fx_holdings
)
select
  coalesce(c.currency_code, e.currency_code, h.currency_code) as currency_code,
  coalesce(c.confirmed_30d, 0) as confirmed_30d,
  coalesce(c.confirmed_60d, 0) as confirmed_60d,
  coalesce(c.confirmed_90d, 0) as confirmed_90d,
  coalesce(e.estimated_30d, 0) as estimated_30d,
  coalesce(e.estimated_60d, 0) as estimated_60d,
  coalesce(e.estimated_90d, 0) as estimated_90d,
  coalesce(h.balance_amount, 0) as current_holdings,
  (
    coalesce(c.confirmed_90d, 0)
    + coalesce(e.estimated_90d, 0)
    - coalesce(h.balance_amount, 0)
  ) as net_exposure
from confirmed c
full outer join estimated e
  on e.currency_code = c.currency_code
full outer join holdings h
  on h.currency_code = coalesce(c.currency_code, e.currency_code);

create materialized view if not exists public.mv_active_travelers as
select
  primary_country as country,
  primary_region as region,
  primary_city as city,
  primary_latitude as latitude,
  primary_longitude as longitude,
  case
    when travel_start_date <= current_date and travel_end_date >= current_date then 'ACTIVE'
    when travel_start_date > current_date then 'UPCOMING'
    else 'COMPLETED'
  end as status_category,
  sum(coalesce(pax_count, 0)) as traveler_count,
  count(*) as itinerary_count,
  sum(case when travel_start_date between current_date and current_date + interval '7 days' then 1 else 0 end) as departing_7d,
  sum(case when travel_end_date between current_date and current_date + interval '7 days' then 1 else 0 end) as returning_7d
from public.itineraries
group by 1, 2, 3, 4, 5, 6;

create materialized view if not exists public.mv_cash_flow_forecast as
with forecast_dates as (
  select generate_series(current_date, current_date + interval '90 days', interval '1 day')::date as forecast_date
),
payments as (
  select
    payment_date as date_key,
    sum(
      case
        when payment_type = 'Deposit' and payment_status in ('Pending', 'Received')
          then coalesce(amount, 0)
        else 0
      end
    ) as expected_deposits,
    sum(
      case
        when payment_type = 'Final Payment' and payment_status in ('Pending', 'Received')
          then coalesce(amount, 0)
        else 0
      end
    ) as expected_payments_due
  from public.customer_payments
  group by payment_date
),
invoices as (
  select due_date as date_key, sum(coalesce(total_amount, 0)) as supplier_invoices_due
  from public.supplier_invoices
  group by due_date
),
debt as (
  select payment_date as date_key, sum(coalesce(principal_amount, 0) + coalesce(interest_amount, 0)) as debt_service_due
  from public.debt_payments
  group by payment_date
)
select
  fd.forecast_date,
  coalesce(p.expected_deposits, 0) as expected_deposits,
  coalesce(p.expected_payments_due, 0) as expected_payments_due,
  coalesce(i.supplier_invoices_due, 0) as supplier_invoices_due,
  coalesce(d.debt_service_due, 0) as debt_service_due,
  coalesce(p.expected_deposits, 0)
    - coalesce(i.supplier_invoices_due, 0)
    - coalesce(d.debt_service_due, 0) as net_cash_flow,
  sum(
    coalesce(p.expected_deposits, 0)
      - coalesce(i.supplier_invoices_due, 0)
      - coalesce(d.debt_service_due, 0)
  ) over (order by fd.forecast_date) as running_balance
from forecast_dates fd
left join payments p on p.date_key = fd.forecast_date
left join invoices i on i.date_key = fd.forecast_date
left join debt d on d.date_key = fd.forecast_date;
