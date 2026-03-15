-- Canonical AP/liquidity rollups sourced from supplier invoice lines + bookings.
-- This replaces header-only payable derivations and feeds payments-out, cash-flow, and FX pressure.

create or replace view public.ap_open_liability_v1 as
with line_base as (
  select
    sil.id as supplier_invoice_line_id,
    sil.external_id as supplier_invoice_line_external_id,
    sil.supplier_invoice_booking_id,
    sil.supplier_invoice_booking_external_id,
    sil.supplier_invoice_id,
    coalesce(sil.due_date, si.due_date) as due_date,
    coalesce(sil.booking_date, sil.service_date, si.invoice_date) as service_date,
    coalesce(
      sil.currency_code,
      sib.currency_code,
      si.currency_code
    ) as currency_code,
    coalesce(
      sil.supplier_id,
      sib.supplier_id,
      si.supplier_id
    ) as supplier_id,
    coalesce(sil.itinerary_id, sib.itinerary_id) as itinerary_id,
    coalesce(
      sil.balance_due,
      sil.cost_due,
      sil.line_amount,
      sil.balance,
      sil.original_cost_due,
      0
    )::numeric as outstanding_amount,
    coalesce(
      sil.line_name,
      sil.item_name,
      sil.line_description,
      sil.description
    ) as line_label,
    coalesce(sil.is_deleted, false) as is_deleted,
    coalesce(sil.is_cancelled, false) as is_cancelled,
    coalesce(sil.is_complete, false) as is_complete
  from public.supplier_invoice_lines sil
  left join public.supplier_invoice_bookings sib
    on sib.id = sil.supplier_invoice_booking_id
    or (
      sil.supplier_invoice_booking_id is null
      and sil.supplier_invoice_booking_external_id is not null
      and sib.external_id = sil.supplier_invoice_booking_external_id
    )
  left join public.supplier_invoices si
    on si.id = sil.supplier_invoice_id
),
filtered as (
  select
    supplier_invoice_line_id,
    supplier_invoice_line_external_id,
    supplier_invoice_booking_id,
    supplier_invoice_booking_external_id,
    supplier_invoice_id,
    due_date,
    service_date,
    currency_code,
    supplier_id,
    itinerary_id,
    outstanding_amount,
    line_label
  from line_base
  where is_deleted = false
    and is_cancelled = false
    and is_complete = false
    and (
      supplier_invoice_booking_id is not null
      or supplier_invoice_booking_external_id is not null
    )
    and coalesce(currency_code, '') <> ''
    and outstanding_amount > 0
),
joined as (
  select
    f.*,
    s.supplier_name
  from filtered f
  left join public.suppliers s
    on s.id = f.supplier_id
)
select
  supplier_invoice_line_id,
  supplier_invoice_line_external_id,
  supplier_invoice_booking_id,
  supplier_invoice_booking_external_id,
  supplier_invoice_id,
  supplier_id,
  supplier_name,
  itinerary_id,
  line_label,
  service_date,
  due_date,
  coalesce(due_date, service_date) as effective_payment_date,
  currency_code,
  outstanding_amount
from joined;

create or replace view public.ap_summary_v1 as
select
  currency_code,
  count(*)::integer as open_line_count,
  count(
    distinct coalesce(supplier_invoice_booking_id::text, supplier_invoice_booking_external_id)
  )::integer as open_booking_count,
  count(distinct supplier_id)::integer as open_supplier_count,
  sum(outstanding_amount)::numeric as total_outstanding_amount,
  min(due_date) as next_due_date
from public.ap_open_liability_v1
group by currency_code;

create or replace view public.ap_aging_v1 as
select
  currency_code,
  count(*)::integer as open_line_count,
  sum(outstanding_amount)::numeric as total_outstanding_amount,
  sum(
    case
      when due_date is null or due_date >= current_date then outstanding_amount
      else 0
    end
  )::numeric as current_not_due_amount,
  sum(
    case
      when due_date between current_date - interval '30 days' and current_date - interval '1 day'
        then outstanding_amount
      else 0
    end
  )::numeric as overdue_1_30_amount,
  sum(
    case
      when due_date between current_date - interval '60 days' and current_date - interval '31 days'
        then outstanding_amount
      else 0
    end
  )::numeric as overdue_31_60_amount,
  sum(
    case
      when due_date between current_date - interval '90 days' and current_date - interval '61 days'
        then outstanding_amount
      else 0
    end
  )::numeric as overdue_61_90_amount,
  sum(
    case
      when due_date < current_date - interval '90 days'
        then outstanding_amount
      else 0
    end
  )::numeric as overdue_90_plus_amount
from public.ap_open_liability_v1
group by currency_code;

create or replace view public.ap_payment_calendar_v1 as
select
  coalesce(due_date, service_date) as payment_date,
  currency_code,
  count(*)::integer as line_count,
  count(distinct supplier_id)::integer as supplier_count,
  sum(outstanding_amount)::numeric as amount_due
from public.ap_open_liability_v1
group by coalesce(due_date, service_date), currency_code;

create or replace view public.ap_pressure_30_60_90_v1 as
select
  currency_code,
  sum(
    case
      when coalesce(due_date, service_date) <= current_date + interval '7 days'
        then outstanding_amount
      else 0
    end
  )::numeric as due_7d_amount,
  sum(
    case
      when coalesce(due_date, service_date) <= current_date + interval '30 days'
        then outstanding_amount
      else 0
    end
  )::numeric as due_30d_amount,
  sum(
    case
      when coalesce(due_date, service_date) <= current_date + interval '60 days'
        then outstanding_amount
      else 0
    end
  )::numeric as due_60d_amount,
  sum(
    case
      when coalesce(due_date, service_date) <= current_date + interval '90 days'
        then outstanding_amount
      else 0
    end
  )::numeric as due_90d_amount,
  count(*) filter (
    where coalesce(due_date, service_date) <= current_date + interval '30 days'
  )::integer as invoices_due_30d_count,
  min(coalesce(due_date, service_date)) filter (
    where coalesce(due_date, service_date) >= current_date
  ) as next_due_date
from public.ap_open_liability_v1
where currency_code in ('AUD', 'NZD', 'ZAR')
group by currency_code;

create or replace view public.fx_invoice_pressure_v1 as
select
  currency_code,
  due_7d_amount,
  due_30d_amount,
  due_60d_amount,
  due_90d_amount,
  invoices_due_30d_count,
  next_due_date
from public.ap_pressure_30_60_90_v1;

grant select on public.ap_open_liability_v1 to authenticated;
grant select on public.ap_open_liability_v1 to service_role;
grant select on public.ap_summary_v1 to authenticated;
grant select on public.ap_summary_v1 to service_role;
grant select on public.ap_aging_v1 to authenticated;
grant select on public.ap_aging_v1 to service_role;
grant select on public.ap_payment_calendar_v1 to authenticated;
grant select on public.ap_payment_calendar_v1 to service_role;
grant select on public.ap_pressure_30_60_90_v1 to authenticated;
grant select on public.ap_pressure_30_60_90_v1 to service_role;
grant select on public.fx_invoice_pressure_v1 to authenticated;
grant select on public.fx_invoice_pressure_v1 to service_role;
