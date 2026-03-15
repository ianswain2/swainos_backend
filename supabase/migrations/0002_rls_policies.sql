-- SwainOS RLS policies based on spec

create or replace function public.is_admin()
returns boolean
language sql
stable
as $$
  select coalesce(auth.jwt() ->> 'role', '') = 'admin';
$$;

-- Enable RLS on all tables
alter table public.agencies enable row level security;
alter table public.contacts enable row level security;
alter table public.itineraries enable row level security;
alter table public.bookings enable row level security;
alter table public.itinerary_items enable row level security;
alter table public.customer_payments enable row level security;
alter table public.transactions enable row level security;
alter table public.debt_schedules enable row level security;
alter table public.debt_payments enable row level security;
alter table public.budgets enable row level security;
alter table public.suppliers enable row level security;
alter table public.supplier_invoices enable row level security;
alter table public.supplier_invoice_lines enable row level security;
alter table public.fx_rates enable row level security;
alter table public.fx_holdings enable row level security;
alter table public.fx_transactions enable row level security;
alter table public.fx_signals enable row level security;
alter table public.locations enable row level security;
alter table public.currencies enable row level security;
alter table public.sync_logs enable row level security;
alter table public.ai_interactions enable row level security;
alter table public.app_users enable row level security;
alter table public.app_settings enable row level security;

-- Read-only tables for authenticated users
create policy agencies_select_authenticated
on public.agencies for select
using (auth.role() = 'authenticated');

create policy contacts_select_authenticated
on public.contacts for select
using (auth.role() = 'authenticated');

create policy itineraries_select_authenticated
on public.itineraries for select
using (auth.role() = 'authenticated');

create policy bookings_select_authenticated
on public.bookings for select
using (auth.role() = 'authenticated');

create policy itinerary_items_select_authenticated
on public.itinerary_items for select
using (auth.role() = 'authenticated');

create policy customer_payments_select_authenticated
on public.customer_payments for select
using (auth.role() = 'authenticated');

create policy transactions_select_authenticated
on public.transactions for select
using (auth.role() = 'authenticated');

create policy suppliers_select_authenticated
on public.suppliers for select
using (auth.role() = 'authenticated');

create policy supplier_invoices_select_authenticated
on public.supplier_invoices for select
using (auth.role() = 'authenticated');

create policy supplier_invoice_lines_select_authenticated
on public.supplier_invoice_lines for select
using (auth.role() = 'authenticated');

create policy fx_signals_select_authenticated
on public.fx_signals for select
using (auth.role() = 'authenticated');

create policy locations_select_authenticated
on public.locations for select
using (auth.role() = 'authenticated');

create policy currencies_select_authenticated
on public.currencies for select
using (auth.role() = 'authenticated');

-- Editable tables for admin users (and service_role)
create policy debt_schedules_crud_admin
on public.debt_schedules for all
using (public.is_admin() or auth.role() = 'service_role')
with check (public.is_admin() or auth.role() = 'service_role');

create policy debt_payments_crud_admin
on public.debt_payments for all
using (public.is_admin() or auth.role() = 'service_role')
with check (public.is_admin() or auth.role() = 'service_role');

create policy budgets_crud_admin
on public.budgets for all
using (public.is_admin() or auth.role() = 'service_role')
with check (public.is_admin() or auth.role() = 'service_role');

create policy fx_holdings_crud_admin
on public.fx_holdings for all
using (public.is_admin() or auth.role() = 'service_role')
with check (public.is_admin() or auth.role() = 'service_role');

create policy fx_transactions_crud_admin
on public.fx_transactions for all
using (public.is_admin() or auth.role() = 'service_role')
with check (public.is_admin() or auth.role() = 'service_role');

create policy app_settings_crud_admin
on public.app_settings for all
using (public.is_admin() or auth.role() = 'service_role')
with check (public.is_admin() or auth.role() = 'service_role');

-- Time-series FX rates: select authenticated, insert service_role
create policy fx_rates_select_authenticated
on public.fx_rates for select
using (auth.role() = 'authenticated');

create policy fx_rates_insert_service
on public.fx_rates for insert
with check (auth.role() = 'service_role');

-- app_users: users can read/update their own row; insert by service_role
create policy app_users_select_self
on public.app_users for select
using (auth.uid() = id);

create policy app_users_update_self
on public.app_users for update
using (auth.uid() = id)
with check (auth.uid() = id);

create policy app_users_insert_service
on public.app_users for insert
with check (auth.role() = 'service_role');

-- sync_logs, ai_interactions: select authenticated, insert service_role
create policy sync_logs_select_authenticated
on public.sync_logs for select
using (auth.role() = 'authenticated');

create policy sync_logs_insert_service
on public.sync_logs for insert
with check (auth.role() = 'service_role');

create policy ai_interactions_select_authenticated
on public.ai_interactions for select
using (auth.role() = 'authenticated');

create policy ai_interactions_insert_service
on public.ai_interactions for insert
with check (auth.role() = 'service_role');
