-- Forward-only migration:
-- Enable RLS and add policies for debt service domain objects.

alter table if exists public.debt_facilities enable row level security;
alter table if exists public.debt_facility_terms enable row level security;
alter table if exists public.debt_payment_schedule enable row level security;
alter table if exists public.debt_payments_actual enable row level security;
alter table if exists public.debt_balance_snapshots enable row level security;
alter table if exists public.debt_covenants enable row level security;
alter table if exists public.debt_covenant_snapshots enable row level security;
alter table if exists public.debt_scenarios enable row level security;
alter table if exists public.debt_scenario_events enable row level security;

drop policy if exists debt_facilities_select_authenticated on public.debt_facilities;
create policy debt_facilities_select_authenticated
on public.debt_facilities for select
using (auth.role() = 'authenticated');

drop policy if exists debt_facilities_crud_admin on public.debt_facilities;
create policy debt_facilities_crud_admin
on public.debt_facilities for all
using (public.is_admin() or auth.role() = 'service_role')
with check (public.is_admin() or auth.role() = 'service_role');

drop policy if exists debt_facility_terms_select_authenticated on public.debt_facility_terms;
create policy debt_facility_terms_select_authenticated
on public.debt_facility_terms for select
using (auth.role() = 'authenticated');

drop policy if exists debt_facility_terms_crud_admin on public.debt_facility_terms;
create policy debt_facility_terms_crud_admin
on public.debt_facility_terms for all
using (public.is_admin() or auth.role() = 'service_role')
with check (public.is_admin() or auth.role() = 'service_role');

drop policy if exists debt_payment_schedule_select_authenticated on public.debt_payment_schedule;
create policy debt_payment_schedule_select_authenticated
on public.debt_payment_schedule for select
using (auth.role() = 'authenticated');

drop policy if exists debt_payment_schedule_crud_admin on public.debt_payment_schedule;
create policy debt_payment_schedule_crud_admin
on public.debt_payment_schedule for all
using (public.is_admin() or auth.role() = 'service_role')
with check (public.is_admin() or auth.role() = 'service_role');

drop policy if exists debt_payments_actual_select_authenticated on public.debt_payments_actual;
create policy debt_payments_actual_select_authenticated
on public.debt_payments_actual for select
using (auth.role() = 'authenticated');

drop policy if exists debt_payments_actual_crud_admin on public.debt_payments_actual;
create policy debt_payments_actual_crud_admin
on public.debt_payments_actual for all
using (public.is_admin() or auth.role() = 'service_role')
with check (public.is_admin() or auth.role() = 'service_role');

drop policy if exists debt_balance_snapshots_select_authenticated on public.debt_balance_snapshots;
create policy debt_balance_snapshots_select_authenticated
on public.debt_balance_snapshots for select
using (auth.role() = 'authenticated');

drop policy if exists debt_balance_snapshots_crud_admin on public.debt_balance_snapshots;
create policy debt_balance_snapshots_crud_admin
on public.debt_balance_snapshots for all
using (public.is_admin() or auth.role() = 'service_role')
with check (public.is_admin() or auth.role() = 'service_role');

drop policy if exists debt_covenants_select_authenticated on public.debt_covenants;
create policy debt_covenants_select_authenticated
on public.debt_covenants for select
using (auth.role() = 'authenticated');

drop policy if exists debt_covenants_crud_admin on public.debt_covenants;
create policy debt_covenants_crud_admin
on public.debt_covenants for all
using (public.is_admin() or auth.role() = 'service_role')
with check (public.is_admin() or auth.role() = 'service_role');

drop policy if exists debt_covenant_snapshots_select_authenticated on public.debt_covenant_snapshots;
create policy debt_covenant_snapshots_select_authenticated
on public.debt_covenant_snapshots for select
using (auth.role() = 'authenticated');

drop policy if exists debt_covenant_snapshots_crud_admin on public.debt_covenant_snapshots;
create policy debt_covenant_snapshots_crud_admin
on public.debt_covenant_snapshots for all
using (public.is_admin() or auth.role() = 'service_role')
with check (public.is_admin() or auth.role() = 'service_role');

drop policy if exists debt_scenarios_select_authenticated on public.debt_scenarios;
create policy debt_scenarios_select_authenticated
on public.debt_scenarios for select
using (auth.role() = 'authenticated');

drop policy if exists debt_scenarios_crud_admin on public.debt_scenarios;
create policy debt_scenarios_crud_admin
on public.debt_scenarios for all
using (public.is_admin() or auth.role() = 'service_role')
with check (public.is_admin() or auth.role() = 'service_role');

drop policy if exists debt_scenario_events_select_authenticated on public.debt_scenario_events;
create policy debt_scenario_events_select_authenticated
on public.debt_scenario_events for select
using (auth.role() = 'authenticated');

drop policy if exists debt_scenario_events_crud_admin on public.debt_scenario_events;
create policy debt_scenario_events_crud_admin
on public.debt_scenario_events for all
using (public.is_admin() or auth.role() = 'service_role')
with check (public.is_admin() or auth.role() = 'service_role');
