-- Harden Supabase lint findings without changing schema contracts.
-- Focus: RLS coverage, view execution context, and function search_path safety.

-- 1) Ensure RLS is enabled and policies are explicit on flagged tables.
alter table if exists public.contacts enable row level security;
alter table if exists public.itinerary_status_reference enable row level security;
alter table if exists public.supplier_invoice_bookings enable row level security;
alter table if exists public.data_jobs enable row level security;
alter table if exists public.data_job_dependencies enable row level security;
alter table if exists public.data_job_runs enable row level security;
alter table if exists public.data_job_run_steps enable row level security;

-- contacts (lint reported RLS enabled but no policy).
drop policy if exists contacts_select_authenticated on public.contacts;
create policy contacts_select_authenticated
on public.contacts for select
using (auth.role() = 'authenticated');

-- itinerary status reference: readable for app users; admin/service can manage.
drop policy if exists itinerary_status_reference_select_authenticated on public.itinerary_status_reference;
create policy itinerary_status_reference_select_authenticated
on public.itinerary_status_reference for select
using (auth.role() = 'authenticated' or auth.role() = 'service_role');

drop policy if exists itinerary_status_reference_admin_manage on public.itinerary_status_reference;
create policy itinerary_status_reference_admin_manage
on public.itinerary_status_reference for all
using (public.is_admin() or auth.role() = 'service_role')
with check (public.is_admin() or auth.role() = 'service_role');

-- supplier invoice bookings: readable in-app, writable by service role.
drop policy if exists supplier_invoice_bookings_select_authenticated on public.supplier_invoice_bookings;
create policy supplier_invoice_bookings_select_authenticated
on public.supplier_invoice_bookings for select
using (auth.role() = 'authenticated' or auth.role() = 'service_role');

drop policy if exists supplier_invoice_bookings_service_manage on public.supplier_invoice_bookings;
create policy supplier_invoice_bookings_service_manage
on public.supplier_invoice_bookings for all
using (auth.role() = 'service_role')
with check (auth.role() = 'service_role');

-- Data jobs control-plane: admin/service scoped.
drop policy if exists data_jobs_admin_manage on public.data_jobs;
create policy data_jobs_admin_manage
on public.data_jobs for all
using (public.is_admin() or auth.role() = 'service_role')
with check (public.is_admin() or auth.role() = 'service_role');

drop policy if exists data_job_dependencies_admin_manage on public.data_job_dependencies;
create policy data_job_dependencies_admin_manage
on public.data_job_dependencies for all
using (public.is_admin() or auth.role() = 'service_role')
with check (public.is_admin() or auth.role() = 'service_role');

drop policy if exists data_job_runs_admin_manage on public.data_job_runs;
create policy data_job_runs_admin_manage
on public.data_job_runs for all
using (public.is_admin() or auth.role() = 'service_role')
with check (public.is_admin() or auth.role() = 'service_role');

drop policy if exists data_job_run_steps_admin_manage on public.data_job_run_steps;
create policy data_job_run_steps_admin_manage
on public.data_job_run_steps for all
using (public.is_admin() or auth.role() = 'service_role')
with check (public.is_admin() or auth.role() = 'service_role');

-- 2) Avoid SECURITY DEFINER view behavior in API-exposed schema.
alter view if exists public.ap_aging_v1 set (security_invoker = true);
alter view if exists public.ap_payment_calendar_v1 set (security_invoker = true);
alter view if exists public.ap_pressure_30_60_90_v1 set (security_invoker = true);
alter view if exists public.ap_open_liability_v1 set (security_invoker = true);
alter view if exists public.v_debt_service_overview set (security_invoker = true);
alter view if exists public.ap_monthly_outflow_v1 set (security_invoker = true);
alter view if exists public.data_job_health_v1 set (security_invoker = true);
alter view if exists public.fx_invoice_pressure_v1 set (security_invoker = true);
alter view if exists public.ap_summary_v1 set (security_invoker = true);

-- 3) Pin function search_path to reduce mutable-path risk.
do $$
begin
  if to_regprocedure('public.set_updated_at()') is not null then
    execute 'alter function public.set_updated_at() set search_path = pg_catalog, public';
  end if;

  if to_regprocedure('public.marketing_search_console_us_workspace_v1(integer)') is not null then
    execute 'alter function public.marketing_search_console_us_workspace_v1(integer) set search_path = pg_catalog, public';
  end if;

  if to_regprocedure('public.marketing_search_console_page_profile_v1(integer,text)') is not null then
    execute 'alter function public.marketing_search_console_page_profile_v1(integer, text) set search_path = pg_catalog, public';
  end if;

  if to_regprocedure('public.marketing_search_console_insights_rollup_v1(integer,text,text)') is not null then
    execute 'alter function public.marketing_search_console_insights_rollup_v1(integer, text, text) set search_path = pg_catalog, public';
  end if;

  if to_regprocedure('public.upsert_locations_from_raw()') is not null then
    execute 'alter function public.upsert_locations_from_raw() set search_path = pg_catalog, public';
  end if;

  if to_regprocedure('public.upsert_location_from_raw(uuid)') is not null then
    execute 'alter function public.upsert_location_from_raw(uuid) set search_path = pg_catalog, public';
  end if;

  if to_regprocedure('public.is_admin()') is not null then
    execute 'alter function public.is_admin() set search_path = pg_catalog, public';
  end if;
end
$$;
