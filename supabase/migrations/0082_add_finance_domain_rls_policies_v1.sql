-- Forward-only migration:
-- Enable RLS and add finance planning domain policies.

alter table if exists public.financial_categories enable row level security;
alter table if exists public.budget_versions enable row level security;
alter table if exists public.budget_lines enable row level security;
alter table if exists public.budget_assumptions enable row level security;
alter table if exists public.account_mapping enable row level security;
alter table if exists public.actuals_monthly enable row level security;
alter table if exists public.quickbooks_sync_cursors enable row level security;
alter table if exists public.quickbooks_sync_runs enable row level security;

drop policy if exists financial_categories_select_authenticated on public.financial_categories;
create policy financial_categories_select_authenticated
on public.financial_categories for select
using (auth.role() = 'authenticated');

drop policy if exists financial_categories_crud_admin on public.financial_categories;
create policy financial_categories_crud_admin
on public.financial_categories for all
using (public.is_admin() or auth.role() = 'service_role')
with check (public.is_admin() or auth.role() = 'service_role');

drop policy if exists budget_versions_select_authenticated on public.budget_versions;
create policy budget_versions_select_authenticated
on public.budget_versions for select
using (auth.role() = 'authenticated');

drop policy if exists budget_versions_crud_admin on public.budget_versions;
create policy budget_versions_crud_admin
on public.budget_versions for all
using (public.is_admin() or auth.role() = 'service_role')
with check (public.is_admin() or auth.role() = 'service_role');

drop policy if exists budget_lines_select_authenticated on public.budget_lines;
create policy budget_lines_select_authenticated
on public.budget_lines for select
using (auth.role() = 'authenticated');

drop policy if exists budget_lines_crud_admin on public.budget_lines;
create policy budget_lines_crud_admin
on public.budget_lines for all
using (public.is_admin() or auth.role() = 'service_role')
with check (public.is_admin() or auth.role() = 'service_role');

drop policy if exists budget_assumptions_select_authenticated on public.budget_assumptions;
create policy budget_assumptions_select_authenticated
on public.budget_assumptions for select
using (auth.role() = 'authenticated');

drop policy if exists budget_assumptions_crud_admin on public.budget_assumptions;
create policy budget_assumptions_crud_admin
on public.budget_assumptions for all
using (public.is_admin() or auth.role() = 'service_role')
with check (public.is_admin() or auth.role() = 'service_role');

drop policy if exists account_mapping_select_authenticated on public.account_mapping;
create policy account_mapping_select_authenticated
on public.account_mapping for select
using (auth.role() = 'authenticated');

drop policy if exists account_mapping_crud_admin on public.account_mapping;
create policy account_mapping_crud_admin
on public.account_mapping for all
using (public.is_admin() or auth.role() = 'service_role')
with check (public.is_admin() or auth.role() = 'service_role');

drop policy if exists actuals_monthly_select_authenticated on public.actuals_monthly;
create policy actuals_monthly_select_authenticated
on public.actuals_monthly for select
using (auth.role() = 'authenticated');

drop policy if exists actuals_monthly_insert_service on public.actuals_monthly;
create policy actuals_monthly_insert_service
on public.actuals_monthly for insert
with check (auth.role() = 'service_role');

drop policy if exists actuals_monthly_update_service on public.actuals_monthly;
create policy actuals_monthly_update_service
on public.actuals_monthly for update
using (auth.role() = 'service_role')
with check (auth.role() = 'service_role');

drop policy if exists quickbooks_sync_cursors_select_authenticated on public.quickbooks_sync_cursors;
create policy quickbooks_sync_cursors_select_authenticated
on public.quickbooks_sync_cursors for select
using (auth.role() = 'authenticated');

drop policy if exists quickbooks_sync_cursors_insert_service on public.quickbooks_sync_cursors;
create policy quickbooks_sync_cursors_insert_service
on public.quickbooks_sync_cursors for insert
with check (auth.role() = 'service_role');

drop policy if exists quickbooks_sync_cursors_update_service on public.quickbooks_sync_cursors;
create policy quickbooks_sync_cursors_update_service
on public.quickbooks_sync_cursors for update
using (auth.role() = 'service_role')
with check (auth.role() = 'service_role');

drop policy if exists quickbooks_sync_runs_select_authenticated on public.quickbooks_sync_runs;
create policy quickbooks_sync_runs_select_authenticated
on public.quickbooks_sync_runs for select
using (auth.role() = 'authenticated');

drop policy if exists quickbooks_sync_runs_insert_service on public.quickbooks_sync_runs;
create policy quickbooks_sync_runs_insert_service
on public.quickbooks_sync_runs for insert
with check (auth.role() = 'service_role');

drop policy if exists quickbooks_sync_runs_update_service on public.quickbooks_sync_runs;
create policy quickbooks_sync_runs_update_service
on public.quickbooks_sync_runs for update
using (auth.role() = 'service_role')
with check (auth.role() = 'service_role');
