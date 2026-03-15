-- Forward-only migration:
-- Deprecate finance budget + QuickBooks planning domain and remove all related data objects.

-- Drop derived views first.
drop view if exists public.ai_budget_alerts_v1;
drop view if exists public.ai_budget_changes_v1;
drop view if exists public.ai_budget_context_v1;
drop view if exists public.variance_monthly_v1;

-- Drop planning runtime tables (includes all seeded/imported budget+actual data).
drop table if exists public.budget_assumptions cascade;
drop table if exists public.budget_lines cascade;
drop table if exists public.actuals_monthly cascade;
drop table if exists public.account_mapping cascade;
drop table if exists public.quickbooks_sync_runs cascade;
drop table if exists public.quickbooks_sync_cursors cascade;
drop table if exists public.budget_versions cascade;
drop table if exists public.financial_categories cascade;
