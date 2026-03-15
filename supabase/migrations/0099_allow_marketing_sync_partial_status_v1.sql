-- Allow marketing sync runs to record partial success when optional sections fail.
alter table if exists public.marketing_web_analytics_sync_runs
  drop constraint if exists marketing_web_analytics_sync_runs_status_check;

alter table if exists public.marketing_web_analytics_sync_runs
  add constraint marketing_web_analytics_sync_runs_status_check
  check (status in ('running', 'success', 'failed', 'partial'));
