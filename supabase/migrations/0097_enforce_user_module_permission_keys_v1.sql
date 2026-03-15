-- Forward-only migration:
-- Enforce canonical permission keys at the database boundary.

alter table public.user_module_permissions
drop constraint if exists user_module_permissions_permission_key_check;

alter table public.user_module_permissions
add constraint user_module_permissions_permission_key_check
check (
  permission_key in (
    'command_center',
    'ai_insights',
    'itinerary_forecast',
    'itinerary_actuals',
    'destination',
    'travel_consultant',
    'travel_agencies',
    'marketing_web_analytics',
    'search_console_insights',
    'cash_flow',
    'debt_service',
    'fx_command',
    'operations',
    'settings_job_controls',
    'settings_run_logs',
    'settings_user_access'
  )
);
