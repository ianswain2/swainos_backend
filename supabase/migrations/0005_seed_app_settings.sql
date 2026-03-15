-- Seed default application settings

insert into public.app_settings (setting_key, setting_value, setting_type, description)
values
  ('salesforce_sync_interval_hours', '2'::jsonb, 'sync', 'Salesforce sync interval in hours'),
  ('quickbooks_sync_interval_hours', '24'::jsonb, 'sync', 'QuickBooks sync interval in hours'),
  ('fx_rate_sync_interval_minutes', '15'::jsonb, 'sync', 'FX rate sync interval in minutes'),
  ('alert_email_addresses', '[]'::jsonb, 'notification', 'Email addresses for sync failure alerts'),
  ('sync_failure_retry_count', '3'::jsonb, 'sync', 'Number of retries for failed sync jobs'),
  ('sync_failure_alert_threshold', '3'::jsonb, 'notification', 'Failures before alerting')
on conflict (setting_key) do nothing;
