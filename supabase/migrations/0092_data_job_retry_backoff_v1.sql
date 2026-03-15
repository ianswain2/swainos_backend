-- Per-job minimum retry backoff after failed runs.
-- Prevents failed recurring jobs from retrying every scheduler tick.

alter table if exists public.data_jobs
  add column if not exists retry_backoff_minutes integer not null default 30;

do $$
begin
  if not exists (
    select 1
    from pg_constraint
    where conname = 'data_jobs_retry_backoff_minutes_valid'
  ) then
    alter table public.data_jobs
      add constraint data_jobs_retry_backoff_minutes_valid
      check (retry_backoff_minutes >= 0 and retry_backoff_minutes <= 10080);
  end if;
end $$;

update public.data_jobs
set retry_backoff_minutes = case
  when schedule_mode = 'recurring' then 30
  else 0
end;
