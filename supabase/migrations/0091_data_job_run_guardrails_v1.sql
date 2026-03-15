-- Guardrails to prevent duplicate concurrent running jobs.
-- Also auto-closes legacy duplicate running rows before enforcing uniqueness.

with ranked_running as (
  select
    id,
    row_number() over (
      partition by job_id
      order by coalesce(started_at, created_at) desc, created_at desc, id desc
    ) as rn
  from public.data_job_runs
  where run_status = 'running'
)
update public.data_job_runs
set
  run_status = 'failed',
  finished_at = coalesce(finished_at, timezone('utc'::text, now())),
  error_code = coalesce(error_code, 'runner_timeout'),
  error_message = coalesce(
    error_message,
    'Auto-closed duplicate running run while applying single-running guardrail.'
  ),
  updated_at = timezone('utc'::text, now())
where id in (select id from ranked_running where rn > 1);

create unique index if not exists idx_data_job_runs_single_running_per_job
  on public.data_job_runs (job_id)
  where run_status = 'running';
