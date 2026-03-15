-- Persist run-level metrics for historical operations analysis.
-- Stores duration and output payload size on each data job run.

alter table if exists public.data_job_runs
  add column if not exists duration_seconds integer;

alter table if exists public.data_job_runs
  add column if not exists output_size_bytes integer;

do $$
begin
  if not exists (
    select 1
    from pg_constraint
    where conname = 'data_job_runs_duration_seconds_nonnegative'
  ) then
    alter table public.data_job_runs
      add constraint data_job_runs_duration_seconds_nonnegative
      check (duration_seconds is null or duration_seconds >= 0);
  end if;

  if not exists (
    select 1
    from pg_constraint
    where conname = 'data_job_runs_output_size_bytes_nonnegative'
  ) then
    alter table public.data_job_runs
      add constraint data_job_runs_output_size_bytes_nonnegative
      check (output_size_bytes is null or output_size_bytes >= 0);
  end if;
end $$;

update public.data_job_runs
set
  duration_seconds = case
    when started_at is not null and finished_at is not null and finished_at >= started_at
      then floor(extract(epoch from (finished_at - started_at)))::integer
    else duration_seconds
  end,
  output_size_bytes = coalesce(output_size_bytes, octet_length(coalesce(output, '{}'::jsonb)::text));
