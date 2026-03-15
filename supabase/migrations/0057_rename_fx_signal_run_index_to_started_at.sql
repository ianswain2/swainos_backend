-- Normalize FX signal run index naming to match indexed column.
-- Applies safely after 0055.

do $$
begin
  if exists (
    select 1
    from pg_class
    where relkind = 'i'
      and relname = 'idx_fx_signal_runs_generated_at'
  ) and not exists (
    select 1
    from pg_class
    where relkind = 'i'
      and relname = 'idx_fx_signal_runs_started_at'
  ) then
    alter index public.idx_fx_signal_runs_generated_at
      rename to idx_fx_signal_runs_started_at;
  elsif exists (
    select 1
    from pg_class
    where relkind = 'i'
      and relname = 'idx_fx_signal_runs_generated_at'
  ) and exists (
    select 1
    from pg_class
    where relkind = 'i'
      and relname = 'idx_fx_signal_runs_started_at'
  ) then
    drop index if exists public.idx_fx_signal_runs_generated_at;
  elsif not exists (
    select 1
    from pg_class
    where relkind = 'i'
      and relname = 'idx_fx_signal_runs_started_at'
  ) then
    create index if not exists idx_fx_signal_runs_started_at
      on public.fx_signal_runs(started_at desc);
  end if;
end;
$$;
