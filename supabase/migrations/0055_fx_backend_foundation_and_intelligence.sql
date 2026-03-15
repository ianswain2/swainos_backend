-- FX backend foundation: de-dup constraints, run auditability, intelligence persistence,
-- ledger integrity automation, invoice pressure view, and refresh RPC support.

-- 1) Rates de-dup and index coverage.
with duplicate_rates as (
  select
    id,
    row_number() over (
      partition by coalesce(currency_pair, ''), rate_timestamp, coalesce(source, '')
      order by created_at desc, id desc
    ) as rn
  from public.fx_rates
)
delete from public.fx_rates r
using duplicate_rates d
where r.id = d.id
  and d.rn > 1;

create unique index if not exists idx_fx_rates_pair_timestamp_source_unique
  on public.fx_rates(currency_pair, rate_timestamp, source);

create index if not exists idx_fx_rates_timestamp
  on public.fx_rates(rate_timestamp desc);

-- 2) Signal run audit table.
create table if not exists public.fx_signal_runs (
  id uuid primary key default gen_random_uuid(),
  run_type text not null,
  status text not null,
  started_at timestamptz not null default timezone('utc', now()),
  completed_at timestamptz null,
  rates_source text null,
  target_currencies text[] not null default '{}',
  records_processed integer not null default 0,
  signals_generated integer not null default 0,
  model_name text null,
  model_tier text null,
  calculation_version text not null default 'v1',
  error_message text null,
  metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default timezone('utc', now()),
  updated_at timestamptz not null default timezone('utc', now())
);

do $$
begin
  if not exists (
    select 1
    from pg_constraint
    where conname = 'chk_fx_signal_runs_type'
      and conrelid = 'public.fx_signal_runs'::regclass
  ) then
    alter table public.fx_signal_runs
      add constraint chk_fx_signal_runs_type
      check (run_type in ('scheduled', 'manual', 'on_demand'));
  end if;
end;
$$;

do $$
begin
  if not exists (
    select 1
    from pg_constraint
    where conname = 'chk_fx_signal_runs_status'
      and conrelid = 'public.fx_signal_runs'::regclass
  ) then
    alter table public.fx_signal_runs
      add constraint chk_fx_signal_runs_status
      check (status in ('running', 'success', 'failed', 'partial', 'skipped'));
  end if;
end;
$$;

create index if not exists idx_fx_signal_runs_generated_at
  on public.fx_signal_runs(started_at desc);

-- 3) Signal table enhancements for explainability and lineage.
alter table public.fx_signals
  add column if not exists run_id uuid null references public.fx_signal_runs(id) on delete set null,
  add column if not exists confidence numeric(5, 4) null,
  add column if not exists reason_summary text null,
  add column if not exists trend_tags text[] not null default '{}',
  add column if not exists source_links jsonb not null default '[]'::jsonb,
  add column if not exists exposure_30d_amount numeric not null default 0,
  add column if not exists invoice_pressure_30d numeric not null default 0,
  add column if not exists invoice_pressure_60d numeric not null default 0,
  add column if not exists invoice_pressure_90d numeric not null default 0,
  add column if not exists metadata jsonb not null default '{}'::jsonb,
  add column if not exists updated_at timestamptz not null default timezone('utc', now());

do $$
begin
  if not exists (
    select 1
    from pg_constraint
    where conname = 'chk_fx_signals_type'
      and conrelid = 'public.fx_signals'::regclass
  ) then
    alter table public.fx_signals
      add constraint chk_fx_signals_type
      check (signal_type is null or signal_type in ('buy_now', 'wait'));
  end if;
end;
$$;

do $$
begin
  if not exists (
    select 1
    from pg_constraint
    where conname = 'chk_fx_signals_strength'
      and conrelid = 'public.fx_signals'::regclass
  ) then
    alter table public.fx_signals
      add constraint chk_fx_signals_strength
      check (signal_strength is null or signal_strength in ('low', 'medium', 'high'));
  end if;
end;
$$;

do $$
begin
  if not exists (
    select 1
    from pg_constraint
    where conname = 'chk_fx_signals_confidence_range'
      and conrelid = 'public.fx_signals'::regclass
  ) then
    alter table public.fx_signals
      add constraint chk_fx_signals_confidence_range
      check (confidence is null or (confidence >= 0 and confidence <= 1));
  end if;
end;
$$;

create index if not exists idx_fx_signals_currency_generated_at
  on public.fx_signals(currency_code, generated_at desc);

-- 4) Ledger consistency guardrails.
update public.fx_transactions
set transaction_type = upper(trim(coalesce(transaction_type, 'ADJUSTMENT')));

update public.fx_transactions
set transaction_type = 'ADJUSTMENT'
where transaction_type not in ('BUY', 'SPEND', 'ADJUSTMENT');

do $$
begin
  if not exists (
    select 1
    from pg_constraint
    where conname = 'chk_fx_transactions_type_v1'
      and conrelid = 'public.fx_transactions'::regclass
  ) then
    alter table public.fx_transactions
      add constraint chk_fx_transactions_type_v1
      check (transaction_type in ('BUY', 'SPEND', 'ADJUSTMENT'));
  end if;
end;
$$;

create index if not exists idx_fx_holdings_currency
  on public.fx_holdings(currency_code);

create or replace function public.recalculate_fx_transaction_balances_v1(p_currency_code text)
returns void
language plpgsql
security definer
set search_path = public
as $$
begin
  with ordered as (
    select
      id,
      sum(coalesce(amount, 0)) over (
        order by transaction_date asc, created_at asc, id asc
      ) as running_balance
    from public.fx_transactions
    where currency_code = p_currency_code
  )
  update public.fx_transactions t
  set
    balance_after = ordered.running_balance,
    updated_at = timezone('utc', now())
  from ordered
  where t.id = ordered.id;
end;
$$;

create or replace function public.recalculate_fx_holdings_v1(p_currency_code text default null)
returns void
language plpgsql
security definer
set search_path = public
as $$
begin
  with aggregated as (
    select
      currency_code,
      sum(coalesce(amount, 0)) as balance_amount,
      sum(case when transaction_type = 'BUY' and amount > 0 then amount else 0 end) as total_purchased,
      sum(case when transaction_type = 'SPEND' then abs(amount) else 0 end) as total_spent,
      case
        when sum(case when transaction_type = 'BUY' and amount > 0 then amount else 0 end) > 0 then
          sum(case when transaction_type = 'BUY' and amount > 0 then amount * coalesce(exchange_rate, 0) else 0 end)
          / sum(case when transaction_type = 'BUY' and amount > 0 then amount else 0 end)
        else null
      end as avg_purchase_rate,
      max(transaction_date) as last_transaction_date
    from public.fx_transactions
    where p_currency_code is null or currency_code = p_currency_code
    group by currency_code
  )
  insert into public.fx_holdings (
    currency_code,
    balance_amount,
    avg_purchase_rate,
    total_purchased,
    total_spent,
    last_transaction_date,
    last_reconciled_at,
    updated_at
  )
  select
    a.currency_code,
    a.balance_amount,
    a.avg_purchase_rate,
    a.total_purchased,
    a.total_spent,
    a.last_transaction_date,
    timezone('utc', now()),
    timezone('utc', now())
  from aggregated a
  on conflict (currency_code) do update
  set
    balance_amount = excluded.balance_amount,
    avg_purchase_rate = excluded.avg_purchase_rate,
    total_purchased = excluded.total_purchased,
    total_spent = excluded.total_spent,
    last_transaction_date = excluded.last_transaction_date,
    last_reconciled_at = excluded.last_reconciled_at,
    updated_at = excluded.updated_at;
end;
$$;

create or replace function public.sync_fx_ledger_rollups_v1()
returns trigger
language plpgsql
security definer
set search_path = public
as $$
declare
  current_currency text;
  previous_currency text;
begin
  if tg_op in ('INSERT', 'UPDATE') then
    current_currency := new.currency_code;
  end if;
  if tg_op in ('UPDATE', 'DELETE') then
    previous_currency := old.currency_code;
  end if;

  if previous_currency is not null then
    perform public.recalculate_fx_transaction_balances_v1(previous_currency);
    perform public.recalculate_fx_holdings_v1(previous_currency);
  end if;

  if current_currency is not null and current_currency is distinct from previous_currency then
    perform public.recalculate_fx_transaction_balances_v1(current_currency);
    perform public.recalculate_fx_holdings_v1(current_currency);
  end if;

  if tg_op = 'DELETE' then
    return old;
  end if;
  return new;
end;
$$;

drop trigger if exists tr_fx_transactions_sync_rollups on public.fx_transactions;
create trigger tr_fx_transactions_sync_rollups
after insert or update or delete on public.fx_transactions
for each row
execute function public.sync_fx_ledger_rollups_v1();

-- 5) Dedicated macro/news intelligence persistence.
create table if not exists public.fx_intelligence_runs (
  id uuid primary key default gen_random_uuid(),
  run_type text not null,
  status text not null,
  started_at timestamptz not null default timezone('utc', now()),
  completed_at timestamptz null,
  source_count integer not null default 0,
  model_name text null,
  model_tier text null,
  error_message text null,
  metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default timezone('utc', now()),
  updated_at timestamptz not null default timezone('utc', now())
);

do $$
begin
  if not exists (
    select 1
    from pg_constraint
    where conname = 'chk_fx_intelligence_runs_type'
      and conrelid = 'public.fx_intelligence_runs'::regclass
  ) then
    alter table public.fx_intelligence_runs
      add constraint chk_fx_intelligence_runs_type
      check (run_type in ('daily', 'on_demand'));
  end if;
end;
$$;

do $$
begin
  if not exists (
    select 1
    from pg_constraint
    where conname = 'chk_fx_intelligence_runs_status'
      and conrelid = 'public.fx_intelligence_runs'::regclass
  ) then
    alter table public.fx_intelligence_runs
      add constraint chk_fx_intelligence_runs_status
      check (status in ('running', 'success', 'failed', 'partial', 'skipped'));
  end if;
end;
$$;

create index if not exists idx_fx_intelligence_runs_started_at
  on public.fx_intelligence_runs(started_at desc);

create table if not exists public.fx_intelligence_items (
  id uuid primary key default gen_random_uuid(),
  run_id uuid not null references public.fx_intelligence_runs(id) on delete cascade,
  currency_code text not null,
  source_type text not null default 'news',
  source_title text not null,
  source_url text not null,
  source_publisher text null,
  source_credibility_score numeric(5, 4) null,
  published_at timestamptz null,
  risk_direction text not null default 'neutral',
  confidence numeric(5, 4) null,
  trend_tags text[] not null default '{}',
  summary text not null,
  raw_payload jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default timezone('utc', now())
);

do $$
begin
  if not exists (
    select 1
    from pg_constraint
    where conname = 'chk_fx_intelligence_items_direction'
      and conrelid = 'public.fx_intelligence_items'::regclass
  ) then
    alter table public.fx_intelligence_items
      add constraint chk_fx_intelligence_items_direction
      check (risk_direction in ('bullish', 'bearish', 'neutral', 'mixed'));
  end if;
end;
$$;

do $$
begin
  if not exists (
    select 1
    from pg_constraint
    where conname = 'chk_fx_intelligence_items_confidence'
      and conrelid = 'public.fx_intelligence_items'::regclass
  ) then
    alter table public.fx_intelligence_items
      add constraint chk_fx_intelligence_items_confidence
      check (confidence is null or (confidence >= 0 and confidence <= 1));
  end if;
end;
$$;

do $$
begin
  if not exists (
    select 1
    from pg_constraint
    where conname = 'chk_fx_intelligence_items_credibility'
      and conrelid = 'public.fx_intelligence_items'::regclass
  ) then
    alter table public.fx_intelligence_items
      add constraint chk_fx_intelligence_items_credibility
      check (source_credibility_score is null or (source_credibility_score >= 0 and source_credibility_score <= 1));
  end if;
end;
$$;

create index if not exists idx_fx_intelligence_items_currency_published
  on public.fx_intelligence_items(currency_code, published_at desc);

create index if not exists idx_fx_intelligence_items_run_id
  on public.fx_intelligence_items(run_id);

create unique index if not exists idx_fx_intelligence_items_source_dedupe
  on public.fx_intelligence_items(currency_code, source_url);

-- 6) Invoice pressure view for due-date weighting.
create or replace view public.fx_invoice_pressure_v1 as
select
  si.currency_code,
  sum(case when si.due_date <= current_date + interval '7 days' then coalesce(si.total_amount, 0) else 0 end) as due_7d_amount,
  sum(case when si.due_date <= current_date + interval '30 days' then coalesce(si.total_amount, 0) else 0 end) as due_30d_amount,
  sum(case when si.due_date <= current_date + interval '60 days' then coalesce(si.total_amount, 0) else 0 end) as due_60d_amount,
  sum(case when si.due_date <= current_date + interval '90 days' then coalesce(si.total_amount, 0) else 0 end) as due_90d_amount,
  count(*) filter (where si.due_date <= current_date + interval '30 days')::integer as invoices_due_30d_count,
  min(si.due_date) filter (where si.due_date >= current_date) as next_due_date
from public.supplier_invoices si
where coalesce(si.payment_status, '') <> 'Paid'
  and si.currency_code in ('AUD', 'NZD', 'ZAR')
group by si.currency_code;

grant select on public.fx_invoice_pressure_v1 to authenticated;
grant select on public.fx_invoice_pressure_v1 to service_role;

-- 7) Exposure refresh RPC to support deterministic run orchestration.
create or replace function public.refresh_fx_exposure_v1()
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
declare
  refreshed_at timestamptz := timezone('utc', now());
begin
  refresh materialized view public.mv_fx_exposure;
  return jsonb_build_object(
    'status', 'ok',
    'refreshed_at', refreshed_at
  );
end;
$$;

-- 8) RLS and grants for new tables.
grant select on public.fx_signal_runs to authenticated;
grant all on public.fx_signal_runs to service_role;
grant select on public.fx_intelligence_runs to authenticated;
grant all on public.fx_intelligence_runs to service_role;
grant select on public.fx_intelligence_items to authenticated;
grant all on public.fx_intelligence_items to service_role;

alter table public.fx_signal_runs enable row level security;
alter table public.fx_intelligence_runs enable row level security;
alter table public.fx_intelligence_items enable row level security;

drop policy if exists fx_signal_runs_select_authenticated on public.fx_signal_runs;
drop policy if exists fx_signal_runs_service_write on public.fx_signal_runs;
drop policy if exists fx_intelligence_runs_select_authenticated on public.fx_intelligence_runs;
drop policy if exists fx_intelligence_runs_service_write on public.fx_intelligence_runs;
drop policy if exists fx_intelligence_items_select_authenticated on public.fx_intelligence_items;
drop policy if exists fx_intelligence_items_service_write on public.fx_intelligence_items;
drop policy if exists sync_logs_update_service on public.sync_logs;

create policy fx_signal_runs_select_authenticated
on public.fx_signal_runs for select
using (auth.role() = 'authenticated');

create policy fx_signal_runs_service_write
on public.fx_signal_runs for all
using (auth.role() = 'service_role')
with check (auth.role() = 'service_role');

create policy fx_intelligence_runs_select_authenticated
on public.fx_intelligence_runs for select
using (auth.role() = 'authenticated');

create policy fx_intelligence_runs_service_write
on public.fx_intelligence_runs for all
using (auth.role() = 'service_role')
with check (auth.role() = 'service_role');

create policy fx_intelligence_items_select_authenticated
on public.fx_intelligence_items for select
using (auth.role() = 'authenticated');

create policy fx_intelligence_items_service_write
on public.fx_intelligence_items for all
using (auth.role() = 'service_role')
with check (auth.role() = 'service_role');

create policy sync_logs_update_service
on public.sync_logs for update
using (auth.role() = 'service_role')
with check (auth.role() = 'service_role');
