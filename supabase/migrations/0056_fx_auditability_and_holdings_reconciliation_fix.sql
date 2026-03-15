-- FX auditability and holdings reconciliation hardening.
-- Applies safely after 0055.

-- 1) Preserve intelligence run lineage by de-duplicating only within a run.
drop index if exists public.idx_fx_intelligence_items_source_dedupe;

create unique index if not exists idx_fx_intelligence_items_run_source_dedupe
  on public.fx_intelligence_items(run_id, source_url);

-- 2) Ensure holdings reconcile to zero when a target currency has no transactions.
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

  if p_currency_code is not null and not exists (
    select 1
    from public.fx_transactions
    where currency_code = p_currency_code
  ) then
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
    values (
      p_currency_code,
      0,
      null,
      0,
      0,
      null,
      timezone('utc', now()),
      timezone('utc', now())
    )
    on conflict (currency_code) do update
    set
      balance_amount = 0,
      avg_purchase_rate = null,
      total_purchased = 0,
      total_spent = 0,
      last_transaction_date = null,
      last_reconciled_at = timezone('utc', now()),
      updated_at = timezone('utc', now());
  end if;
end;
$$;
