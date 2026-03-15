-- Prevent recursive trigger execution when ledger rollup recalculation updates
-- the same fx_transactions rows that fired the trigger.

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
  -- Recalculation updates fx_transactions.balance_after, which should not
  -- recursively trigger another rollup pass.
  if pg_trigger_depth() > 1 then
    if tg_op = 'DELETE' then
      return old;
    end if;
    return new;
  end if;

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
