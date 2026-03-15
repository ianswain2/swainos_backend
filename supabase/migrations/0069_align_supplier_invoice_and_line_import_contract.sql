-- Align supplier invoice + line tables to Salesforce import contract.
-- Additive only: preserve legacy columns consumed by existing APIs/views.

alter table public.supplier_invoices
  add column if not exists invoice_name text,
  add column if not exists supplier_external_id text,
  add column if not exists amount numeric,
  add column if not exists commission_amount numeric,
  add column if not exists commission_amount_rollup numeric,
  add column if not exists tax_total numeric,
  add column if not exists invoice_type text,
  add column if not exists brand_external_id text,
  add column if not exists external_code text,
  add column if not exists has_email_confirmation boolean,
  add column if not exists sent_to_external_date date,
  add column if not exists sent_to_external_system boolean,
  add column if not exists supplier_sequence text,
  add column if not exists is_deleted boolean default false;

-- Keep legacy invoice_number populated for compatibility with older readers.
update public.supplier_invoices
set invoice_number = coalesce(invoice_number, invoice_name)
where coalesce(invoice_number, '') = ''
  and invoice_name is not null;

alter table public.supplier_invoice_lines
  add column if not exists line_name text,
  add column if not exists item_name text,
  add column if not exists line_description text,
  add column if not exists booking_date date,
  add column if not exists due_date date,
  add column if not exists balance_due numeric,
  add column if not exists balance numeric,
  add column if not exists cost_due numeric,
  add column if not exists cost_invoiced numeric,
  add column if not exists original_cost_due numeric,
  add column if not exists commission_due numeric,
  add column if not exists commission_invoiced numeric,
  add column if not exists commission_balance_due numeric,
  add column if not exists payment_rule_type text,
  add column if not exists payment_rule_key text,
  add column if not exists adjustment_type text,
  add column if not exists custom_type text,
  add column if not exists is_cancelled boolean default false,
  add column if not exists is_complete boolean default false,
  add column if not exists is_credit_line boolean default false,
  add column if not exists is_manual_adjustment boolean default false,
  add column if not exists skip_processing boolean default false,
  add column if not exists comment text,
  add column if not exists is_deleted boolean default false,
  add column if not exists synced_at timestamptz,
  add column if not exists supplier_invoice_external_id text,
  add column if not exists itinerary_item_external_id text,
  add column if not exists itinerary_external_id text,
  add column if not exists supplier_external_id text,
  add column if not exists itinerary_item_id uuid,
  add column if not exists supplier_id uuid;

do $$
begin
  if not exists (
    select 1
    from pg_constraint
    where conname = 'supplier_invoice_lines_itinerary_item_id_fkey'
  ) then
    alter table public.supplier_invoice_lines
      add constraint supplier_invoice_lines_itinerary_item_id_fkey
      foreign key (itinerary_item_id) references public.itinerary_items(id) not valid;
  end if;
end;
$$;

do $$
begin
  if not exists (
    select 1
    from pg_constraint
    where conname = 'supplier_invoice_lines_supplier_id_fkey'
  ) then
    alter table public.supplier_invoice_lines
      add constraint supplier_invoice_lines_supplier_id_fkey
      foreign key (supplier_id) references public.suppliers(id) not valid;
  end if;
end;
$$;

-- Keep legacy line-level compatibility fields hydrated when missing.
update public.supplier_invoice_lines
set description = coalesce(description, line_description),
    service_date = coalesce(service_date, booking_date),
    line_amount = coalesce(line_amount, cost_due, balance_due, balance)
where (description is null and line_description is not null)
   or (service_date is null and booking_date is not null)
   or (line_amount is null and coalesce(cost_due, balance_due, balance) is not null);
