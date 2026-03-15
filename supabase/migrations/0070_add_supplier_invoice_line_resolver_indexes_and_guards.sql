-- Resolver-path indexes and safe integrity guards for supplier invoice imports.

create index if not exists idx_supplier_invoices_supplier_external_id
  on public.supplier_invoices(supplier_external_id);

create index if not exists idx_supplier_invoices_invoice_date
  on public.supplier_invoices(invoice_date);

create index if not exists idx_supplier_invoices_is_deleted
  on public.supplier_invoices(is_deleted);

create index if not exists idx_supplier_invoices_external_code
  on public.supplier_invoices(external_code);

create index if not exists idx_supplier_invoice_lines_supplier_invoice_external_id
  on public.supplier_invoice_lines(supplier_invoice_external_id);

create index if not exists idx_supplier_invoice_lines_itinerary_item_external_id
  on public.supplier_invoice_lines(itinerary_item_external_id);

create index if not exists idx_supplier_invoice_lines_itinerary_external_id
  on public.supplier_invoice_lines(itinerary_external_id);

create index if not exists idx_supplier_invoice_lines_supplier_external_id
  on public.supplier_invoice_lines(supplier_external_id);

create index if not exists idx_supplier_invoice_lines_supplier_invoice_id
  on public.supplier_invoice_lines(supplier_invoice_id);

create index if not exists idx_supplier_invoice_lines_itinerary_item_id
  on public.supplier_invoice_lines(itinerary_item_id);

create index if not exists idx_supplier_invoice_lines_itinerary_id
  on public.supplier_invoice_lines(itinerary_id);

create index if not exists idx_supplier_invoice_lines_supplier_id
  on public.supplier_invoice_lines(supplier_id);

create index if not exists idx_supplier_invoice_lines_due_date
  on public.supplier_invoice_lines(due_date);

create index if not exists idx_supplier_invoice_lines_is_deleted
  on public.supplier_invoice_lines(is_deleted);

do $$
begin
  if not exists (
    select 1
    from pg_constraint
    where conname = 'supplier_invoice_lines_parent_key_presence_check'
  ) then
    alter table public.supplier_invoice_lines
      add constraint supplier_invoice_lines_parent_key_presence_check
      check (
        supplier_invoice_id is not null
        or supplier_invoice_external_id is not null
      ) not valid;
  end if;
end;
$$;
