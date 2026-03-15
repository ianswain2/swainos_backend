-- Add resolver-path indexes for supplier_invoice_bookings and
-- align supplier_invoice_lines to booking-parent semantics.

create index if not exists idx_supplier_invoice_bookings_external_id
  on public.supplier_invoice_bookings(external_id);

create index if not exists idx_supplier_invoice_bookings_itinerary_external_id
  on public.supplier_invoice_bookings(itinerary_external_id);

create index if not exists idx_supplier_invoice_bookings_supplier_external_id
  on public.supplier_invoice_bookings(supplier_external_id);

create index if not exists idx_supplier_invoice_bookings_itinerary_id
  on public.supplier_invoice_bookings(itinerary_id);

create index if not exists idx_supplier_invoice_bookings_supplier_id
  on public.supplier_invoice_bookings(supplier_id);

create index if not exists idx_supplier_invoice_bookings_is_deleted
  on public.supplier_invoice_bookings(is_deleted);

alter table public.supplier_invoice_lines
  add column if not exists supplier_invoice_booking_id uuid,
  add column if not exists supplier_invoice_booking_external_id text;

do $$
begin
  if not exists (
    select 1
    from pg_constraint
    where conname = 'supplier_invoice_lines_supplier_invoice_booking_id_fkey'
  ) then
    alter table public.supplier_invoice_lines
      add constraint supplier_invoice_lines_supplier_invoice_booking_id_fkey
      foreign key (supplier_invoice_booking_id)
      references public.supplier_invoice_bookings(id) not valid;
  end if;
end;
$$;

create index if not exists idx_supplier_invoice_lines_supplier_invoice_booking_id
  on public.supplier_invoice_lines(supplier_invoice_booking_id);

create index if not exists idx_supplier_invoice_lines_supplier_invoice_booking_external_id
  on public.supplier_invoice_lines(supplier_invoice_booking_external_id);

do $$
begin
  if exists (
    select 1
    from pg_constraint
    where conname = 'supplier_invoice_lines_parent_key_presence_check'
  ) then
    alter table public.supplier_invoice_lines
      drop constraint supplier_invoice_lines_parent_key_presence_check;
  end if;
end;
$$;

do $$
begin
  alter table public.supplier_invoice_lines
    add constraint supplier_invoice_lines_parent_key_presence_check
    check (
      supplier_invoice_booking_id is not null
      or supplier_invoice_booking_external_id is not null
      or supplier_invoice_id is not null
      or supplier_invoice_external_id is not null
    ) not valid;
end;
$$;
