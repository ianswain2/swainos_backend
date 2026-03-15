-- Validate supplier-invoice domain constraints after strict resolver imports.
-- Safe to run multiple times; each validation is gated by constraint existence.

do $$
begin
  if exists (
    select 1
    from pg_constraint
    where conname = 'supplier_invoice_bookings_itinerary_id_fkey'
  ) then
    alter table public.supplier_invoice_bookings
      validate constraint supplier_invoice_bookings_itinerary_id_fkey;
  end if;
end;
$$;

do $$
begin
  if exists (
    select 1
    from pg_constraint
    where conname = 'supplier_invoice_bookings_supplier_id_fkey'
  ) then
    alter table public.supplier_invoice_bookings
      validate constraint supplier_invoice_bookings_supplier_id_fkey;
  end if;
end;
$$;

do $$
begin
  if exists (
    select 1
    from pg_constraint
    where conname = 'supplier_invoice_lines_itinerary_item_id_fkey'
  ) then
    alter table public.supplier_invoice_lines
      validate constraint supplier_invoice_lines_itinerary_item_id_fkey;
  end if;
end;
$$;

do $$
begin
  if exists (
    select 1
    from pg_constraint
    where conname = 'supplier_invoice_lines_supplier_id_fkey'
  ) then
    alter table public.supplier_invoice_lines
      validate constraint supplier_invoice_lines_supplier_id_fkey;
  end if;
end;
$$;

do $$
begin
  if exists (
    select 1
    from pg_constraint
    where conname = 'supplier_invoice_lines_supplier_invoice_booking_id_fkey'
  ) then
    alter table public.supplier_invoice_lines
      validate constraint supplier_invoice_lines_supplier_invoice_booking_id_fkey;
  end if;
end;
$$;

do $$
begin
  if exists (
    select 1
    from pg_constraint
    where conname = 'supplier_invoice_lines_parent_key_presence_check'
  ) then
    alter table public.supplier_invoice_lines
      validate constraint supplier_invoice_lines_parent_key_presence_check;
  end if;
end;
$$;
