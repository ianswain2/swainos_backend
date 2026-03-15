-- Create supplier invoice bookings (Salesforce a29 object) as canonical
-- parent object for supplier invoice lines.

create table if not exists public.supplier_invoice_bookings (
  id uuid primary key default gen_random_uuid(),
  external_id text not null unique,
  booking_name text,
  balance_due numeric,
  commission_balance_due numeric,
  commission_due numeric,
  commission_invoiced numeric,
  cost_due numeric,
  cost_invoiced numeric,
  currency_code text,
  is_complete boolean default false,
  is_deleted boolean default false,
  created_at timestamptz,
  updated_at timestamptz,
  synced_at timestamptz,
  itinerary_external_id text,
  supplier_external_id text,
  itinerary_id uuid,
  supplier_id uuid
);

do $$
begin
  if not exists (
    select 1
    from pg_constraint
    where conname = 'supplier_invoice_bookings_external_id_not_blank_check'
  ) then
    alter table public.supplier_invoice_bookings
      add constraint supplier_invoice_bookings_external_id_not_blank_check
      check (btrim(external_id) <> '');
  end if;
end;
$$;

do $$
begin
  if not exists (
    select 1
    from pg_constraint
    where conname = 'supplier_invoice_bookings_itinerary_id_fkey'
  ) then
    alter table public.supplier_invoice_bookings
      add constraint supplier_invoice_bookings_itinerary_id_fkey
      foreign key (itinerary_id) references public.itineraries(id) not valid;
  end if;
end;
$$;

do $$
begin
  if not exists (
    select 1
    from pg_constraint
    where conname = 'supplier_invoice_bookings_supplier_id_fkey'
  ) then
    alter table public.supplier_invoice_bookings
      add constraint supplier_invoice_bookings_supplier_id_fkey
      foreign key (supplier_id) references public.suppliers(id) not valid;
  end if;
end;
$$;
