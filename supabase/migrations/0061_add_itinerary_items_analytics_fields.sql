-- Forward-only migration:
-- Add itinerary_items analytics fields for destination, pricing, and lifecycle state tracking.

alter table if exists public.itinerary_items
add column if not exists full_service_name text,
add column if not exists unit_price numeric,
add column if not exists total_price numeric,
add column if not exists subtotal_price numeric,
add column if not exists subtotal_cost numeric,
add column if not exists gross_margin numeric,
add column if not exists profit_margin_percent numeric,
add column if not exists is_cancelled boolean,
add column if not exists cancelled_date date,
add column if not exists is_invoiced boolean,
add column if not exists is_deleted boolean,
add column if not exists voucher_title text,
add column if not exists destination_continent text;
