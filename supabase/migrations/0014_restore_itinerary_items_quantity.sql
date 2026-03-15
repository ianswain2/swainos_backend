-- Restore itinerary_items.quantity
alter table if exists public.itinerary_items
add column if not exists quantity integer;
