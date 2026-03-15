-- Remove itinerary_items.quantity (not meaningful for travel services)
alter table if exists public.itinerary_items
drop column if exists quantity;
