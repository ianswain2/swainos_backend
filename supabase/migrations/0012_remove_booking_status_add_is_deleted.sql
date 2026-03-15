-- Replace booking_status with is_deleted flag from source system

alter table public.bookings
  add column if not exists is_deleted boolean default false;

update public.bookings
set is_deleted = false
where is_deleted is null;

drop index if exists idx_bookings_booking_status;

alter table public.bookings
  drop column if exists booking_status;
