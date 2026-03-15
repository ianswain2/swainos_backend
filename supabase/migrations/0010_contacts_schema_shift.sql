-- Replace advisors with contacts and remove customers

-- Create contacts table
create table if not exists public.contacts (
  id uuid primary key default gen_random_uuid(),
  external_id text unique,
  contact_type text,
  agency_id uuid references public.agencies(id),
  full_name text,
  email text,
  phone text,
  mobile_phone text,
  address_country text,
  is_primary boolean default false,
  created_at timestamptz default now(),
  updated_at timestamptz default now()
);

-- Update itineraries to use primary_contact_id
alter table public.itineraries
  add column if not exists primary_contact_id uuid references public.contacts(id),
  add column if not exists primary_contact_type text;

alter table public.itineraries
  drop column if exists customer_id,
  drop column if exists advisor_id;

-- Drop obsolete tables
drop table if exists public.customers cascade;
drop table if exists public.advisors cascade;
