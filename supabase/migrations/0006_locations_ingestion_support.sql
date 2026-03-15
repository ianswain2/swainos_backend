-- Support ingesting Salesforce/Kaptio locations into canonical locations table

create table if not exists public.locations_raw (
  id uuid primary key default gen_random_uuid(),
  country_code text,
  country_name text,
  region_name text,
  city_name text,
  latitude numeric,
  longitude numeric,
  timezone text,
  is_primary_destination boolean default false,
  source text default 'salesforce_kaptio',
  ingested_at timestamptz default now()
);

create table if not exists public.location_mappings (
  id uuid primary key default gen_random_uuid(),
  raw_location_id uuid references public.locations_raw(id),
  location_id uuid references public.locations(id),
  created_at timestamptz default now()
);

create unique index if not exists uq_location_mappings_raw on public.location_mappings(raw_location_id);
create unique index if not exists uq_locations_country_region_city
  on public.locations(country_code, region_name, city_name);

create or replace function public.upsert_location_from_raw(raw_id uuid)
returns uuid
language plpgsql
as $$
declare
  raw_row record;
  normalized_country_name text;
  normalized_region text;
  normalized_city text;
  resolved_location_id uuid;
begin
  select * into raw_row
  from public.locations_raw
  where id = raw_id;

  if not found then
    return null;
  end if;

  normalized_country_name := nullif(trim(raw_row.country_name), '');
  normalized_region := coalesce(nullif(trim(raw_row.region_name), ''), normalized_country_name);
  normalized_city := coalesce(nullif(trim(raw_row.city_name), ''), normalized_country_name);

  insert into public.locations (
    country_code,
    country_name,
    region_name,
    city_name,
    latitude,
    longitude,
    timezone,
    is_primary_destination
  ) values (
    raw_row.country_code,
    normalized_country_name,
    normalized_region,
    normalized_city,
    raw_row.latitude,
    raw_row.longitude,
    raw_row.timezone,
    coalesce(raw_row.is_primary_destination, false)
  )
  on conflict (country_code, region_name, city_name)
  do update set
    latitude = coalesce(excluded.latitude, public.locations.latitude),
    longitude = coalesce(excluded.longitude, public.locations.longitude),
    timezone = coalesce(excluded.timezone, public.locations.timezone),
    is_primary_destination = public.locations.is_primary_destination or excluded.is_primary_destination
  returning id into resolved_location_id;

  insert into public.location_mappings (raw_location_id, location_id)
  values (raw_id, resolved_location_id)
  on conflict (raw_location_id) do nothing;

  return resolved_location_id;
end;
$$;

create or replace function public.upsert_locations_from_raw()
returns integer
language plpgsql
as $$
declare
  raw_record record;
  processed_count integer := 0;
begin
  for raw_record in select id from public.locations_raw loop
    perform public.upsert_location_from_raw(raw_record.id);
    processed_count := processed_count + 1;
  end loop;

  return processed_count;
end;
$$;

-- RLS for ingestion tables
alter table public.locations_raw enable row level security;
alter table public.location_mappings enable row level security;

create policy locations_raw_select_authenticated
on public.locations_raw for select
using (auth.role() = 'authenticated');

create policy locations_raw_insert_service
on public.locations_raw for insert
with check (auth.role() = 'service_role');

create policy location_mappings_select_authenticated
on public.location_mappings for select
using (auth.role() = 'authenticated');

create policy location_mappings_insert_service
on public.location_mappings for insert
with check (auth.role() = 'service_role');

create policy locations_insert_service
on public.locations for insert
with check (auth.role() = 'service_role');

create policy locations_update_service
on public.locations for update
using (auth.role() = 'service_role')
with check (auth.role() = 'service_role');
