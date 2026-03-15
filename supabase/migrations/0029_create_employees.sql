-- Create employees table for travel consultant analytics.

create table if not exists public.employees (
  id uuid primary key default gen_random_uuid(),
  external_id text not null unique,
  first_name text not null,
  last_name text not null,
  email text not null unique,
  salary numeric(12,2),
  commission_rate numeric(5,4) not null default 0.1500,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create index if not exists idx_employees_last_name
  on public.employees(last_name);

create index if not exists idx_employees_external_id
  on public.employees(external_id);

alter table public.employees enable row level security;

drop policy if exists employees_select_admin on public.employees;
create policy employees_select_admin
on public.employees for select
using (public.is_admin() or auth.role() = 'service_role');

drop policy if exists employees_crud_service on public.employees;
create policy employees_crud_service
on public.employees for all
using (auth.role() = 'service_role')
with check (auth.role() = 'service_role');

create or replace function public.set_updated_at()
returns trigger as $$
begin
  new.updated_at = now();
  return new;
end;
$$ language plpgsql;

drop trigger if exists tr_employees_set_updated_at on public.employees;
create trigger tr_employees_set_updated_at
before update on public.employees
for each row execute function public.set_updated_at();
