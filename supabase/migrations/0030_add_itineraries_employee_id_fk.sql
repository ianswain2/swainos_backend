-- Add canonical consultant ownership foreign key to itineraries.

alter table public.itineraries
  add column if not exists employee_id uuid;

alter table public.itineraries
  drop constraint if exists itineraries_employee_id_fkey;

alter table public.itineraries
  add constraint itineraries_employee_id_fkey
  foreign key (employee_id) references public.employees(id) on delete set null;

create index if not exists idx_itineraries_employee_id
  on public.itineraries(employee_id);
