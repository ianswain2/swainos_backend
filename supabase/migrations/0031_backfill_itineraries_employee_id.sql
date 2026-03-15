-- Backfill itineraries.employee_id from owner_external_id -> employees.external_id.
-- Prerequisite: employees table should be populated before running this migration.

do $$
begin
  if (select count(*) from public.employees) = 0 then
    raise warning 'employees table is empty; itinerary employee backfill will update 0 rows';
  end if;
end $$;

update public.itineraries i
set
  employee_id = e.id,
  updated_at = now()
from public.employees e
where i.employee_id is null
  and i.owner_external_id is not null
  and i.owner_external_id = e.external_id;
