-- Forward-only migration:
-- Create invite-only auth access domain with role + module permission controls.

create table if not exists public.user_profiles (
  user_id uuid primary key references auth.users(id) on delete cascade,
  email text not null unique,
  role text not null check (role in ('admin', 'member')),
  is_active boolean not null default true,
  created_by uuid null references auth.users(id) on delete set null,
  updated_by uuid null references auth.users(id) on delete set null,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create table if not exists public.user_module_permissions (
  id uuid primary key default gen_random_uuid(),
  user_id uuid not null references public.user_profiles(user_id) on delete cascade,
  permission_key text not null,
  created_by uuid null references auth.users(id) on delete set null,
  created_at timestamptz not null default now(),
  unique (user_id, permission_key)
);

create index if not exists idx_user_profiles_role on public.user_profiles(role);
create index if not exists idx_user_profiles_is_active on public.user_profiles(is_active);
create index if not exists idx_user_module_permissions_user_id on public.user_module_permissions(user_id);
create index if not exists idx_user_module_permissions_permission_key on public.user_module_permissions(permission_key);

drop trigger if exists trg_user_profiles_updated_at on public.user_profiles;
create trigger trg_user_profiles_updated_at
before update on public.user_profiles
for each row
execute function public.set_updated_at();

create or replace function public.is_platform_admin()
returns boolean
language sql
stable
security definer
set search_path = pg_catalog, public
as $$
  select exists (
    select 1
    from public.user_profiles up
    where up.user_id = auth.uid()
      and up.role = 'admin'
      and up.is_active = true
  );
$$;

alter table public.user_profiles enable row level security;
alter table public.user_module_permissions enable row level security;

drop policy if exists user_profiles_select_self on public.user_profiles;
create policy user_profiles_select_self
on public.user_profiles for select
using (auth.uid() = user_id);

drop policy if exists user_profiles_select_admin on public.user_profiles;
create policy user_profiles_select_admin
on public.user_profiles for select
using (public.is_platform_admin() or auth.role() = 'service_role');

drop policy if exists user_profiles_manage_admin on public.user_profiles;
create policy user_profiles_manage_admin
on public.user_profiles for all
using (public.is_platform_admin() or auth.role() = 'service_role')
with check (public.is_platform_admin() or auth.role() = 'service_role');

drop policy if exists user_module_permissions_select_self on public.user_module_permissions;
create policy user_module_permissions_select_self
on public.user_module_permissions for select
using (
  exists (
    select 1
    from public.user_profiles up
    where up.user_id = auth.uid()
      and up.user_id = user_module_permissions.user_id
  )
);

drop policy if exists user_module_permissions_select_admin on public.user_module_permissions;
create policy user_module_permissions_select_admin
on public.user_module_permissions for select
using (public.is_platform_admin() or auth.role() = 'service_role');

drop policy if exists user_module_permissions_manage_admin on public.user_module_permissions;
create policy user_module_permissions_manage_admin
on public.user_module_permissions for all
using (public.is_platform_admin() or auth.role() = 'service_role')
with check (public.is_platform_admin() or auth.role() = 'service_role');

create or replace view public.user_access_summary_v1 as
select
  up.user_id,
  up.email,
  up.role,
  up.is_active,
  up.created_at,
  up.updated_at,
  coalesce(
    array_agg(ump.permission_key order by ump.permission_key)
      filter (where ump.permission_key is not null),
    '{}'::text[]
  ) as permission_keys
from public.user_profiles up
left join public.user_module_permissions ump on ump.user_id = up.user_id
group by up.user_id, up.email, up.role, up.is_active, up.created_at, up.updated_at;

alter view public.user_access_summary_v1 set (security_invoker = true);

-- Bootstrap initial admin profile if auth identity already exists.
do $$
declare
  admin_id uuid;
begin
  select id into admin_id
  from auth.users
  where lower(email) = 'ianswain2@gmail.com'
  limit 1;

  if admin_id is not null then
    insert into public.user_profiles (user_id, email, role, is_active, created_by, updated_by)
    values (admin_id, 'ianswain2@gmail.com', 'admin', true, admin_id, admin_id)
    on conflict (user_id) do update
      set email = excluded.email,
          role = 'admin',
          is_active = true,
          updated_by = excluded.updated_by,
          updated_at = now();
  end if;
end
$$;
