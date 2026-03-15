-- Forward-only migration:
-- Ensure the primary admin profile exists and is active after auth cutover.

do $$
declare
  admin_email text := 'ianswain2@gmail.com';
  admin_id uuid;
begin
  select id into admin_id
  from auth.users
  where lower(email) = admin_email
  limit 1;

  if admin_id is null then
    raise notice 'Primary admin auth user (%) not found yet; invite/create user first, then re-run this statement manually.', admin_email;
    return;
  end if;

  insert into public.user_profiles (user_id, email, role, is_active, created_by, updated_by)
  values (admin_id, admin_email, 'admin', true, admin_id, admin_id)
  on conflict (user_id) do update
    set email = excluded.email,
        role = 'admin',
        is_active = true,
        updated_by = excluded.updated_by,
        updated_at = now();
end
$$;
