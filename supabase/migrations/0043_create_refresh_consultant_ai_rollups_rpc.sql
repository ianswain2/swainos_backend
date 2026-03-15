create or replace function public.refresh_consultant_ai_rollups_v1()
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
begin
  refresh materialized view public.mv_travel_consultant_profile_monthly;
  refresh materialized view public.mv_travel_consultant_funnel_monthly;
  refresh materialized view public.mv_travel_consultant_leaderboard_monthly;
  refresh materialized view public.mv_travel_consultant_compensation_monthly;

  refresh materialized view public.ai_context_travel_consultant_v1;
  refresh materialized view public.ai_context_itinerary_health_v1;
  refresh materialized view public.ai_context_command_center_v1;

  return jsonb_build_object(
    'status', 'ok',
    'refreshedAt', now(),
    'views', jsonb_build_array(
      'mv_travel_consultant_profile_monthly',
      'mv_travel_consultant_funnel_monthly',
      'mv_travel_consultant_leaderboard_monthly',
      'mv_travel_consultant_compensation_monthly',
      'ai_context_travel_consultant_v1',
      'ai_context_itinerary_health_v1',
      'ai_context_command_center_v1'
    )
  );
end;
$$;

revoke all on function public.refresh_consultant_ai_rollups_v1() from public;
grant execute on function public.refresh_consultant_ai_rollups_v1() to service_role;
