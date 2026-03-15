-- Reduce public API exposure on analytics materialized views.
-- Conservative pass: remove anon/public read access, retain authenticated + service_role.

do $$
declare
  object_name text;
  target_object regclass;
  materialized_view_names text[] := array[
    'mv_itinerary_revenue_monthly',
    'mv_itinerary_revenue_weekly',
    'mv_monthly_revenue',
    'mv_itinerary_consortia_actuals_monthly',
    'mv_rolling_metrics',
    'mv_fx_exposure',
    'mv_itinerary_trade_agency_actuals_monthly',
    'mv_active_travelers',
    'mv_cash_flow_forecast',
    'mv_itinerary_lead_flow_monthly',
    'ai_context_consultant_benchmarks_v1',
    'mv_itinerary_trends',
    'ai_context_itinerary_health_v1',
    'mv_itinerary_status_trends',
    'ai_context_command_center_v1',
    'mv_itinerary_pipeline_stages',
    'ai_context_company_metrics_v1',
    'mv_itinerary_deposit_monthly',
    'mv_itinerary_consortia_monthly',
    'mv_itinerary_destination_booked_monthly',
    'mv_itinerary_trade_agency_monthly',
    'mv_travel_consultant_funnel_monthly',
    'mv_travel_consultant_profile_monthly',
    'mv_travel_consultant_leaderboard_monthly',
    'mv_travel_consultant_compensation_monthly',
    'ai_context_travel_consultant_v1'
  ];
begin
  foreach object_name in array materialized_view_names loop
    target_object := to_regclass(format('public.%I', object_name));

    if target_object is not null then
      execute format('revoke select on table %s from anon', target_object);
      execute format('revoke select on table %s from public', target_object);

      execute format('grant select on table %s to authenticated', target_object);
      execute format('grant select on table %s to service_role', target_object);
    end if;
  end loop;
end
$$;
