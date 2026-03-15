alter table public.ai_insight_events enable row level security;
alter table public.ai_recommendation_queue enable row level security;
alter table public.ai_briefings_daily enable row level security;

create policy ai_insight_events_select_authenticated
on public.ai_insight_events for select
using (auth.role() = 'authenticated');

create policy ai_insight_events_insert_service
on public.ai_insight_events for insert
with check (auth.role() = 'service_role');

create policy ai_insight_events_update_admin_or_service
on public.ai_insight_events for update
using (public.is_admin() or auth.role() = 'service_role')
with check (public.is_admin() or auth.role() = 'service_role');

create policy ai_recommendation_queue_select_authenticated
on public.ai_recommendation_queue for select
using (auth.role() = 'authenticated');

create policy ai_recommendation_queue_insert_service
on public.ai_recommendation_queue for insert
with check (auth.role() = 'service_role');

create policy ai_recommendation_queue_update_admin_or_service
on public.ai_recommendation_queue for update
using (public.is_admin() or auth.role() = 'service_role')
with check (public.is_admin() or auth.role() = 'service_role');

create policy ai_briefings_daily_select_authenticated
on public.ai_briefings_daily for select
using (auth.role() = 'authenticated');

create policy ai_briefings_daily_insert_service
on public.ai_briefings_daily for insert
with check (auth.role() = 'service_role');

create policy ai_briefings_daily_update_admin_or_service
on public.ai_briefings_daily for update
using (public.is_admin() or auth.role() = 'service_role')
with check (public.is_admin() or auth.role() = 'service_role');

