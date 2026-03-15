create index if not exists idx_ai_insight_events_domain
  on public.ai_insight_events(domain);

create index if not exists idx_ai_insight_events_severity
  on public.ai_insight_events(severity);

create index if not exists idx_ai_insight_events_status
  on public.ai_insight_events(status);

create index if not exists idx_ai_insight_events_created_at
  on public.ai_insight_events(created_at desc);

create index if not exists idx_ai_insight_events_entity_lookup
  on public.ai_insight_events(entity_type, entity_id, created_at desc);

create index if not exists idx_ai_insight_events_domain_status_recency
  on public.ai_insight_events(domain, status, created_at desc);

create index if not exists idx_ai_recommendation_queue_status_priority
  on public.ai_recommendation_queue(status, priority, created_at desc);

create index if not exists idx_ai_recommendation_queue_domain_status
  on public.ai_recommendation_queue(domain, status, created_at desc);

create index if not exists idx_ai_recommendation_queue_owner_status
  on public.ai_recommendation_queue(owner_user_id, status, created_at desc);

create index if not exists idx_ai_briefings_daily_briefing_date
  on public.ai_briefings_daily(briefing_date desc);

