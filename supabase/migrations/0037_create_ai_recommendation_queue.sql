create table if not exists public.ai_recommendation_queue (
  id uuid primary key default gen_random_uuid(),
  insight_event_id uuid references public.ai_insight_events(id) on delete set null,
  domain text not null,
  status text not null default 'new',
  entity_type text,
  entity_id text,
  title text not null,
  summary text not null,
  recommended_action text not null,
  priority integer not null default 3,
  confidence numeric(5,4) not null default 0,
  owner_user_id uuid references public.app_users(id),
  due_date date,
  resolution_note text,
  evidence jsonb not null default '{}'::jsonb,
  generated_at timestamptz not null default now(),
  model_name text,
  model_tier text,
  tokens_used integer,
  latency_ms integer,
  run_id text,
  completed_at timestamptz,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint ai_recommendation_queue_domain_check check (
    domain in ('command_center', 'travel_consultant', 'itinerary', 'fx', 'destination', 'invoices', 'platform')
  ),
  constraint ai_recommendation_queue_status_check check (
    status in ('new', 'acknowledged', 'in_progress', 'resolved', 'dismissed')
  ),
  constraint ai_recommendation_queue_priority_check check (priority between 1 and 5),
  constraint ai_recommendation_queue_confidence_check check (confidence >= 0 and confidence <= 1)
);

