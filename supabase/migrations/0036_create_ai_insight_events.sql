create table if not exists public.ai_insight_events (
  id uuid primary key default gen_random_uuid(),
  insight_type text not null,
  domain text not null,
  severity text not null,
  status text not null default 'new',
  entity_type text,
  entity_id text,
  title text not null,
  summary text not null,
  recommended_action text,
  priority integer not null default 3,
  confidence numeric(5,4) not null default 0,
  evidence jsonb not null default '{}'::jsonb,
  source_metrics jsonb not null default '{}'::jsonb,
  metadata jsonb not null default '{}'::jsonb,
  generated_at timestamptz not null default now(),
  model_name text,
  model_tier text,
  tokens_used integer,
  latency_ms integer,
  run_id text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint ai_insight_events_insight_type_check check (
    insight_type in ('briefing', 'anomaly', 'recommendation', 'forecast_narrative', 'coaching_signal')
  ),
  constraint ai_insight_events_domain_check check (
    domain in ('command_center', 'travel_consultant', 'itinerary', 'fx', 'destination', 'invoices', 'platform')
  ),
  constraint ai_insight_events_severity_check check (
    severity in ('low', 'medium', 'high', 'critical')
  ),
  constraint ai_insight_events_status_check check (
    status in ('new', 'acknowledged', 'in_progress', 'resolved', 'dismissed')
  ),
  constraint ai_insight_events_priority_check check (priority between 1 and 5),
  constraint ai_insight_events_confidence_check check (confidence >= 0 and confidence <= 1)
);

