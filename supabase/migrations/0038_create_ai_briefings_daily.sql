create table if not exists public.ai_briefings_daily (
  id uuid primary key default gen_random_uuid(),
  briefing_date date not null unique,
  title text not null,
  summary text not null,
  highlights jsonb not null default '[]'::jsonb,
  top_actions jsonb not null default '[]'::jsonb,
  confidence numeric(5,4) not null default 0,
  evidence jsonb not null default '{}'::jsonb,
  generated_at timestamptz not null default now(),
  model_name text,
  model_tier text,
  tokens_used integer,
  latency_ms integer,
  run_id text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint ai_briefings_daily_confidence_check check (confidence >= 0 and confidence <= 1)
);

