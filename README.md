# SwainOS Backend

## Overview
FastAPI backend for SwainOS analytics, travel trade intelligence, FX workflows, and AI insights.

## Architecture
- `src/api`: route handlers
- `src/services`: business logic
- `src/repositories`: Supabase data access
- `src/schemas`: request/response contracts
- `src/analytics`: deterministic analytics helpers
- `src/core`: config/errors/Supabase client
- `src/shared`: response/time utilities
- `scripts`: upsert and refresh operations

## API Prefix
Default prefix: `/api/v1`

## Endpoint Families
- Health
- Cash flow / deposits / payments-out
- Booking forecasts / itinerary trends / itinerary lead flow
- Itinerary revenue (outlook, deposits, conversion, channels, actuals)
- Travel consultant / travel agents / travel agencies / travel trade search
- FX rates / exposure / signals / holdings / transactions / intelligence / invoice pressure
- Marketing web analytics (overview / search / ai-insights / health / sync)
- AI insights (briefing/feed/recommendations/history/entities/run)

## Local Development
1. Create `.env` in repo root.
2. Configure `SUPABASE_URL` and either service-role or anon key.
3. Install backend with dev tooling:
   - `cd /Users/ianswain/Desktop/SwainOS_BackEnd`
   - `python3 -m pip install -e ".[dev]"`
4. Start API:
   - `cd /Users/ianswain/Desktop/SwainOS_BackEnd`
   - `uvicorn src.main:app --reload`
5. Verify:
   - `http://127.0.0.1:8000/api/v1/health`
   - `http://127.0.0.1:8000/api/v1/healthz`
   - `http://127.0.0.1:8000/api/v1/health/ready`

## Quality Gates
- Lint: `python3 -m ruff check src tests`
- Format check: `python3 -m black --check src tests`
- Import order check: `python3 -m isort --check-only src tests`
- Type check: `python3 -m mypy src`
- Tests: `python3 -m pytest`

## Key Environment Variables
- `SUPABASE_URL`
- `SUPABASE_SERVICE_ROLE_KEY` or `SUPABASE_ANON_KEY`
- `API_PREFIX`
- `CORS_ALLOW_ORIGINS`
- `FX_MANUAL_RUN_TOKEN`
- `MARKETING_MANUAL_RUN_TOKEN` (optional; gates manual marketing sync endpoint when set)
- `AI_MANUAL_RUN_TOKEN`
- FX provider and behavior settings in `src/core/config.py`
- GA4 settings:
  - `GOOGLE_PROJECT_ID`
  - `GOOGLE_SERVICE_ACCOUNT_EMAIL`
  - `GOOGLE_SERVICE_ACCOUNT_KEY_JSON`
  - `GOOGLE_GA4_PROPERTY_ID`
  - `GOOGLE_GSC_SITE_URL` (optional/deferred)
  - `MARKETING_DEFAULT_TIMEZONE`
  - `MARKETING_DEFAULT_CURRENCY`

## Conventions
- Python modules: `snake_case.py`
- Layering: route -> service -> repository
- API payload fields: `camelCase`
- Query params: `snake_case`

## Documentation
- `../SwianOS_Documentation/docs/swainos-code-documentation-backend.md`
- `../SwianOS_Documentation/docs/frontend-data-queries.md`
- `../SwianOS_Documentation/docs/sample-payloads.md`

## Salesforce Read-Only Sync
- Runtime script: `scripts/sync_salesforce_readonly.py`
- Permission preflight: `scripts/validate_salesforce_readonly_permissions.py`
- Mode: read-only ingestion from Salesforce Bulk API 2.0 (`queryAll`)
- Incremental cursor: `SystemModstamp + Id` (stored in Supabase runtime tables)
- Sequence: `agencies -> suppliers -> employees -> itineraries -> itinerary_items`
- Guardrails:
  - endpoint allowlist in `src/integrations/salesforce_bulk_client.py`
  - no retry behavior on failures
  - singleton lock to avoid overlapping scheduled runs
