# SwainOS Backend

## Overview
SwainOS Backend is a FastAPI service that powers analytics and command-center modules for Swain Destinations. It provides typed API endpoints for cash flow, deposits, payments out, itinerary forecasting/actuals, and FX.

## Architecture
- `src/api`: route handlers
- `src/services`: business logic
- `src/repositories`: database access and mapping
- `src/schemas`: request/response schemas
- `src/analytics`: rollups and forecasting calculations
- `src/core`: config, logging, error handling, Supabase integration
- `src/shared`: common response/time helpers
- `scripts`: ingestion/upsert utilities

## API Surface
All endpoints are under `api_prefix` (default `/api/v1`):
- `GET /health`, `GET /healthz`
- `GET /cash-flow/summary`, `GET /cash-flow/timeseries`
- `GET /deposits/summary`
- `GET /payments-out/summary`
- `GET /booking-forecasts`
- `GET /itinerary-trends`
- `GET /itinerary-lead-flow`
- `GET /itinerary-revenue/outlook`, `GET /itinerary-revenue/deposits`
- `GET /itinerary-revenue/conversion`, `GET /itinerary-revenue/channels`
- `GET /itinerary-revenue/actuals-yoy`, `GET /itinerary-revenue/actuals-channels`
- `GET /fx/rates`, `POST /fx/rates/run`
- `GET /fx/exposure`
- `GET /fx/signals`, `POST /fx/signals/run`
- `GET /fx/transactions`, `POST /fx/transactions`
- `GET /fx/holdings`
- `GET /fx/intelligence`, `POST /fx/intelligence/run`
- `GET /fx/invoice-pressure`
- `GET /travel-consultants/leaderboard`
- `GET /travel-consultants/{employee_id}/profile`
- `GET /travel-consultants/{employee_id}/forecast`
- `GET /travel-agents/leaderboard`, `GET /travel-agents/{agent_id}/profile`
- `GET /travel-agencies/leaderboard`, `GET /travel-agencies/{agency_id}/profile`
- `GET /travel-trade/search`
- `GET /ai-insights/briefing`
- `GET /ai-insights/feed`, `GET /ai-insights/recommendations`
- `PATCH /ai-insights/recommendations/{recommendation_id}`
- `GET /ai-insights/history`
- `GET /ai-insights/entities/{entity_type}/{entity_id}`
- `POST /ai-insights/run`

## Configuration
Environment is loaded from `.env` via `src/core/config.py`.

Required:
- `SUPABASE_URL`

Optional:
- `SUPABASE_SERVICE_ROLE_KEY`
- `SUPABASE_ANON_KEY`
- `API_PREFIX` (defaults to `/api/v1`)
- `CORS_ALLOW_ORIGINS` (defaults to localhost frontend origins)
- `APP_NAME`
- `ENVIRONMENT`
- `FX_MANUAL_RUN_TOKEN` (required to enable manual FX run endpoints)
- `FX_BASE_CURRENCY`, `FX_TARGET_CURRENCIES`
- `FX_PRIMARY_PROVIDER`, `FX_PRIMARY_API_KEY`, `FX_PRIMARY_BASE_URL`
- `FX_INTELLIGENCE_ON_DEMAND_ENABLED`
- `FX_STALE_AFTER_MINUTES`, `FX_PULL_INTERVAL_MINUTES`

## Local Development
1. Create `.env` in repo root and set at least `SUPABASE_URL`.
2. Start the server:
   - `cd /Users/ianswain/Desktop/SwainOS_BackEnd`
   - `uvicorn src.main:app --reload`
3. Validate health:
   - `http://127.0.0.1:8000/api/v1/health`

## Project Conventions
- Python files/modules: `snake_case.py`
- Layering: route -> service -> repository
- API JSON: camelCase at boundary
- DB naming: snake_case

## Documentation References
- Goals: `../SwianOS_Documentation/docs/goals.md`
- Objectives: `../SwianOS_Documentation/docs/objectives.md`
- Purpose: `../SwianOS_Documentation/docs/purpose.md`
- Scope and modules: `../SwianOS_Documentation/docs/scope-and-modules.md`
- Phases and success criteria: `../SwianOS_Documentation/docs/success-criteria-and-phases.md`
