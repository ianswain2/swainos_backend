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
- `GET /itinerary-revenue/outlook`
- `GET /itinerary-revenue/deposits`
- `GET /itinerary-revenue/conversion`
- `GET /itinerary-revenue/channels`
- `GET /itinerary-revenue/actuals-yoy`
- `GET /itinerary-revenue/actuals-channels`
- `GET /fx/rates`, `GET /fx/exposure`

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
