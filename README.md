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
- `AI_MANUAL_RUN_TOKEN`
- FX provider and behavior settings in `src/core/config.py`

## Conventions
- Python modules: `snake_case.py`
- Layering: route -> service -> repository
- API payload fields: `camelCase`
- Query params: `snake_case`

## Documentation
- `../SwianOS_Documentation/docs/swainos-code-documentation-backend.md`
- `../SwianOS_Documentation/docs/frontend-data-queries.md`
- `../SwianOS_Documentation/docs/sample-payloads.md`
