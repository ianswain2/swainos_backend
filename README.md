SwainOS Backend

Overview
- SwainOS is an AI-first business command center for Swain Destinations.
- Backend provides APIs, data ingestion, analytics, and AI services.

Documentation
- Goals: `SwianOS_Documentation/docs/goals.md`
- Objectives: `SwianOS_Documentation/docs/objectives.md`
- Purpose: `SwianOS_Documentation/docs/purpose.md`
- Scope and modules: `SwianOS_Documentation/docs/scope-and-modules.md`
- Phases and success criteria: `SwianOS_Documentation/docs/success-criteria-and-phases.md`

Structure
- Root: src
- API routes: src/api (FastAPI route handlers by module)
- Services: src/services (business logic)
- Repositories: src/repositories (DB access, mapping)
- Schemas: src/schemas (request/response validation)
- Models: src/models
- Scripts: scripts/ (ingestion utilities and one-off loaders)
- Analytics: src/analytics (calculations, signal generators)
- Core: src/core (config, logging, errors)
- Shared: src/shared (types, utils)

Modules (spec-aligned)
- dashboard, cash_flow, debt_service, revenue, fx, ai, settings
- integrations: salesforce, quickbooks, fx_rates, news (planned)

Conventions
- Files/modules: snake_case.py
- Classes: PascalCase
- Functions/variables: snake_case
- API: /api/v1/resource, snake_case query params
- DB: snake_case columns, plural tables

Alignment sources
- SwainOS Project Specification (Feb 2026)
- SwainOS backend rules (FastAPI layering, error envelopes, mapping conventions)
