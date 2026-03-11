from __future__ import annotations

from fastapi import APIRouter, Depends

from src.api.ai_insights import router as ai_insights_router
from src.api.ai_runs import router as ai_runs_router
from src.api.ap import router as ap_router
from src.api.auth import router as auth_router
from src.api.authz import require_marketing_permission, require_permission
from src.api.booking_forecasts import router as booking_forecasts_router
from src.api.cash_flow import router as cash_flow_router
from src.api.data_job_runs import router as data_job_runs_router
from src.api.data_jobs import router as data_jobs_router
from src.api.debt_service import router as debt_service_router
from src.api.deposits import router as deposits_router
from src.api.fx import router as fx_router
from src.api.fx_runs import router as fx_runs_router
from src.api.health import router as health_router
from src.api.itinerary_destinations import router as itinerary_destinations_router
from src.api.itinerary_lead_flow import router as itinerary_lead_flow_router
from src.api.itinerary_revenue import router as itinerary_revenue_router
from src.api.itinerary_trends import router as itinerary_trends_router
from src.api.marketing_web_analytics import router as marketing_web_analytics_router
from src.api.payments_out import router as payments_out_router
from src.api.revenue_bookings import router as revenue_bookings_router
from src.api.settings_user_access import router as settings_user_access_router
from src.api.travel_agencies import router as travel_agencies_router
from src.api.travel_agents import router as travel_agents_router
from src.api.travel_consultants import router as travel_consultants_router
from src.api.travel_trade_search import router as travel_trade_search_router

api_router = APIRouter()
api_router.include_router(health_router)
api_router.include_router(auth_router)
api_router.include_router(
    data_jobs_router,
    dependencies=[Depends(require_permission("settings_job_controls"))],
)
api_router.include_router(
    data_job_runs_router,
    dependencies=[Depends(require_permission("settings_run_logs"))],
)
api_router.include_router(
    ap_router,
    dependencies=[Depends(require_permission("cash_flow"))],
)
api_router.include_router(
    cash_flow_router,
    dependencies=[Depends(require_permission("cash_flow"))],
)
api_router.include_router(
    deposits_router,
    dependencies=[Depends(require_permission("command_center"))],
)
api_router.include_router(
    debt_service_router,
    dependencies=[Depends(require_permission("debt_service"))],
)
api_router.include_router(
    payments_out_router,
    dependencies=[Depends(require_permission("command_center"))],
)
api_router.include_router(
    revenue_bookings_router,
    dependencies=[Depends(require_permission("itinerary_forecast"))],
)
api_router.include_router(
    booking_forecasts_router,
    dependencies=[Depends(require_permission("command_center"))],
)
api_router.include_router(
    itinerary_trends_router,
    dependencies=[Depends(require_permission("command_center"))],
)
api_router.include_router(
    itinerary_lead_flow_router,
    dependencies=[Depends(require_permission("itinerary_actuals"))],
)
api_router.include_router(
    itinerary_revenue_router,
    dependencies=[Depends(require_permission("itinerary_forecast"))],
)
api_router.include_router(
    itinerary_destinations_router,
    dependencies=[Depends(require_permission("destination"))],
)
api_router.include_router(
    fx_router,
    dependencies=[Depends(require_permission("fx_command"))],
)
api_router.include_router(
    fx_runs_router,
    dependencies=[Depends(require_permission("fx_command"))],
)
api_router.include_router(
    marketing_web_analytics_router,
    dependencies=[Depends(require_marketing_permission)],
)
api_router.include_router(
    travel_consultants_router,
    dependencies=[Depends(require_permission("travel_consultant"))],
)
api_router.include_router(
    travel_agents_router,
    dependencies=[Depends(require_permission("travel_agencies"))],
)
api_router.include_router(
    travel_agencies_router,
    dependencies=[Depends(require_permission("travel_agencies"))],
)
api_router.include_router(
    travel_trade_search_router,
    dependencies=[Depends(require_permission("travel_agencies"))],
)
api_router.include_router(
    ai_insights_router,
    dependencies=[Depends(require_permission("ai_insights"))],
)
api_router.include_router(
    ai_runs_router,
    dependencies=[Depends(require_permission("ai_insights"))],
)
api_router.include_router(
    settings_user_access_router,
    dependencies=[Depends(require_permission("settings_user_access"))],
)
