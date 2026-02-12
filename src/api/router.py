from __future__ import annotations

from fastapi import APIRouter

from src.api.booking_forecasts import router as booking_forecasts_router
from src.api.cash_flow import router as cash_flow_router
from src.api.deposits import router as deposits_router
from src.api.fx import router as fx_router
from src.api.health import router as health_router
from src.api.itinerary_revenue import router as itinerary_revenue_router
from src.api.itinerary_lead_flow import router as itinerary_lead_flow_router
from src.api.itinerary_trends import router as itinerary_trends_router
from src.api.payments_out import router as payments_out_router


api_router = APIRouter()
api_router.include_router(health_router)
api_router.include_router(cash_flow_router)
api_router.include_router(deposits_router)
api_router.include_router(payments_out_router)
api_router.include_router(booking_forecasts_router)
api_router.include_router(itinerary_trends_router)
api_router.include_router(itinerary_lead_flow_router)
api_router.include_router(itinerary_revenue_router)
api_router.include_router(fx_router)
