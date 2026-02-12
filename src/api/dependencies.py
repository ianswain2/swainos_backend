from __future__ import annotations

from functools import lru_cache

from src.repositories.fx_repository import FxRepository
from src.repositories.itinerary_pipeline_repository import ItineraryPipelineRepository
from src.repositories.itinerary_revenue_repository import ItineraryRevenueRepository
from src.repositories.revenue_bookings_repository import RevenueBookingsRepository
from src.services.fx_service import FxService
from src.services.itinerary_revenue_service import ItineraryRevenueService
from src.services.revenue_bookings_service import RevenueBookingsService


@lru_cache
def get_revenue_bookings_repository() -> RevenueBookingsRepository:
    return RevenueBookingsRepository()


def get_revenue_bookings_service() -> RevenueBookingsService:
    return RevenueBookingsService(repository=get_revenue_bookings_repository())


@lru_cache
def get_itinerary_revenue_repository() -> ItineraryRevenueRepository:
    return ItineraryRevenueRepository()


def get_itinerary_revenue_service() -> ItineraryRevenueService:
    return ItineraryRevenueService(
        revenue_repository=get_itinerary_revenue_repository(),
        pipeline_repository=get_itinerary_pipeline_repository(),
    )


@lru_cache
def get_itinerary_pipeline_repository() -> ItineraryPipelineRepository:
    return ItineraryPipelineRepository()


@lru_cache
def get_fx_repository() -> FxRepository:
    return FxRepository()


def get_fx_service() -> FxService:
    return FxService(repository=get_fx_repository())
