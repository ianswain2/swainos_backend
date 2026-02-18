from __future__ import annotations

from functools import lru_cache

from src.repositories.fx_repository import FxRepository
from src.repositories.ai_insights_repository import AiInsightsRepository
from src.repositories.itinerary_pipeline_repository import ItineraryPipelineRepository
from src.repositories.itinerary_revenue_repository import ItineraryRevenueRepository
from src.repositories.revenue_bookings_repository import RevenueBookingsRepository
from src.repositories.travel_consultants_repository import TravelConsultantsRepository
from src.repositories.travel_agents_repository import TravelAgentsRepository
from src.repositories.travel_agencies_repository import TravelAgenciesRepository
from src.repositories.travel_trade_search_repository import TravelTradeSearchRepository
from src.services.fx_service import FxService
from src.services.fx_intelligence_service import FxIntelligenceService
from src.services.ai_insights_service import AiInsightsService
from src.services.ai_orchestration_service import AiOrchestrationService
from src.services.itinerary_revenue_service import ItineraryRevenueService
from src.services.openai_insights_service import OpenAiInsightsService
from src.services.revenue_bookings_service import RevenueBookingsService
from src.services.travel_consultants_service import TravelConsultantsService
from src.services.travel_agents_service import TravelAgentsService
from src.services.travel_agencies_service import TravelAgenciesService
from src.services.travel_trade_search_service import TravelTradeSearchService


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


def get_fx_intelligence_service() -> FxIntelligenceService:
    return FxIntelligenceService(
        repository=get_fx_repository(),
        openai_service=get_openai_insights_service(),
    )


@lru_cache
def get_travel_consultants_repository() -> TravelConsultantsRepository:
    return TravelConsultantsRepository()


def get_travel_consultants_service() -> TravelConsultantsService:
    return TravelConsultantsService(repository=get_travel_consultants_repository())


@lru_cache
def get_travel_agents_repository() -> TravelAgentsRepository:
    return TravelAgentsRepository()


def get_travel_agents_service() -> TravelAgentsService:
    return TravelAgentsService(repository=get_travel_agents_repository())


@lru_cache
def get_travel_agencies_repository() -> TravelAgenciesRepository:
    return TravelAgenciesRepository()


def get_travel_agencies_service() -> TravelAgenciesService:
    return TravelAgenciesService(repository=get_travel_agencies_repository())


@lru_cache
def get_travel_trade_search_repository() -> TravelTradeSearchRepository:
    return TravelTradeSearchRepository()


def get_travel_trade_search_service() -> TravelTradeSearchService:
    return TravelTradeSearchService(repository=get_travel_trade_search_repository())


@lru_cache
def get_ai_insights_repository() -> AiInsightsRepository:
    return AiInsightsRepository()


@lru_cache
def get_openai_insights_service() -> OpenAiInsightsService:
    return OpenAiInsightsService()


def get_ai_orchestration_service() -> AiOrchestrationService:
    return AiOrchestrationService(
        repository=get_ai_insights_repository(),
        openai_service=get_openai_insights_service(),
        travel_consultants_service=get_travel_consultants_service(),
    )


def get_ai_insights_service() -> AiInsightsService:
    return AiInsightsService(repository=get_ai_insights_repository())
