from __future__ import annotations

from functools import lru_cache

from src.repositories.fx_repository import FxRepository
from src.repositories.marketing_web_analytics_repository import MarketingWebAnalyticsRepository
from src.repositories.ai_insights_repository import AiInsightsRepository
from src.repositories.itinerary_pipeline_repository import ItineraryPipelineRepository
from src.repositories.itinerary_destinations_repository import ItineraryDestinationsRepository
from src.repositories.itinerary_revenue_repository import ItineraryRevenueRepository
from src.repositories.revenue_bookings_repository import RevenueBookingsRepository
from src.repositories.travel_consultants_repository import TravelConsultantsRepository
from src.repositories.travel_agents_repository import TravelAgentsRepository
from src.repositories.travel_agencies_repository import TravelAgenciesRepository
from src.repositories.travel_trade_search_repository import TravelTradeSearchRepository
from src.repositories.debt_service_repository import DebtServiceRepository
from src.services.fx_service import FxService
from src.services.marketing_web_analytics_service import MarketingWebAnalyticsService
from src.services.fx_intelligence_service import FxIntelligenceService
from src.services.ai_insights_service import AiInsightsService
from src.services.itinerary_revenue_service import ItineraryRevenueService
from src.services.itinerary_destinations_service import ItineraryDestinationsService
from src.services.openai_insights_service import OpenAiInsightsService
from src.services.revenue_bookings_service import RevenueBookingsService
from src.services.travel_consultants_service import TravelConsultantsService
from src.services.travel_agents_service import TravelAgentsService
from src.services.travel_agencies_service import TravelAgenciesService
from src.services.travel_trade_search_service import TravelTradeSearchService
from src.services.debt_service_service import DebtServiceService
from src.integrations.google_analytics_client import GoogleAnalyticsClient


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
def get_itinerary_destinations_repository() -> ItineraryDestinationsRepository:
    return ItineraryDestinationsRepository()


def get_itinerary_destinations_service() -> ItineraryDestinationsService:
    return ItineraryDestinationsService(repository=get_itinerary_destinations_repository())


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
def get_debt_service_repository() -> DebtServiceRepository:
    return DebtServiceRepository()


def get_debt_service_service() -> DebtServiceService:
    return DebtServiceService(repository=get_debt_service_repository())


@lru_cache
def get_ai_insights_repository() -> AiInsightsRepository:
    return AiInsightsRepository()


@lru_cache
def get_openai_insights_service() -> OpenAiInsightsService:
    return OpenAiInsightsService()


def get_ai_insights_service() -> AiInsightsService:
    return AiInsightsService(repository=get_ai_insights_repository())


@lru_cache
def get_marketing_web_analytics_repository() -> MarketingWebAnalyticsRepository:
    return MarketingWebAnalyticsRepository()


@lru_cache
def get_google_analytics_client() -> GoogleAnalyticsClient:
    return GoogleAnalyticsClient()


def get_marketing_web_analytics_service() -> MarketingWebAnalyticsService:
    return MarketingWebAnalyticsService(
        repository=get_marketing_web_analytics_repository(),
        ga_client=get_google_analytics_client(),
    )
