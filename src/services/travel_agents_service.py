from __future__ import annotations

import calendar
from collections import defaultdict
from datetime import date
from typing import Dict, List, Optional, Tuple

from src.core.errors import NotFoundError
from src.repositories.travel_agents_repository import TravelAgentsRepository
from src.schemas.travel_agents import (
    TravelAgentConsultantAffinity,
    TravelAgentIdentity,
    TravelAgentKpis,
    TravelAgentLeaderboardFilters,
    TravelAgentLeaderboardResponse,
    TravelAgentLeaderboardRow,
    TravelAgentOperationalItinerary,
    TravelAgentProfileFilters,
    TravelAgentProfileResponse,
    TravelAgentYoyPoint,
    TravelAgentYoySeries,
)

MONTH_LABELS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]


class TravelAgentsService:
    def __init__(self, repository: TravelAgentsRepository) -> None:
        self.repository = repository

    def get_leaderboard(self, filters: TravelAgentLeaderboardFilters) -> TravelAgentLeaderboardResponse:
        period_start, period_end = self._resolve_period_window(filters.period_type, filters.year, filters.month)
        rows = self.repository.list_rollup_rows(period_start, period_end)
        by_agent = self._aggregate_rows(rows)
        sort_key = self._sort_key(filters.sort_by)
        ranked = sorted(
            by_agent.values(),
            key=lambda item: self._leaderboard_sort_tuple(item, sort_key, filters.sort_order),
        )[: filters.top_n]
        rankings = [
            TravelAgentLeaderboardRow(
                rank=index + 1,
                agent_id=item["agent_id"],
                agent_external_id=item["agent_external_id"],
                agent_name=item["agent_name"],
                agent_email=item["agent_email"],
                agency_id=item["agency_id"],
                agency_external_id=item["agency_external_id"],
                agency_name=item["agency_name"],
                leads_count=int(item["leads_count"]),
                converted_leads_count=int(item["converted_leads_count"]),
                booked_itineraries_count=int(item["booked_itineraries_count"]),
                gross_amount=round(float(item["gross_amount"]), 2),
                gross_profit_amount=round(float(item["gross_profit_amount"]), 2),
                conversion_rate=self._rate(item["converted_leads_count"], item["leads_count"]),
            )
            for index, item in enumerate(ranked)
        ]
        return TravelAgentLeaderboardResponse(
            period_start=period_start,
            period_end=period_end,
            period_type=filters.period_type,
            sort_by=filters.sort_by,
            sort_order=filters.sort_order,
            top_n=filters.top_n,
            rankings=rankings,
        )

    def get_profile(self, agent_id: str, filters: TravelAgentProfileFilters) -> TravelAgentProfileResponse:
        agent = self.repository.get_agent(agent_id)
        if not agent:
            raise NotFoundError("Travel agent not found")
        agency = self.repository.get_agency_for_agent(str(agent["agency_id"]))
        if not agency:
            raise NotFoundError("Travel agency not found for travel agent")

        period_start, period_end = self._resolve_period_window(filters.period_type, filters.year, filters.month)
        rows = self.repository.list_rollup_rows(period_start, period_end, agent_id=agent_id)
        aggregate = self._aggregate_rows(rows).get(agent_id)
        if not aggregate:
            aggregate = {
                "agent_id": str(agent["id"]),
                "agent_external_id": str(agent.get("external_id") or ""),
                "agent_name": self._agent_name(agent),
                "agent_email": agent.get("email"),
                "agency_id": str(agency["id"]),
                "agency_external_id": str(agency.get("external_id") or ""),
                "agency_name": str(agency.get("agency_name") or ""),
                "leads_count": 0,
                "converted_leads_count": 0,
                "booked_itineraries_count": 0,
                "gross_amount": 0.0,
                "gross_profit_amount": 0.0,
            }

        affinity_rows = self.repository.list_affinity_rows(period_start, period_end, agent_id=agent_id)
        by_consultant: Dict[str, Dict[str, object]] = {}
        for row in affinity_rows:
            employee_id = str(row.get("employee_id") or "")
            if not employee_id:
                continue
            bucket = by_consultant.setdefault(
                employee_id,
                {
                    "employee_id": employee_id,
                    "employee_external_id": str(row.get("employee_external_id") or ""),
                    "employee_name": f"{str(row.get('employee_first_name') or '')} {str(row.get('employee_last_name') or '')}".strip(),
                    "converted_leads_count": 0,
                    "closed_won_itineraries_count": 0,
                },
            )
            bucket["converted_leads_count"] = int(bucket["converted_leads_count"]) + self._to_int(
                row.get("converted_leads_count")
            )
            bucket["closed_won_itineraries_count"] = int(
                bucket["closed_won_itineraries_count"]
            ) + self._to_int(row.get("closed_won_itineraries_count"))
        ranked_consultants = sorted(
            by_consultant.values(),
            key=lambda item: (
                -int(item["converted_leads_count"]),
                -int(item["closed_won_itineraries_count"]),
                str(item["employee_name"]),
            ),
        )[: filters.top_n]
        consultants = [
            TravelAgentConsultantAffinity(
                employee_id=str(item["employee_id"]),
                employee_external_id=str(item["employee_external_id"]),
                employee_name=str(item["employee_name"]),
                converted_leads_count=int(item["converted_leads_count"]),
                closed_won_itineraries_count=int(item["closed_won_itineraries_count"]),
            )
            for item in ranked_consultants
        ]

        current_year = period_end.year
        prior_year = current_year - 1
        current_rows = self.repository.list_rollup_rows(date(current_year, 1, 1), date(current_year, 12, 31), agent_id)
        prior_rows = self.repository.list_rollup_rows(date(prior_year, 1, 1), date(prior_year, 12, 31), agent_id)
        yoy_series = [
            self._build_yoy_series("leads", current_year, prior_year, current_rows, prior_rows),
            self._build_yoy_series("bookedItineraries", current_year, prior_year, current_rows, prior_rows),
            self._build_yoy_series("grossRevenue", current_year, prior_year, current_rows, prior_rows),
            self._build_yoy_series("grossProfit", current_year, prior_year, current_rows, prior_rows),
        ]

        today = date.today()
        open_status_values = self.repository.list_open_status_values()
        current_traveling = self.repository.list_current_traveling_itineraries(
            str(agent.get("external_id") or ""),
            today,
            limit=10,
        )
        top_open = self.repository.list_top_open_itineraries(
            str(agent.get("external_id") or ""),
            open_status_values,
            limit=5,
        )

        return TravelAgentProfileResponse(
            agent=TravelAgentIdentity(
                agent_id=str(agent["id"]),
                agent_external_id=str(agent.get("external_id") or ""),
                agent_name=self._agent_name(agent),
                agent_email=agent.get("email"),
                agency_id=str(agency["id"]),
                agency_external_id=str(agency.get("external_id") or ""),
                agency_name=str(agency.get("agency_name") or ""),
            ),
            period_start=period_start,
            period_end=period_end,
            period_type=filters.period_type,
            kpis=TravelAgentKpis(
                leads_count=int(aggregate["leads_count"]),
                converted_leads_count=int(aggregate["converted_leads_count"]),
                booked_itineraries_count=int(aggregate["booked_itineraries_count"]),
                gross_amount=round(float(aggregate["gross_amount"]), 2),
                gross_profit_amount=round(float(aggregate["gross_profit_amount"]), 2),
                conversion_rate=self._rate(
                    int(aggregate["converted_leads_count"]),
                    int(aggregate["leads_count"]),
                ),
            ),
            yoy_series=yoy_series,
            primary_travel_consultants=consultants,
            current_traveling_files=[self._map_operational_row(row) for row in current_traveling],
            top_open_itineraries=[self._map_operational_row(row) for row in top_open],
        )

    def _build_yoy_series(
        self,
        metric: str,
        current_year: int,
        prior_year: int,
        current_rows: List[dict],
        prior_rows: List[dict],
    ) -> TravelAgentYoySeries:
        current = defaultdict(float)
        prior = defaultdict(float)
        for row in current_rows:
            period = self._to_date(row.get("period_start"))
            current[period.month] += self._metric_value(row, metric)
        for row in prior_rows:
            period = self._to_date(row.get("period_start"))
            prior[period.month] += self._metric_value(row, metric)
        points: List[TravelAgentYoyPoint] = []
        for month in range(1, 13):
            points.append(
                TravelAgentYoyPoint(
                    month=month,
                    month_label=MONTH_LABELS[month - 1],
                    current_year_value=round(current[month], 2),
                    prior_year_value=round(prior[month], 2),
                )
            )
        total_current = sum(point.current_year_value for point in points)
        total_prior = sum(point.prior_year_value for point in points)
        yoy_delta_pct = ((total_current - total_prior) / total_prior) if total_prior else 0.0
        return TravelAgentYoySeries(
            metric=metric,
            current_year=current_year,
            prior_year=prior_year,
            points=points,
            total_current_year_value=round(total_current, 2),
            total_prior_year_value=round(total_prior, 2),
            yoy_delta_pct=round(yoy_delta_pct, 4),
        )

    @staticmethod
    def _metric_value(row: dict, metric: str) -> float:
        if metric == "leads":
            return float(row.get("leads_count") or 0)
        if metric == "bookedItineraries":
            return float(row.get("traveled_itineraries_count") or 0)
        if metric == "grossRevenue":
            return float(row.get("gross_amount") or 0)
        return float(row.get("gross_profit_amount") or 0)

    @staticmethod
    def _to_int(value: object) -> int:
        if value is None:
            return 0
        return int(float(value))

    @staticmethod
    def _to_date(value: object) -> date:
        if isinstance(value, date):
            return value
        return date.fromisoformat(str(value))

    @staticmethod
    def _rate(numerator: int, denominator: int) -> float:
        return round((numerator / denominator) if denominator else 0.0, 4)

    @staticmethod
    def _sort_key(sort_by: str) -> str:
        if sort_by == "leads":
            return "leads_count"
        if sort_by == "converted_leads":
            return "converted_leads_count"
        if sort_by == "booked_itineraries":
            return "booked_itineraries_count"
        if sort_by == "gross":
            return "gross_amount"
        return "gross_profit_amount"

    @staticmethod
    def _leaderboard_sort_tuple(item: Dict[str, object], sort_key: str, sort_order: str) -> tuple[float, float, str]:
        primary = float(item[sort_key] or 0)
        gross_profit = float(item["gross_profit_amount"] or 0)
        name = str(item["agent_name"] or "").lower()
        if sort_order == "desc":
            return (-primary, -gross_profit, name)
        return (primary, gross_profit, name)

    def _aggregate_rows(self, rows: List[dict]) -> Dict[str, Dict[str, object]]:
        aggregate: Dict[str, Dict[str, object]] = {}
        for row in rows:
            agent_id = str(row.get("agent_id") or "")
            if not agent_id:
                continue
            bucket = aggregate.setdefault(
                agent_id,
                {
                    "agent_id": agent_id,
                    "agent_external_id": str(row.get("agent_external_id") or ""),
                    "agent_name": str(row.get("agent_name") or ""),
                    "agent_email": row.get("agent_email"),
                    "agency_id": str(row.get("agency_id") or ""),
                    "agency_external_id": str(row.get("agency_external_id") or ""),
                    "agency_name": str(row.get("agency_name") or ""),
                    "leads_count": 0,
                    "converted_leads_count": 0,
                    "booked_itineraries_count": 0,
                    "gross_amount": 0.0,
                    "gross_profit_amount": 0.0,
                },
            )
            bucket["leads_count"] = int(bucket["leads_count"]) + self._to_int(row.get("leads_count"))
            bucket["converted_leads_count"] = int(bucket["converted_leads_count"]) + self._to_int(
                row.get("converted_leads_count")
            )
            bucket["booked_itineraries_count"] = int(bucket["booked_itineraries_count"]) + self._to_int(
                row.get("traveled_itineraries_count")
            )
            bucket["gross_amount"] = float(bucket["gross_amount"]) + float(row.get("gross_amount") or 0)
            bucket["gross_profit_amount"] = float(bucket["gross_profit_amount"]) + float(
                row.get("gross_profit_amount") or 0
            )
        return aggregate

    def _map_operational_row(self, row: dict) -> TravelAgentOperationalItinerary:
        return TravelAgentOperationalItinerary(
            itinerary_id=str(row.get("id") or ""),
            itinerary_number=str(row.get("itinerary_number") or ""),
            itinerary_name=str(row.get("itinerary_name") or "") or None,
            itinerary_status=str(row.get("itinerary_status") or ""),
            travel_start_date=self._optional_to_date(row.get("travel_start_date")),
            travel_end_date=self._optional_to_date(row.get("travel_end_date")),
            gross_amount=float(row.get("gross_amount") or 0),
            gross_profit_amount=float(row.get("gross_profit") or 0),
        )

    @staticmethod
    def _optional_to_date(value: object) -> Optional[date]:
        if value in (None, ""):
            return None
        if isinstance(value, date):
            return value
        return date.fromisoformat(str(value))

    @staticmethod
    def _agent_name(agent: dict) -> str:
        first = str(agent.get("first_name") or "").strip()
        last = str(agent.get("last_name") or "").strip()
        return f"{first} {last}".strip() or "Unnamed Agent"

    @staticmethod
    def _resolve_period_window(
        period_type: str, year: Optional[int], month: Optional[int]
    ) -> Tuple[date, date]:
        today = date.today()
        if period_type == "rolling12":
            end_month_start = date(today.year, today.month, 1)
            period_start = TravelAgentsService._add_months(end_month_start, -11)
            period_end = TravelAgentsService._month_end(end_month_start)
            return period_start, period_end
        if period_type == "year":
            period_year = year or today.year
            return date(period_year, 1, 1), date(period_year, 12, 31)
        period_year = year or today.year
        period_month = month or today.month
        period_start = date(period_year, period_month, 1)
        period_end = TravelAgentsService._month_end(period_start)
        return period_start, period_end

    @staticmethod
    def _month_end(period_start: date) -> date:
        return date(
            period_start.year,
            period_start.month,
            calendar.monthrange(period_start.year, period_start.month)[1],
        )

    @staticmethod
    def _add_months(base: date, months: int) -> date:
        month_index = base.month - 1 + months
        year = base.year + month_index // 12
        month = month_index % 12 + 1
        return date(year, month, 1)
