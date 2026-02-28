from __future__ import annotations

from collections import defaultdict
from datetime import date
from typing import Dict, List, Optional, Tuple

from src.repositories.itinerary_destinations_repository import ItineraryDestinationsRepository
from src.schemas.itinerary_destinations import (
    DestinationCityBreakdownPoint,
    DestinationCountryBreakdownPoint,
    DestinationCountrySummaryPoint,
    DestinationKpis,
    DestinationTrendPoint,
    DestinationMatrixCell,
    DestinationMatrixTotals,
    DestinationCountryMatrixRow,
    DestinationCityMatrixRow,
    ItineraryDestinationBreakdownResponse,
    ItineraryDestinationMatrixResponse,
    ItineraryDestinationSummaryResponse,
    ItineraryDestinationTrendsResponse,
)


class ItineraryDestinationsService:
    def __init__(self, repository: ItineraryDestinationsRepository) -> None:
        self.repository = repository

    def get_summary(self, year: int, top_n: int) -> ItineraryDestinationSummaryResponse:
        rows = self.repository.list_destination_rollups(year=year)
        totals = self._aggregate_totals(rows)
        country_rollups = self._aggregate_by_country(rows)
        ordered_countries = sorted(
            country_rollups.items(),
            key=lambda item: item[1]["booked_total_price"],
            reverse=True,
        )
        top_countries = [self._to_country_summary_point(country, values, totals["booked_total_price"]) for country, values in ordered_countries[:top_n]]
        kpis = DestinationKpis(
            active_item_count=int(round(totals["active_item_count"])),
            booked_itineraries_count=int(round(totals["booked_itineraries_count"])),
            booked_total_price=totals["booked_total_price"],
            booked_total_cost=totals["booked_total_cost"],
            booked_gross_margin=totals["booked_gross_margin"],
            booked_margin_pct=self._safe_ratio(totals["booked_gross_margin"], totals["booked_total_price"]),
            country_count=len(country_rollups),
            city_count=len({(str(row.get("location_country") or ""), str(row.get("location_city") or "")) for row in rows}),
        )
        return ItineraryDestinationSummaryResponse(year=year, kpis=kpis, top_countries=top_countries)

    def get_trends(
        self,
        year: int,
        country: Optional[str],
        city: Optional[str],
    ) -> ItineraryDestinationTrendsResponse:
        rows = self.repository.list_destination_rollups(year=year, country=country, city=city)
        by_period: Dict[date, Dict[str, float]] = defaultdict(
            lambda: {
                "period_end_ordinal": 0.0,
                "active_item_count": 0.0,
                "booked_itineraries_count": 0.0,
                "booked_total_price": 0.0,
                "booked_gross_margin": 0.0,
            }
        )
        for row in rows:
            period_start = self._to_date(row.get("period_start"))
            period_end = self._to_date(row.get("period_end"))
            if not period_start:
                continue
            bucket = by_period[period_start]
            if period_end:
                bucket["period_end_ordinal"] = period_end.toordinal()
            bucket["active_item_count"] += float(row.get("active_item_count") or 0.0)
            bucket["booked_itineraries_count"] += float(row.get("booked_itinerary_count") or 0.0)
            bucket["booked_total_price"] += float(row.get("booked_total_price") or 0.0)
            bucket["booked_gross_margin"] += float(row.get("booked_gross_margin") or 0.0)

        timeline: List[DestinationTrendPoint] = []
        for month in range(1, 13):
            period_start = date(year, month, 1)
            default_period_end = (
                date(year + 1, 1, 1) if month == 12 else date(year, month + 1, 1)
            ) - date.resolution
            values = by_period.get(period_start, {})
            period_end = (
                date.fromordinal(int(values.get("period_end_ordinal", 0)))
                if values.get("period_end_ordinal")
                else default_period_end
            )
            gross = float(values.get("booked_total_price", 0.0))
            margin = float(values.get("booked_gross_margin", 0.0))
            timeline.append(
                DestinationTrendPoint(
                    period_start=period_start,
                    period_end=period_end,
                    active_item_count=int(round(values.get("active_item_count", 0.0))),
                    booked_itineraries_count=int(round(values.get("booked_itineraries_count", 0.0))),
                    booked_total_price=gross,
                    booked_gross_margin=margin,
                    booked_margin_pct=self._safe_ratio(margin, gross),
                )
            )
        return ItineraryDestinationTrendsResponse(
            year=year,
            country=country,
            city=city,
            timeline=timeline,
        )

    def get_breakdown(
        self,
        year: int,
        country: Optional[str],
        top_n: int,
    ) -> ItineraryDestinationBreakdownResponse:
        rows = self.repository.list_destination_rollups(year=year, country=country)
        by_country_city: Dict[Tuple[str, str], Dict[str, float]] = defaultdict(
            lambda: {
                "active_item_count": 0.0,
                "booked_itineraries_count": 0.0,
                "booked_total_price": 0.0,
                "booked_gross_margin": 0.0,
            }
        )
        for row in rows:
            country_name = str(row.get("location_country") or "Unknown")
            city_name = str(row.get("location_city") or "Unspecified")
            key = (country_name, city_name)
            bucket = by_country_city[key]
            bucket["active_item_count"] += float(row.get("active_item_count") or 0.0)
            bucket["booked_itineraries_count"] += float(row.get("booked_itinerary_count") or 0.0)
            bucket["booked_total_price"] += float(row.get("booked_total_price") or 0.0)
            bucket["booked_gross_margin"] += float(row.get("booked_gross_margin") or 0.0)

        by_country: Dict[str, Dict[str, object]] = defaultdict(
            lambda: {
                "active_item_count": 0.0,
                "booked_itineraries_count": 0.0,
                "booked_total_price": 0.0,
                "booked_gross_margin": 0.0,
                "cities": [],
            }
        )
        for (country_name, city_name), values in by_country_city.items():
            city_point = DestinationCityBreakdownPoint(
                city=city_name,
                active_item_count=int(round(values["active_item_count"])),
                booked_itineraries_count=int(round(values["booked_itineraries_count"])),
                booked_total_price=values["booked_total_price"],
                booked_gross_margin=values["booked_gross_margin"],
                booked_margin_pct=self._safe_ratio(
                    values["booked_gross_margin"], values["booked_total_price"]
                ),
            )
            country_bucket = by_country[country_name]
            country_bucket["active_item_count"] = float(country_bucket["active_item_count"]) + values["active_item_count"]
            country_bucket["booked_itineraries_count"] = float(country_bucket["booked_itineraries_count"]) + values["booked_itineraries_count"]
            country_bucket["booked_total_price"] = float(country_bucket["booked_total_price"]) + values["booked_total_price"]
            country_bucket["booked_gross_margin"] = float(country_bucket["booked_gross_margin"]) + values["booked_gross_margin"]
            cities = country_bucket["cities"]
            if isinstance(cities, list):
                cities.append(city_point)

        ordered_countries = sorted(
            by_country.items(),
            key=lambda item: float(item[1]["booked_total_price"]),
            reverse=True,
        )[:top_n]

        countries: List[DestinationCountryBreakdownPoint] = []
        for country_name, values in ordered_countries:
            cities = values["cities"]
            ordered_cities = (
                sorted(cities, key=lambda city_point: city_point.booked_total_price, reverse=True)[:top_n]
                if isinstance(cities, list)
                else []
            )
            total_price = float(values["booked_total_price"])
            total_margin = float(values["booked_gross_margin"])
            countries.append(
                DestinationCountryBreakdownPoint(
                    country=country_name,
                    active_item_count=int(round(float(values["active_item_count"]))),
                    booked_itineraries_count=int(round(float(values["booked_itineraries_count"]))),
                    booked_total_price=total_price,
                    booked_gross_margin=total_margin,
                    booked_margin_pct=self._safe_ratio(total_margin, total_price),
                    top_cities=ordered_cities,
                )
            )
        return ItineraryDestinationBreakdownResponse(year=year, country=country, countries=countries)

    def get_matrix(
        self, year: int, country: Optional[str], top_n: int
    ) -> ItineraryDestinationMatrixResponse:
        all_rows = self.repository.list_destination_rollups(year=year)
        prior_rows = self.repository.list_destination_rollups(year=year - 1)
        by_country_month: Dict[Tuple[str, int], Dict[str, float]] = defaultdict(
            lambda: {
                "revenue_amount": 0.0,
                "passenger_count": 0.0,
                "cost_amount": 0.0,
                "margin_amount": 0.0,
            }
        )
        by_country_month_prior: Dict[Tuple[str, int], Dict[str, float]] = defaultdict(
            lambda: {
                "revenue_amount": 0.0,
                "passenger_count": 0.0,
                "cost_amount": 0.0,
                "margin_amount": 0.0,
            }
        )
        country_gross_profit_totals: Dict[str, float] = defaultdict(float)
        for row in all_rows:
            period_start = self._to_date(row.get("period_start"))
            if not period_start:
                continue
            country_name = str(row.get("location_country") or "Unknown")
            month = period_start.month
            revenue = float(row.get("booked_total_price") or 0.0)
            passengers = float(row.get("booked_quantity") or 0.0)
            cost = float(row.get("booked_total_cost") or 0.0)
            margin = float(row.get("booked_gross_margin") or 0.0)
            bucket = by_country_month[(country_name, month)]
            bucket["revenue_amount"] += revenue
            bucket["passenger_count"] += passengers
            bucket["cost_amount"] += cost
            bucket["margin_amount"] += margin
            country_gross_profit_totals[country_name] += margin

        for row in prior_rows:
            period_start = self._to_date(row.get("period_start"))
            if not period_start:
                continue
            country_name = str(row.get("location_country") or "Unknown")
            month = period_start.month
            revenue = float(row.get("booked_total_price") or 0.0)
            passengers = float(row.get("booked_quantity") or 0.0)
            cost = float(row.get("booked_total_cost") or 0.0)
            margin = float(row.get("booked_gross_margin") or 0.0)
            bucket = by_country_month_prior[(country_name, month)]
            bucket["revenue_amount"] += revenue
            bucket["passenger_count"] += passengers
            bucket["cost_amount"] += cost
            bucket["margin_amount"] += margin

        if country:
            ordered_countries = [country]
        else:
            ordered_countries = [
                country_name
                for country_name, _ in sorted(
                    country_gross_profit_totals.items(), key=lambda item: item[1], reverse=True
                )[:top_n]
            ]

        months = list(range(1, 13))
        country_matrix: List[DestinationCountryMatrixRow] = []
        for country_name in ordered_countries:
            month_cells: List[DestinationMatrixCell] = []
            current_totals = {
                "revenue_amount": 0.0,
                "passenger_count": 0.0,
                "cost_amount": 0.0,
                "margin_amount": 0.0,
            }
            prior_totals = {
                "revenue_amount": 0.0,
                "passenger_count": 0.0,
                "cost_amount": 0.0,
                "margin_amount": 0.0,
            }
            for month in months:
                values = by_country_month[(country_name, month)]
                prior_values = by_country_month_prior[(country_name, month)]
                current_totals["revenue_amount"] += values["revenue_amount"]
                current_totals["passenger_count"] += values["passenger_count"]
                current_totals["cost_amount"] += values["cost_amount"]
                current_totals["margin_amount"] += values["margin_amount"]
                prior_totals["revenue_amount"] += prior_values["revenue_amount"]
                prior_totals["passenger_count"] += prior_values["passenger_count"]
                prior_totals["cost_amount"] += prior_values["cost_amount"]
                prior_totals["margin_amount"] += prior_values["margin_amount"]
                month_cells.append(
                    DestinationMatrixCell(
                        month=month,
                        revenue_amount=values["revenue_amount"],
                        passenger_count=values["passenger_count"],
                        cost_amount=values["cost_amount"],
                        margin_amount=values["margin_amount"],
                        margin_pct=self._safe_ratio(values["margin_amount"], values["revenue_amount"]),
                        revenue_yoy_pct=self._safe_yoy_ratio(
                            values["revenue_amount"], prior_values["revenue_amount"]
                        ),
                        passenger_yoy_pct=self._safe_yoy_ratio(
                            values["passenger_count"], prior_values["passenger_count"]
                        ),
                        cost_yoy_pct=self._safe_yoy_ratio(
                            values["cost_amount"], prior_values["cost_amount"]
                        ),
                        margin_yoy_pct=self._safe_yoy_ratio(
                            values["margin_amount"], prior_values["margin_amount"]
                        ),
                    )
                )
            country_matrix.append(
                DestinationCountryMatrixRow(
                    country=country_name,
                    months=month_cells,
                    totals=DestinationMatrixTotals(
                        revenue_amount=current_totals["revenue_amount"],
                        passenger_count=current_totals["passenger_count"],
                        cost_amount=current_totals["cost_amount"],
                        margin_amount=current_totals["margin_amount"],
                        revenue_yoy_pct=self._safe_yoy_ratio(
                            current_totals["revenue_amount"], prior_totals["revenue_amount"]
                        ),
                        passenger_yoy_pct=self._safe_yoy_ratio(
                            current_totals["passenger_count"], prior_totals["passenger_count"]
                        ),
                        cost_yoy_pct=self._safe_yoy_ratio(
                            current_totals["cost_amount"], prior_totals["cost_amount"]
                        ),
                        margin_yoy_pct=self._safe_yoy_ratio(
                            current_totals["margin_amount"], prior_totals["margin_amount"]
                        ),
                    ),
                )
            )

        city_matrix: List[DestinationCityMatrixRow] = []
        if country:
            country_rows = self.repository.list_destination_rollups(year=year, country=country)
            country_prior_rows = self.repository.list_destination_rollups(
                year=year - 1, country=country
            )
            by_city_month: Dict[Tuple[str, int], Dict[str, float]] = defaultdict(
                lambda: {
                    "revenue_amount": 0.0,
                    "passenger_count": 0.0,
                    "cost_amount": 0.0,
                    "margin_amount": 0.0,
                }
            )
            by_city_month_prior: Dict[Tuple[str, int], Dict[str, float]] = defaultdict(
                lambda: {
                    "revenue_amount": 0.0,
                    "passenger_count": 0.0,
                    "cost_amount": 0.0,
                    "margin_amount": 0.0,
                }
            )
            city_revenue_totals: Dict[str, float] = defaultdict(float)
            for row in country_rows:
                period_start = self._to_date(row.get("period_start"))
                if not period_start:
                    continue
                city_name = str(row.get("location_city") or "Unspecified")
                month = period_start.month
                revenue = float(row.get("booked_total_price") or 0.0)
                passengers = float(row.get("booked_quantity") or 0.0)
                cost = float(row.get("booked_total_cost") or 0.0)
                margin = float(row.get("booked_gross_margin") or 0.0)
                bucket = by_city_month[(city_name, month)]
                bucket["revenue_amount"] += revenue
                bucket["passenger_count"] += passengers
                bucket["cost_amount"] += cost
                bucket["margin_amount"] += margin
                city_revenue_totals[city_name] += revenue
            for row in country_prior_rows:
                period_start = self._to_date(row.get("period_start"))
                if not period_start:
                    continue
                city_name = str(row.get("location_city") or "Unspecified")
                month = period_start.month
                revenue = float(row.get("booked_total_price") or 0.0)
                passengers = float(row.get("booked_quantity") or 0.0)
                cost = float(row.get("booked_total_cost") or 0.0)
                margin = float(row.get("booked_gross_margin") or 0.0)
                bucket = by_city_month_prior[(city_name, month)]
                bucket["revenue_amount"] += revenue
                bucket["passenger_count"] += passengers
                bucket["cost_amount"] += cost
                bucket["margin_amount"] += margin
            top_cities = [
                city_name
                for city_name, _ in sorted(
                    city_revenue_totals.items(), key=lambda item: item[1], reverse=True
                )[:top_n]
            ]
            for city_name in top_cities:
                month_cells: List[DestinationMatrixCell] = []
                current_totals = {
                    "revenue_amount": 0.0,
                    "passenger_count": 0.0,
                    "cost_amount": 0.0,
                    "margin_amount": 0.0,
                }
                prior_totals = {
                    "revenue_amount": 0.0,
                    "passenger_count": 0.0,
                    "cost_amount": 0.0,
                    "margin_amount": 0.0,
                }
                for month in months:
                    values = by_city_month[(city_name, month)]
                    prior_values = by_city_month_prior[(city_name, month)]
                    current_totals["revenue_amount"] += values["revenue_amount"]
                    current_totals["passenger_count"] += values["passenger_count"]
                    current_totals["cost_amount"] += values["cost_amount"]
                    current_totals["margin_amount"] += values["margin_amount"]
                    prior_totals["revenue_amount"] += prior_values["revenue_amount"]
                    prior_totals["passenger_count"] += prior_values["passenger_count"]
                    prior_totals["cost_amount"] += prior_values["cost_amount"]
                    prior_totals["margin_amount"] += prior_values["margin_amount"]
                    month_cells.append(
                        DestinationMatrixCell(
                            month=month,
                            revenue_amount=values["revenue_amount"],
                            passenger_count=values["passenger_count"],
                            cost_amount=values["cost_amount"],
                            margin_amount=values["margin_amount"],
                            margin_pct=self._safe_ratio(values["margin_amount"], values["revenue_amount"]),
                            revenue_yoy_pct=self._safe_yoy_ratio(
                                values["revenue_amount"], prior_values["revenue_amount"]
                            ),
                            passenger_yoy_pct=self._safe_yoy_ratio(
                                values["passenger_count"], prior_values["passenger_count"]
                            ),
                            cost_yoy_pct=self._safe_yoy_ratio(
                                values["cost_amount"], prior_values["cost_amount"]
                            ),
                            margin_yoy_pct=self._safe_yoy_ratio(
                                values["margin_amount"], prior_values["margin_amount"]
                            ),
                        )
                    )
                city_matrix.append(
                    DestinationCityMatrixRow(
                        city=city_name,
                        months=month_cells,
                        totals=DestinationMatrixTotals(
                            revenue_amount=current_totals["revenue_amount"],
                            passenger_count=current_totals["passenger_count"],
                            cost_amount=current_totals["cost_amount"],
                            margin_amount=current_totals["margin_amount"],
                            revenue_yoy_pct=self._safe_yoy_ratio(
                                current_totals["revenue_amount"], prior_totals["revenue_amount"]
                            ),
                            passenger_yoy_pct=self._safe_yoy_ratio(
                                current_totals["passenger_count"], prior_totals["passenger_count"]
                            ),
                            cost_yoy_pct=self._safe_yoy_ratio(
                                current_totals["cost_amount"], prior_totals["cost_amount"]
                            ),
                            margin_yoy_pct=self._safe_yoy_ratio(
                                current_totals["margin_amount"], prior_totals["margin_amount"]
                            ),
                        ),
                    )
                )

        return ItineraryDestinationMatrixResponse(
            year=year,
            country=country,
            months=months,
            country_matrix=country_matrix,
            city_matrix=city_matrix,
        )

    @staticmethod
    def _to_date(value: object) -> Optional[date]:
        if isinstance(value, date):
            return value
        if isinstance(value, str):
            try:
                return date.fromisoformat(value)
            except ValueError:
                return None
        return None

    @staticmethod
    def _safe_ratio(numerator: float, denominator: float) -> float:
        if denominator <= 0:
            return 0.0
        return numerator / denominator

    @staticmethod
    def _safe_yoy_ratio(current_value: float, prior_value: float) -> Optional[float]:
        if prior_value == 0:
            return None
        return (current_value - prior_value) / abs(prior_value)

    def _aggregate_totals(self, rows: List[dict]) -> Dict[str, float]:
        totals = {
            "active_item_count": 0.0,
            "booked_itineraries_count": 0.0,
            "booked_total_price": 0.0,
            "booked_total_cost": 0.0,
            "booked_gross_margin": 0.0,
        }
        for row in rows:
            totals["active_item_count"] += float(row.get("active_item_count") or 0.0)
            totals["booked_itineraries_count"] += float(row.get("booked_itinerary_count") or 0.0)
            totals["booked_total_price"] += float(row.get("booked_total_price") or 0.0)
            totals["booked_total_cost"] += float(row.get("booked_total_cost") or 0.0)
            totals["booked_gross_margin"] += float(row.get("booked_gross_margin") or 0.0)
        return totals

    def _aggregate_by_country(self, rows: List[dict]) -> Dict[str, Dict[str, float]]:
        country_rollups: Dict[str, Dict[str, float]] = defaultdict(
            lambda: {
                "active_item_count": 0.0,
                "booked_itineraries_count": 0.0,
                "booked_total_price": 0.0,
                "booked_gross_margin": 0.0,
            }
        )
        for row in rows:
            country = str(row.get("location_country") or "Unknown")
            bucket = country_rollups[country]
            bucket["active_item_count"] += float(row.get("active_item_count") or 0.0)
            bucket["booked_itineraries_count"] += float(row.get("booked_itinerary_count") or 0.0)
            bucket["booked_total_price"] += float(row.get("booked_total_price") or 0.0)
            bucket["booked_gross_margin"] += float(row.get("booked_gross_margin") or 0.0)
        return country_rollups

    def _to_country_summary_point(
        self,
        country: str,
        values: Dict[str, float],
        total_booked_total_price: float,
    ) -> DestinationCountrySummaryPoint:
        return DestinationCountrySummaryPoint(
            country=country,
            active_item_count=int(round(values["active_item_count"])),
            booked_itineraries_count=int(round(values["booked_itineraries_count"])),
            booked_total_price=values["booked_total_price"],
            booked_gross_margin=values["booked_gross_margin"],
            booked_margin_pct=self._safe_ratio(values["booked_gross_margin"], values["booked_total_price"]),
            booked_share_pct=self._safe_ratio(values["booked_total_price"], total_booked_total_price),
        )
