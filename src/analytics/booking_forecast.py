from __future__ import annotations

from collections import defaultdict
from datetime import date
from statistics import mean, pstdev
from typing import Dict, Iterable, List

from src.models.revenue_bookings import BookingRecord
from src.schemas.revenue_bookings import BookingForecastPoint


def _month_start(value: date) -> date:
    return date(value.year, value.month, 1)


def forecast_bookings(
    bookings: Iterable[BookingRecord], lookback_months: int, horizon_months: int
) -> List[BookingForecastPoint]:
    monthly_counts: Dict[date, int] = defaultdict(int)
    for booking in bookings:
        if booking.service_start_date:
            monthly_counts[_month_start(booking.service_start_date)] += 1

    sorted_months = sorted(monthly_counts.keys())[-lookback_months:]
    history = [monthly_counts[month] for month in sorted_months]
    if not history:
        return []

    avg = mean(history)
    variance = pstdev(history) if len(history) > 1 else 0.0
    confidence = max(0.2, min(0.9, 1.0 - (variance / avg))) if avg > 0 else 0.3

    last_month = sorted_months[-1]
    forecast: List[BookingForecastPoint] = []
    year = last_month.year
    month = last_month.month
    for _ in range(horizon_months):
        month += 1
        if month > 12:
            month = 1
            year += 1
        forecast.append(
            BookingForecastPoint(
                period_start=date(year, month, 1),
                projected_bookings=int(round(avg)),
                confidence=confidence,
            )
        )
    return forecast
