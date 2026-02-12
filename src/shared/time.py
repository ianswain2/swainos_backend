from __future__ import annotations

from datetime import date, timedelta
from typing import Tuple

from src.core.errors import BadRequestError


def parse_time_window(window: str) -> Tuple[date, date]:
    today = date.today()
    try:
        if window.endswith("d"):
            days = int(window[:-1])
            return today - timedelta(days=days), today
        if window.endswith("m"):
            months = int(window[:-1])
            return today - timedelta(days=30 * months), today
    except ValueError as exc:
        raise BadRequestError("Unsupported time window format") from exc
    raise BadRequestError("Unsupported time window format")


def parse_forward_time_window(window: str) -> Tuple[date, date]:
    today = date.today()
    start = today.replace(day=1)
    try:
        if window.endswith("d"):
            days = int(window[:-1])
            return start, today + timedelta(days=days)
        if window.endswith("m"):
            months = int(window[:-1])
            if months <= 0:
                raise BadRequestError("Unsupported time window format")
            end = _add_months(start, months) - timedelta(days=1)
            return start, end
    except ValueError as exc:
        raise BadRequestError("Unsupported time window format") from exc
    raise BadRequestError("Unsupported time window format")


def _add_months(value: date, months: int) -> date:
    month_index = value.month - 1 + months
    year = value.year + month_index // 12
    month = month_index % 12 + 1
    return date(year, month, 1)
