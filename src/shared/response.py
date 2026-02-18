from __future__ import annotations

from math import ceil
from typing import Generic, Optional, Sequence, TypeVar

from src.shared.base import BaseSchema


T = TypeVar("T")


class Pagination(BaseSchema):
    page: int
    page_size: int
    total_items: int
    total_pages: int


class Meta(BaseSchema):
    as_of_date: str
    source: str
    time_window: str
    calculation_version: str
    currency: Optional[str] = None
    data_status: Optional[str] = None
    is_stale: Optional[bool] = None
    degraded: Optional[bool] = None
    generated_at: Optional[str] = None


class ResponseEnvelope(BaseSchema, Generic[T]):
    data: T
    pagination: Optional[Pagination] = None
    meta: Optional[Meta] = None


def build_pagination(page: int, page_size: int, total_items: int) -> Pagination:
    total_pages = ceil(total_items / page_size) if page_size else 0
    return Pagination(
        page=page,
        page_size=page_size,
        total_items=total_items,
        total_pages=total_pages,
    )


def paginate_list(items: Sequence[T], page: int, page_size: int) -> tuple[list[T], Pagination]:
    total_items = len(items)
    pagination = build_pagination(page, page_size, total_items)
    start_index = (page - 1) * page_size
    end_index = start_index + page_size
    return list(items[start_index:end_index]), pagination
