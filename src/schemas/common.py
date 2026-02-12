from __future__ import annotations

from typing import Optional

from src.shared.base import BaseSchema


class Lineage(BaseSchema):
    source_system: str
    source_record_id: Optional[str] = None
    ingested_at: Optional[str] = None
