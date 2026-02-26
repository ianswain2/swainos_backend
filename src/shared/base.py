from __future__ import annotations

from decimal import Decimal

from pydantic import BaseModel, model_serializer
from pydantic.config import ConfigDict


def to_camel(value: str) -> str:
    parts = value.split("_")
    return parts[0] + "".join(word.capitalize() for word in parts[1:])


class BaseSchema(BaseModel):
    model_config = ConfigDict(
        alias_generator=to_camel,
        populate_by_name=True,
    )

    @model_serializer(mode="wrap")
    def _serialize_model(self, handler):  # type: ignore[no-untyped-def]
        return _convert_decimals(handler(self))


def _convert_decimals(value):  # type: ignore[no-untyped-def]
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, list):
        return [_convert_decimals(item) for item in value]
    if isinstance(value, dict):
        return {key: _convert_decimals(item) for key, item in value.items()}
    return value
