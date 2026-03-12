from __future__ import annotations

import json
import logging
import sys
from typing import Any, Optional

from src.core.request_context import get_request_id


class RequestIdFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        record.request_id = get_request_id() or "-"
        return True


class JsonFormatter(logging.Formatter):
    _reserved_keys = set(logging.makeLogRecord({}).__dict__.keys())

    @staticmethod
    def _to_json_safe(value: Any) -> Any:
        if value is None or isinstance(value, (str, int, float, bool)):
            return value
        if isinstance(value, dict):
            return {str(k): JsonFormatter._to_json_safe(v) for k, v in value.items()}
        if isinstance(value, (list, tuple)):
            return [JsonFormatter._to_json_safe(item) for item in value]
        return str(value)

    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "timestamp": self.formatTime(record, self.datefmt),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "requestId": getattr(record, "request_id", "-"),
        }
        extra_payload = {
            key: self._to_json_safe(value)
            for key, value in record.__dict__.items()
            if key not in self._reserved_keys and key != "request_id"
        }
        if extra_payload:
            payload["context"] = extra_payload
        return json.dumps(payload, ensure_ascii=True)


def configure_logging(level: Optional[str] = None) -> None:
    log_level = level or "INFO"
    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    root_logger.setLevel(log_level.upper())

    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(log_level.upper())
    handler.addFilter(RequestIdFilter())
    handler.setFormatter(JsonFormatter())
    root_logger.addHandler(handler)
