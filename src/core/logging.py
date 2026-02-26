from __future__ import annotations

import json
import logging
import sys
from typing import Optional

from src.core.request_context import get_request_id


class RequestIdFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        record.request_id = get_request_id() or "-"
        return True


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "timestamp": self.formatTime(record, self.datefmt),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "requestId": getattr(record, "request_id", "-"),
        }
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
