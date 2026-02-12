from __future__ import annotations

import logging
import sys
from typing import Optional


def configure_logging(level: Optional[str] = None) -> None:
    log_level = level or "INFO"
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )
