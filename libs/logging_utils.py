from __future__ import annotations

import logging
import os
from typing import Optional

_CONFIGURED = False


def configure_logging(level: str | int | None = None) -> logging.Logger:
    """Configure root logging once with sane defaults."""
    global _CONFIGURED
    if _CONFIGURED:
        return logging.getLogger()
    level_value: int
    if level is None:
        level_value = getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO)
    elif isinstance(level, str):
        level_value = getattr(logging, level.upper(), logging.INFO)
    else:
        level_value = level
    logging.basicConfig(
        level=level_value,
        format="%(asctime)s %(levelname)s %(name)s:%(lineno)d - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    _CONFIGURED = True
    return logging.getLogger()


def log_exception(logger: Optional[logging.Logger], message: str, exc: BaseException) -> None:
    target_logger = logger or logging.getLogger("app")
    target_logger.error("%s: %s", message, exc, exc_info=True)
