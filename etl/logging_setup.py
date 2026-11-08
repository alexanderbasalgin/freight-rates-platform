"""Структурированное логирование: одна строка на событие, поля в формате key=value."""

from __future__ import annotations

import logging
import os


def setup_logging() -> None:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s level=%(levelname)s logger=%(name)s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )
