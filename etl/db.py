"""Подключения к БД и выполнение SQL-файлов."""

from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import Any

import psycopg

from etl.config import DbConfig

log = logging.getLogger(__name__)


def connect(cfg: DbConfig, retries: int = 30, delay: float = 2.0) -> psycopg.Connection:
    """Подключение с повторами: в docker-compose БД может ещё подниматься."""
    last_error: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            # ClientCursor: параметры подставляются на клиенте, что позволяет
            # выполнять SQL-файлы из нескольких выражений с именованными параметрами.
            return psycopg.connect(cfg.conninfo, cursor_factory=psycopg.ClientCursor)
        except psycopg.OperationalError as exc:
            last_error = exc
            log.info("db=%s not ready (attempt %d/%d): %s", cfg.dbname, attempt, retries, exc)
            time.sleep(delay)
    raise RuntimeError(f"Не удалось подключиться к {cfg.dbname}") from last_error


def run_sql_file(conn: psycopg.Connection, path: Path, params: dict[str, Any] | None = None) -> None:
    sql = path.read_text(encoding="utf-8")
    # параметры передаём только файлам с плейсхолдерами: в остальных '%' не экранируется
    use_params = params if params and "%(" in sql else None
    with conn.cursor() as cur:
        cur.execute(sql, use_params)
    log.debug("executed %s", path.name)
