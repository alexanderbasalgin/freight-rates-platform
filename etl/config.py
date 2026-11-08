"""Конфигурация из переменных окружения (см. .env.example). Паролей и абсолютных путей в коде нет."""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import date
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SQL_DIR = PROJECT_ROOT / "sql"


def _env(name: str, default: str | None = None) -> str:
    value = os.getenv(name, default)
    if value is None:
        raise RuntimeError(f"Не задана переменная окружения {name}")
    return value


@dataclass(frozen=True)
class DbConfig:
    host: str
    port: int
    dbname: str
    user: str
    password: str

    @property
    def conninfo(self) -> str:
        return (
            f"host={self.host} port={self.port} dbname={self.dbname} "
            f"user={self.user} password={self.password} application_name=freight-rates-platform"
        )


def source_db() -> DbConfig:
    return DbConfig(
        host=_env("SOURCE_DB_HOST", "localhost"),
        port=int(_env("SOURCE_DB_PORT", "5432")),
        dbname=_env("SOURCE_DB_NAME", "deals_bigdata"),
        user=_env("SOURCE_DB_USER", "postgres"),
        password=_env("SOURCE_DB_PASSWORD"),
    )


def dwh_db() -> DbConfig:
    return DbConfig(
        host=_env("DWH_DB_HOST", "localhost"),
        port=int(_env("DWH_DB_PORT", "5432")),
        dbname=_env("DWH_DB_NAME", "dwh"),
        user=_env("DWH_DB_USER", "postgres"),
        password=_env("DWH_DB_PASSWORD"),
    )


def as_of() -> date:
    """Дата снимка данных. Это не системная дата: от неё считаются будущее, свежесть и границы ML."""
    return date.fromisoformat(_env("AS_OF", "2026-10-26"))


def random_seed() -> int:
    return int(_env("RANDOM_SEED", "42"))


def model_dir() -> Path:
    path = Path(_env("MODEL_DIR", str(PROJECT_ROOT / "artifacts")))
    path.mkdir(parents=True, exist_ok=True)
    return path
