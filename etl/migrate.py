"""Идемпотентная инициализация схемы DWH: python -m etl.migrate"""

from __future__ import annotations

import logging

from etl import config, db
from etl.logging_setup import setup_logging

log = logging.getLogger("etl.migrate")


def migrate() -> None:
    files = sorted((config.SQL_DIR / "dwh").glob("*.sql"))
    with db.connect(config.dwh_db()) as conn:
        # advisory lock: параллельные запуски не будут применять DDL одновременно
        conn.execute("SELECT pg_advisory_xact_lock(hashtext('dwh_migrate'))")
        for path in files:
            db.run_sql_file(conn, path)
            log.info("step=migrate file=%s status=applied", path.name)


if __name__ == "__main__":
    setup_logging()
    migrate()
