"""Пересчёт аналитических витрин: python -m etl.marts"""

from __future__ import annotations

import json
import logging
import time

from etl import config, db
from etl.logging_setup import setup_logging

log = logging.getLogger("etl.marts")


def refresh_marts() -> dict:
    as_of = config.as_of()
    t0 = time.monotonic()
    with db.connect(config.dwh_db()) as conn:
        with conn.transaction():
            conn.execute("SELECT pg_advisory_xact_lock(hashtext('mart_refresh'))")
            for path in sorted((config.SQL_DIR / "mart").glob("*.sql")):
                db.run_sql_file(conn, path, {"as_of": as_of})
                log.info("step=marts file=%s status=applied", path.name)
            stats = dict(zip(
                ("rows", "months", "routes", "high", "medium", "low_data"),
                conn.execute(
                    """
                    SELECT count(*), count(DISTINCT month),
                           count(DISTINCT (loading_city_gcid, unloading_city_gcid)),
                           count(*) FILTER (WHERE reliability_level = 'HIGH'),
                           count(*) FILTER (WHERE reliability_level = 'MEDIUM'),
                           count(*) FILTER (WHERE reliability_level = 'LOW_DATA')
                    FROM mart.mart_route_card
                    """
                ).fetchone(),
            ))
    stats["as_of"] = str(as_of)
    stats["duration_sec"] = round(time.monotonic() - t0, 1)
    log.info("step=marts status=success table=mart.mart_route_card stats=%s", json.dumps(stats))
    return stats


if __name__ == "__main__":
    setup_logging()
    refresh_marts()
