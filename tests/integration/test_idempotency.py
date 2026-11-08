"""Повторный запуск ETL и витрин за уже загруженный период не меняет ни строк, ни сумм, ни перцентилей."""

from datetime import date

import pytest

from etl.marts import refresh_marts
from etl.run import run_window

pytestmark = pytest.mark.integration

SNAPSHOT_SQL = """
SELECT
  (SELECT count(*) FROM raw.prices),
  (SELECT count(*) FROM core.fact_price),
  (SELECT sum(price_rub) FROM core.fact_price),
  (SELECT count(*) FROM core.fact_price_body_group),
  (SELECT count(*) FROM core.fact_price_date),
  (SELECT count(*) FROM core.dq_quarantine_prices),
  (SELECT count(*) FROM mart.mart_route_card),
  (SELECT sum(trips_count) FROM mart.mart_route_card),
  (SELECT sum(median_trip) FROM mart.mart_route_card),
  (SELECT sum(p75_km) FROM mart.mart_route_card)
"""


def test_rerun_same_window_is_idempotent(dwh):
    before = dwh.execute(SNAPSHOT_SQL).fetchone()
    # окно в середине истории + окно с плановыми заявками после as_of
    for start, end in ((date(2026, 9, 1), date(2026, 9, 30)), (date(2026, 10, 20), date(2026, 11, 15))):
        first = run_window(start, end)
        second = run_window(start, end)
        assert first["rows_loaded_core"] == second["rows_loaded_core"]
        assert first["rows_quarantined"] == second["rows_quarantined"]
    refresh_marts()
    refresh_marts()
    after = dwh.execute(SNAPSHOT_SQL).fetchone()
    assert before == after
