"""Витрина маршрутов и результаты пакетного скоринга."""

from datetime import date

import pytest

from etl import config
from tests.integration.conftest import scalar

pytestmark = pytest.mark.integration


def test_route_card_grain_is_unique(dwh):
    assert scalar(dwh, """
        SELECT count(*) - count(DISTINCT (month, loading_city_gcid, unloading_city_gcid, body_group_code, weight_category))
        FROM mart.mart_route_card
    """) == 0
    assert scalar(dwh, "SELECT count(*) FROM mart.mart_route_card") > 0


def test_route_card_has_no_future_or_invalid_cells(dwh):
    as_of = config.as_of()
    assert scalar(dwh, "SELECT max(last_deal_date) FROM mart.mart_route_card") <= as_of
    assert scalar(dwh, "SELECT max(month) FROM mart.mart_route_card") == date(as_of.year, as_of.month, 1)
    assert scalar(dwh, """
        SELECT count(*) FROM mart.mart_route_card
        WHERE NOT (p25_trip <= median_trip AND median_trip <= p75_trip AND p25_km <= median_km AND median_km <= p75_km)
           OR trips_count < 1 OR carriers_count > trips_count
    """) == 0


def test_route_card_matches_independent_recount(dwh):
    """Для одной ячейки пересчитываем trips_count и медиану прямо из core и сверяем с витриной."""
    cell = dwh.execute("""
        SELECT month, loading_city_gcid, unloading_city_gcid, body_group_code, weight_category, trips_count, median_trip
        FROM mart.mart_route_card ORDER BY trips_count DESC, month DESC LIMIT 1
    """).fetchone()
    recount = dwh.execute("""
        SELECT count(DISTINCT f.price_id),
               round(percentile_cont(0.5) WITHIN GROUP (ORDER BY f.price_rub)::numeric, 2)
        FROM core.fact_price f
        JOIN core.fact_price_body_group g ON g.price_id = f.price_id
        WHERE f.is_fact AND NOT f.complex AND NOT f.outlier_mark AND f.distance > 0 AND f.price_source > 0
          AND f.loading_city_gcid = %s AND f.unloading_city_gcid = %s AND g.body_group_code = %s
          AND f.weight_category = %s
          AND EXISTS (SELECT 1 FROM core.fact_price_date d WHERE d.price_id = f.price_id
                      AND date_trunc('month', d.price_date) = %s AND d.price_date <= %s)
    """, (cell[1], cell[2], cell[3], cell[4], cell[0], config.as_of())).fetchone()
    assert recount == (cell[5], cell[6])


def test_mom_yoy_use_strict_calendar_neighbours(dwh):
    # если календарного предыдущего месяца нет, MoM обязан быть NULL
    assert scalar(dwh, """
        SELECT count(*) FROM mart.mart_route_card c
        WHERE c.mom_pct IS NOT NULL AND NOT EXISTS (
            SELECT 1 FROM mart.mart_route_card p
            WHERE (p.loading_city_gcid, p.unloading_city_gcid, p.body_group_code, p.weight_category)
                = (c.loading_city_gcid, c.unloading_city_gcid, c.body_group_code, c.weight_category)
              AND p.month = (c.month - interval '1 month')::date)
    """) == 0
    assert scalar(dwh, """
        SELECT count(*) FROM mart.mart_route_card c
        JOIN mart.mart_route_card p
          ON (p.loading_city_gcid, p.unloading_city_gcid, p.body_group_code, p.weight_category)
           = (c.loading_city_gcid, c.unloading_city_gcid, c.body_group_code, c.weight_category)
         AND p.month = (c.month - interval '12 months')::date
        WHERE c.yoy_pct IS NULL OR abs(c.yoy_pct - (c.median_trip - p.median_trip) / p.median_trip * 100) > 0.05
    """) == 0


def test_reliability_levels(dwh):
    assert scalar(dwh, """
        SELECT count(*) FROM mart.mart_route_card
        WHERE reliability_level <> CASE
            WHEN trips_count >= 30 AND carriers_count >= 5 AND freshness_days <= 30 THEN 'HIGH'
            WHEN trips_count >= 10 THEN 'MEDIUM' ELSE 'LOW_DATA' END
    """) == 0


def test_cv_rate_null_for_single_trip(dwh):
    assert scalar(dwh, "SELECT count(*) FROM mart.mart_route_card WHERE trips_count < 2 AND cv_rate IS NOT NULL") == 0


def test_predictions_written_for_test_and_future(dwh):
    as_of = config.as_of()
    rows = dict(dwh.execute(
        "SELECT scoring_period, count(*) FROM mart.ml_price_predictions GROUP BY 1").fetchall())
    assert rows.get("test", 0) > 0 and rows.get("future", 0) > 0
    assert scalar(dwh, """
        SELECT count(*) FROM mart.ml_price_predictions
        WHERE scoring_period = 'future'
          AND (actual_price_rub IS NOT NULL OR abs_error_rub IS NOT NULL OR ape_pct IS NOT NULL OR loading_date <= %s)
    """, (as_of,)) == 0
    assert scalar(dwh, "SELECT count(*) FROM mart.ml_price_predictions WHERE scoring_period = 'test' "
                       "AND (actual_price_rub IS NULL OR loading_date > %s)", (as_of,)) == 0


def test_model_metrics_recorded(dwh):
    metrics = scalar(dwh, "SELECT metrics FROM mart.ml_model_runs WHERE model_name = 'catboost_v1' AND as_of = %s",
                     (config.as_of(),))
    assert {"r2", "mape_pct", "rmse_rub", "mae_rub"} <= set(metrics)
    assert metrics["r2"] > 0.5
