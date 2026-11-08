"""Правила очистки и обогащения: SQL-функции и состояние core после загрузки."""

import pytest

from tests.integration.conftest import scalar

pytestmark = pytest.mark.integration


@pytest.mark.parametrize("weight, expected", [
    (38.0, "G20T"), (30.0, "G20T"), (29.96, "G20T"), (29.94, "FTL"), (10.0, "FTL"), (9.96, "FTL"),
    (5.0, "10T"), (4.9, "5T"), (2.5, "5T"), (2.44, "L5T"), (0.5, "L5T"), (0.45, "L5T"), (0.44, "OTH"), (0.0, "OTH"),
])
def test_weight_category_boundaries(dwh, weight, expected):
    assert scalar(dwh, "SELECT core.weight_category(%s)", (weight,)) == expected


@pytest.mark.parametrize("volume, expected", [(0, "OTH"), (0.1, "FTL"), (120, "FTL"), (120.5, "OTH"), (150, "OTH")])
def test_volume_category(dwh, volume, expected):
    assert scalar(dwh, "SELECT core.volume_category(%s)", (volume,)) == expected


@pytest.mark.parametrize("body_type, code", [
    (56, 56), (57, 56), (61, 56), (4, 4), (46, 4), (48, 4), (2, 2), (1, 15), (15, 15), (3, 0), (50, 0), (None, -1),
])
def test_body_group_mapping(dwh, body_type, code):
    assert scalar(dwh, "SELECT core.body_group_code(%s)", (body_type,)) == code


def test_cities_deduplicated_by_min_id(dwh):
    assert scalar(dwh, "SELECT count(*) - count(DISTINCT gc_id) FROM core.dim_city") == 0
    row = dwh.execute("SELECT city_name, region_name, source_rows FROM core.dim_city WHERE gc_id = 1003").fetchone()
    assert row == ("Казань", "Республика Татарстан", 2)
    assert scalar(dwh, "SELECT city_id = (SELECT min(id) FROM raw.cities WHERE gc_id = 1001) "
                       "FROM core.dim_city WHERE gc_id = 1001")


def test_no_invalid_rows_in_core(dwh):
    bad = scalar(dwh, """
        SELECT count(*) FROM core.fact_price
        WHERE price_source <= 0 OR (currency_code = '643' AND price_source <= 1000) OR distance <= 0
           OR total_weight < 0 OR total_volume < 0 OR price_rub IS NULL
    """)
    assert bad == 0


def test_every_raw_row_is_loaded_or_quarantined(dwh):
    assert scalar(dwh, """
        SELECT count(*) FROM raw.prices p
        WHERE (EXISTS (SELECT 1 FROM core.fact_price f WHERE f.price_id = p.id))
            = (EXISTS (SELECT 1 FROM core.dq_quarantine_prices q WHERE q.price_id = p.id))
    """) == 0


def test_quarantine_has_expected_reasons(dwh):
    reasons = {r for (r,) in dwh.execute("SELECT DISTINCT reason_code FROM core.dq_quarantine_prices")}
    assert {"PRICE_NON_POSITIVE", "FX_RATE_MISSING"} <= reasons


def test_foreign_currency_converted_with_exact_rate(dwh):
    assert scalar(dwh, """
        SELECT count(*) FROM core.fact_price f
        LEFT JOIN core.fx_rate r ON r.currency_code = f.currency_code AND r.rate_date = f.fx_rate_date
        WHERE f.currency_code <> '643'
          AND (r.rate IS NULL OR f.fx_rate_date IS DISTINCT FROM (SELECT nearest_rate_date FROM raw.prices WHERE id = f.price_id)
               OR abs(f.price_rub - round((f.price_source * r.rate)::numeric, 2)) > 0.01)
    """) == 0
    assert scalar(dwh, "SELECT count(*) FROM core.fact_price WHERE currency_code <> '643'") > 0


def test_body_groups_unique_per_price_and_cover_all_prices(dwh):
    assert scalar(dwh, "SELECT count(*) FROM core.fact_price f WHERE NOT EXISTS "
                       "(SELECT 1 FROM core.fact_price_body_group g WHERE g.price_id = f.price_id)") == 0
    # дочерние коды одной группы (56 + 61, 4 + 46) схлопнуты: PK (price_id, body_group_code) это гарантирует,
    # проверяем, что сделки с двумя кодами одной группы дают одну строку
    assert scalar(dwh, """
        SELECT count(*) FROM (
            SELECT b.price_id FROM raw.price_body_types b
            JOIN core.fact_price f ON f.price_id = b.price_id
            GROUP BY b.price_id HAVING count(DISTINCT core.body_group_code(b.body_type_id)) = 1 AND count(*) > 1
        ) multi
        WHERE (SELECT count(*) FROM core.fact_price_body_group g WHERE g.price_id = multi.price_id) <> 1
    """) == 0
