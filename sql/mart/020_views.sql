-- Представления для двух сценариев BI и пост-анализа ML.

-- Сводная таблица маршрутной сети (тендеры, бюджетирование): последний месяц по каждой ячейке.
CREATE OR REPLACE VIEW mart.v_route_network_latest AS
SELECT DISTINCT ON (loading_city_gcid, unloading_city_gcid, body_group_code, weight_category) *
FROM mart.mart_route_card
ORDER BY loading_city_gcid, unloading_city_gcid, body_group_code, weight_category, month DESC;

-- Факт vs Прогноз с атрибутами маршрута и рыночным коридором месяца погрузки.
CREATE OR REPLACE VIEW mart.v_ml_fact_vs_forecast AS
SELECT p.*,
       lc.city_name AS loading_city, uc.city_name AS unloading_city,
       r.p25_trip, r.median_trip, r.p75_trip, r.reliability_level,
       CASE WHEN p.actual_price_rub IS NOT NULL AND r.p75_trip IS NOT NULL
            THEN p.actual_price_rub > r.p75_trip END AS above_market_p75
FROM mart.ml_price_predictions p
LEFT JOIN core.dim_city lc ON lc.gc_id = p.loading_city_gcid
LEFT JOIN core.dim_city uc ON uc.gc_id = p.unloading_city_gcid
LEFT JOIN core.fact_price f ON f.price_id = p.price_id
LEFT JOIN mart.mart_route_card r
       ON r.month = date_trunc('month', p.loading_date)::date
      AND r.loading_city_gcid = p.loading_city_gcid AND r.unloading_city_gcid = p.unloading_city_gcid
      AND r.body_group_code = f.primary_body_group_code AND r.weight_category = f.weight_category;
