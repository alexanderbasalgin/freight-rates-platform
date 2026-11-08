-- Полный пересчёт витрины mart.mart_route_card на дату снимка %(as_of)s.
-- Зерно: месяц (price_dates.price_date) × город отправления × город назначения × группа кузова × весовая категория.
-- Выполняется в транзакции: DELETE + INSERT атомарны, повторный запуск даёт тот же результат.
-- Перцентили не агрегируются инкрементально, поэтому витрина считается целиком
-- (на 300 тыс. сделок это секунды; для больших объёмов - пересчёт только затронутых месяцев).

DELETE FROM mart.mart_route_card;

INSERT INTO mart.mart_route_card (
    month, loading_city_gcid, unloading_city_gcid, body_group_code, weight_category,
    loading_city, loading_region, unloading_city, unloading_region, body_group,
    p25_trip, median_trip, p75_trip, avg_trip, p25_km, median_km, p75_km,
    trips_count, carriers_count, cv_rate, mom_pct, yoy_pct,
    first_deal_date, last_deal_date, freshness_days, reliability_level, as_of)
WITH market AS (
    -- рыночный факт: завершённые прямые перевозки без выбросов, только прошлые даты
    SELECT f.price_id,
           date_trunc('month', d.price_date)::date AS month,
           d.price_date,
           f.loading_city_gcid, f.unloading_city_gcid, bg.body_group_code, f.weight_category,
           f.price_rub, f.price_rub / nullif(f.distance, 0)::numeric AS price_km,
           f.price_type, f.organization_id
    FROM core.fact_price f
    JOIN core.fact_price_date d ON d.price_id = f.price_id
    JOIN core.fact_price_body_group bg ON bg.price_id = f.price_id
    WHERE f.is_fact
      AND f.price_type IN (1, 2, 3, 4, 5, 9)
      AND NOT f.complex
      AND f.price_source > 0
      AND f.distance > 0
      AND NOT f.outlier_mark
      AND d.price_date <= %(as_of)s
),
deal_month AS (
    -- одна сделка учитывается в ячейке месяца один раз
    SELECT DISTINCT ON (price_id, month, body_group_code) *
    FROM market
    ORDER BY price_id, month, body_group_code, price_date DESC
),
cells AS (
    SELECT month, loading_city_gcid, unloading_city_gcid, body_group_code, weight_category,
           percentile_cont(0.25) WITHIN GROUP (ORDER BY price_rub) AS p25_trip,
           percentile_cont(0.50) WITHIN GROUP (ORDER BY price_rub) AS median_trip,
           percentile_cont(0.75) WITHIN GROUP (ORDER BY price_rub) AS p75_trip,
           avg(price_rub)                                          AS avg_trip,
           stddev_samp(price_rub)                                  AS std_trip,
           percentile_cont(0.25) WITHIN GROUP (ORDER BY price_km)  AS p25_km,
           percentile_cont(0.50) WITHIN GROUP (ORDER BY price_km)  AS median_km,
           percentile_cont(0.75) WITHIN GROUP (ORDER BY price_km)  AS p75_km,
           count(DISTINCT price_id)                                AS trips_count,
           count(DISTINCT organization_id) FILTER (WHERE price_type IN (3, 9)) AS carriers_count,
           min(price_date)                                         AS first_deal_date,
           max(price_date)                                         AS last_deal_date
    FROM deal_month
    GROUP BY month, loading_city_gcid, unloading_city_gcid, body_group_code, weight_category
)
SELECT c.month, c.loading_city_gcid, c.unloading_city_gcid, c.body_group_code, c.weight_category,
       lc.city_name, lc.region_name, uc.city_name, uc.region_name, g.body_group,
       round(c.p25_trip::numeric, 2), round(c.median_trip::numeric, 2), round(c.p75_trip::numeric, 2),
       round(c.avg_trip, 2),
       round(c.p25_km::numeric, 2), round(c.median_km::numeric, 2), round(c.p75_km::numeric, 2),
       c.trips_count, c.carriers_count,
       CASE WHEN c.trips_count >= 2 AND c.avg_trip <> 0 THEN round(c.std_trip / c.avg_trip, 4) END,
       -- MoM / YoY: строго календарный предыдущий месяц и тот же месяц год назад, без подстановки
       CASE WHEN pm.median_trip <> 0
            THEN round(((c.median_trip - pm.median_trip) / pm.median_trip * 100)::numeric, 2) END,
       CASE WHEN py.median_trip <> 0
            THEN round(((c.median_trip - py.median_trip) / py.median_trip * 100)::numeric, 2) END,
       c.first_deal_date, c.last_deal_date,
       (%(as_of)s::date - c.last_deal_date),
       CASE
           WHEN c.trips_count >= 30 AND c.carriers_count >= 5 AND (%(as_of)s::date - c.last_deal_date) <= 30 THEN 'HIGH'
           WHEN c.trips_count >= 10 THEN 'MEDIUM'
           ELSE 'LOW_DATA'
       END,
       %(as_of)s::date
FROM cells c
JOIN core.dim_body_group g ON g.body_group_code = c.body_group_code
LEFT JOIN core.dim_city lc ON lc.gc_id = c.loading_city_gcid
LEFT JOIN core.dim_city uc ON uc.gc_id = c.unloading_city_gcid
LEFT JOIN cells pm
       ON pm.loading_city_gcid = c.loading_city_gcid AND pm.unloading_city_gcid = c.unloading_city_gcid
      AND pm.body_group_code = c.body_group_code AND pm.weight_category = c.weight_category
      AND pm.month = (c.month - interval '1 month')::date
LEFT JOIN cells py
       ON py.loading_city_gcid = c.loading_city_gcid AND py.unloading_city_gcid = c.unloading_city_gcid
      AND py.body_group_code = c.body_group_code AND py.weight_category = c.weight_category
      AND py.month = (c.month - interval '12 months')::date;
