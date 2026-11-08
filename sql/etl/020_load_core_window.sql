-- Загрузка окна [start; end] по loading_date из raw в core.
-- Параметры: %(start)s, %(end)s, %(run_id)s. Выполняется в одной транзакции с выгрузкой raw,
-- поэтому повторный запуск за то же окно атомарно перезаписывает данные (DELETE + INSERT).

-- 1. Перезапись окна в core (дочерние таблицы чистятся каскадом).
--    Чистим и по id из нового батча: если в источнике у сделки сменилась loading_date,
--    старая версия из другого окна не останется дублем.
DELETE FROM core.fact_price
WHERE loading_date BETWEEN %(start)s AND %(end)s
   OR price_id IN (SELECT id FROM raw.prices WHERE loading_date BETWEEN %(start)s AND %(end)s);
DELETE FROM core.dq_quarantine_prices
WHERE loading_date BETWEEN %(start)s AND %(end)s
   OR price_id IN (SELECT id FROM raw.prices WHERE loading_date BETWEEN %(start)s AND %(end)s);

-- 2. Правила качества данных. Одна сделка может нарушать несколько правил: сохраняем все причины.
CREATE TEMP TABLE _dq ON COMMIT DROP AS
SELECT p.id AS price_id, r.reason_code, r.reason_details
FROM raw.prices p
LEFT JOIN core.dim_city lc ON lc.gc_id = p.loading_city_gcid
LEFT JOIN core.dim_city uc ON uc.gc_id = p.unloading_city_gcid
LEFT JOIN core.fx_rate fx ON fx.currency_code = p.currency_code AND fx.rate_date = p.nearest_rate_date
CROSS JOIN LATERAL (VALUES
    ('PRICE_NON_POSITIVE',     p.price IS NULL OR p.price <= 0,
                               'price = ' || coalesce(p.price::text, 'NULL')),
    ('RUB_PRICE_BELOW_MIN',    p.currency_code = '643' AND p.price > 0 AND p.price <= 1000,
                               'RUB price <= 1000: ' || p.price::text),
    ('DISTANCE_NON_POSITIVE',  p.distance IS NULL OR p.distance <= 0,
                               'distance = ' || coalesce(p.distance::text, 'NULL')),
    ('WEIGHT_NEGATIVE',        p.total_weight < 0, 'total_weight = ' || p.total_weight::text),
    ('VOLUME_NEGATIVE',        p.total_volume < 0, 'total_volume = ' || p.total_volume::text),
    ('LOADING_CITY_UNKNOWN',   lc.gc_id IS NULL, 'loading_city_gcid = ' || p.loading_city_gcid::text),
    ('UNLOADING_CITY_UNKNOWN', uc.gc_id IS NULL, 'unloading_city_gcid = ' || p.unloading_city_gcid::text),
    ('CURRENCY_UNKNOWN',       p.currency_code IS NULL OR p.currency_code NOT IN ('643', '840', '978'),
                               'currency_code = ' || coalesce(p.currency_code, 'NULL')),
    ('FX_RATE_MISSING',        p.currency_code IN ('840', '978') AND fx.rate IS NULL,
                               'no exact rate for ' || p.currency_code || ' on nearest_rate_date = '
                               || coalesce(p.nearest_rate_date::text, 'NULL'))
) AS r (reason_code, failed, reason_details)
WHERE p.loading_date BETWEEN %(start)s AND %(end)s
  AND r.failed;

INSERT INTO core.dq_quarantine_prices (price_id, reason_code, reason_details, loading_date, raw_row, run_id)
SELECT d.price_id, d.reason_code, d.reason_details, p.loading_date,
       to_jsonb(p) - '_run_id' - '_extracted_at', %(run_id)s
FROM _dq d
JOIN raw.prices p ON p.id = d.price_id;

-- 3. Нормализованные факты: конвертация в RUB по точному курсу на nearest_rate_date,
--    заполнение пропусков в весовой и объёмной категориях.
INSERT INTO core.fact_price (
    price_id, created_at, loading_date, end_loading_date, loading_city_gcid, unloading_city_gcid,
    subscriber_id, organization_id, price_type, is_fact, complex, outlier_mark,
    distance, total_weight, total_volume, body_type, primary_body_group_code,
    weight_category, weight_category_filled, volume_category, volume_category_filled,
    currency_code, price_source, fx_rate, fx_rate_date, price_rub, source, run_id)
SELECT p.id, p.created_at, p.loading_date, p.end_loading_date, p.loading_city_gcid, p.unloading_city_gcid,
       p.subscriber_id, p.organization_id, p.price_type, p.is_fact, coalesce(p.complex, false),
       coalesce(p.outlier_mark, false),
       p.distance, p.total_weight, p.total_volume, p.body_type,
       -- основная группа кузова для ML: prices.body_type, затем первый код из price_body_types, затем «Не указан»
       CASE WHEN p.body_type IS NOT NULL THEN core.body_group_code(p.body_type)
            ELSE coalesce((SELECT min(core.body_group_code(b.body_type_id))
                           FROM raw.price_body_types b WHERE b.price_id = p.id), -1)
       END,
       coalesce(p.weight_category, core.weight_category(p.total_weight)), p.weight_category IS NULL,
       coalesce(p.volume_category, core.volume_category(p.total_volume)), p.volume_category IS NULL,
       p.currency_code, p.price,
       CASE WHEN p.currency_code = '643' THEN 1.0 ELSE fx.rate END,
       CASE WHEN p.currency_code = '643' THEN p.nearest_rate_date ELSE fx.rate_date END,
       round((p.price * CASE WHEN p.currency_code = '643' THEN 1.0 ELSE fx.rate END)::numeric, 2),
       p.source, %(run_id)s
FROM raw.prices p
LEFT JOIN core.fx_rate fx ON fx.currency_code = p.currency_code AND fx.rate_date = p.nearest_rate_date
WHERE p.loading_date BETWEEN %(start)s AND %(end)s
  AND NOT EXISTS (SELECT 1 FROM _dq d WHERE d.price_id = p.id);

-- 4. Группы кузова: из price_body_types (несколько кодов одной группы схлопываются в одну строку);
--    если связей нет -> prices.body_type; если и его нет -> «Не указан» (-1).
INSERT INTO core.fact_price_body_group (price_id, body_group_code)
SELECT DISTINCT f.price_id, core.body_group_code(b.body_type_id)
FROM core.fact_price f
JOIN raw.price_body_types b ON b.price_id = f.price_id
WHERE f.loading_date BETWEEN %(start)s AND %(end)s
UNION
SELECT f.price_id, core.body_group_code(f.body_type)
FROM core.fact_price f
WHERE f.loading_date BETWEEN %(start)s AND %(end)s
  AND NOT EXISTS (SELECT 1 FROM raw.price_body_types b WHERE b.price_id = f.price_id);

-- 5. Помесячная развёртка: из price_dates; если её нет, сделка живёт в месяце loading_date.
INSERT INTO core.fact_price_date (price_id, price_date)
SELECT d.price_id, d.price_date
FROM core.fact_price f
JOIN raw.price_dates d ON d.price_id = f.price_id
WHERE f.loading_date BETWEEN %(start)s AND %(end)s
UNION
SELECT f.price_id, f.loading_date
FROM core.fact_price f
WHERE f.loading_date BETWEEN %(start)s AND %(end)s
  AND NOT EXISTS (SELECT 1 FROM raw.price_dates d WHERE d.price_id = f.price_id);
