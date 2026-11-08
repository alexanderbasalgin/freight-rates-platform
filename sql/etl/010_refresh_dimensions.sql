-- Полное обновление справочников core из raw (маленькие таблицы, выполняется в той же транзакции, что и загрузка окна).

-- Города: gc_id в источнике не уникален -> берём строку с минимальным cities.id.
-- Город и регион разбираются из full_name_local формата «Город, Регион».
DELETE FROM core.dim_city;
INSERT INTO core.dim_city (gc_id, city_id, city_name, region_name, full_name_local, latitude, longitude, source_rows)
SELECT DISTINCT ON (c.gc_id)
       c.gc_id,
       c.id,
       coalesce(nullif(btrim(split_part(c.full_name_local, ',', 1)), ''), c.name),
       CASE WHEN strpos(c.full_name_local, ',') > 0
            THEN nullif(btrim(substr(c.full_name_local, strpos(c.full_name_local, ',') + 1)), '')
       END,
       c.full_name_local,
       c.latitude,
       c.longitude,
       count(*) OVER (PARTITION BY c.gc_id)
FROM raw.cities c
ORDER BY c.gc_id, c.id;

-- Курсы ЦБ: при дублях на одну дату берём последнюю загруженную строку.
DELETE FROM core.fx_rate;
INSERT INTO core.fx_rate (currency_code, rate_date, rate)
SELECT DISTINCT ON (num_code, setting_date) num_code, setting_date, value
FROM raw.exchange_rates
WHERE value > 0
ORDER BY num_code, setting_date, id DESC;
