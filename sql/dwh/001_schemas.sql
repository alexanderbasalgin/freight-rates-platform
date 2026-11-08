-- DWH: слои raw -> core -> mart + служебная схема meta.
-- Все объекты создаются идемпотентно (IF NOT EXISTS / CREATE OR REPLACE),
-- поэтому миграцию можно запускать при каждом старте пайплайна.

CREATE SCHEMA IF NOT EXISTS meta;
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS core;
CREATE SCHEMA IF NOT EXISTS mart;

-- ---------------------------------------------------------------- meta
CREATE TABLE IF NOT EXISTS meta.etl_runs (
    run_id       bigserial PRIMARY KEY,
    pipeline     text        NOT NULL,
    window_start date,
    window_end   date,
    status       text        NOT NULL DEFAULT 'running',
    started_at   timestamptz NOT NULL DEFAULT now(),
    finished_at  timestamptz,
    stats        jsonb,
    error        text
);

-- ---------------------------------------------------------------- raw
-- Сырые копии источника без преобразований. Факты грузятся окном по loading_date,
-- справочники перезаливаются целиком (они маленькие).
CREATE TABLE IF NOT EXISTS raw.prices (
    id                  bigint PRIMARY KEY,
    created_at          timestamp,
    updated_at          timestamp,
    subscriber_id       integer,
    organization_id     integer,
    loading_date        date             NOT NULL,
    loading_city_gcid   integer          NOT NULL,
    unloading_city_gcid integer          NOT NULL,
    distance            double precision,
    total_weight        double precision,
    total_volume        double precision,
    body_type           integer,
    price               double precision,
    currency_code       varchar(3),
    complex             boolean,
    price_type          smallint,
    is_fact             boolean,
    nearest_rate_date   date,
    end_loading_date    date,
    source              varchar(255),
    outlier_mark        boolean,
    weight_category     varchar(4),
    volume_category     varchar(4),
    _run_id             bigint           NOT NULL,
    _extracted_at       timestamptz      NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS raw_prices_loading_date_idx ON raw.prices (loading_date);

CREATE TABLE IF NOT EXISTS raw.price_body_types (
    price_id     bigint  NOT NULL REFERENCES raw.prices (id) ON DELETE CASCADE,
    body_type_id integer NOT NULL,
    PRIMARY KEY (price_id, body_type_id)
);

CREATE TABLE IF NOT EXISTS raw.price_dates (
    price_id   bigint NOT NULL REFERENCES raw.prices (id) ON DELETE CASCADE,
    price_date date   NOT NULL,
    PRIMARY KEY (price_id, price_date)
);

CREATE TABLE IF NOT EXISTS raw.cities (
    id              bigint PRIMARY KEY,
    gc_id           integer NOT NULL,
    name            text,
    full_name_local text,
    latitude        double precision,
    longitude       double precision
);

CREATE TABLE IF NOT EXISTS raw.body_types (
    id          bigint PRIMARY KEY,
    name        text,
    id_from_1c  integer,
    std_id      integer
);

CREATE TABLE IF NOT EXISTS raw.exchange_rates (
    id           bigint PRIMARY KEY,
    num_code     varchar(3),
    value        double precision,
    setting_date date
);

-- ---------------------------------------------------------------- core: правила
-- Весовая категория по весу, округлённому до 0.1 т (как в исходном job).
CREATE OR REPLACE FUNCTION core.weight_category(w double precision)
RETURNS varchar(4) LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
    SELECT CASE
        WHEN w IS NULL THEN NULL
        WHEN round(w::numeric, 1) >= 30  THEN 'G20T'
        WHEN round(w::numeric, 1) >= 10  THEN 'FTL'
        WHEN round(w::numeric, 1) >= 5   THEN '10T'
        WHEN round(w::numeric, 1) >= 2.5 THEN '5T'
        WHEN round(w::numeric, 1) >= 0.5 THEN 'L5T'
        ELSE 'OTH'
    END
$$;

CREATE OR REPLACE FUNCTION core.volume_category(v double precision)
RETURNS varchar(4) LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
    SELECT CASE WHEN v IS NULL THEN NULL WHEN v > 0 AND v <= 120 THEN 'FTL' ELSE 'OTH' END
$$;

-- Код бизнес-группы кузова: 56 Тент, 4 Рефрижератор, 2 Изотерм, 15 Борт, 0 Прочие, -1 Не указан.
-- Маппинг задан явно по кодам 1С из требований, а не через body_types.std_id,
-- чтобы новые коды справочника не меняли группировку молча.
CREATE OR REPLACE FUNCTION core.body_group_code(body_type_id integer)
RETURNS integer LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
    SELECT CASE
        WHEN body_type_id IS NULL THEN -1
        WHEN body_type_id IN (56, 57, 58, 59, 60, 61) THEN 56
        WHEN body_type_id IN (4, 46, 47, 48) THEN 4
        WHEN body_type_id = 2 THEN 2
        WHEN body_type_id IN (1, 15) THEN 15
        ELSE 0
    END
$$;

-- ---------------------------------------------------------------- core: измерения
CREATE TABLE IF NOT EXISTS core.dim_body_group (
    body_group_code integer PRIMARY KEY,
    body_group      text    NOT NULL UNIQUE
);
INSERT INTO core.dim_body_group (body_group_code, body_group) VALUES
    (56, 'Тент'), (4, 'Рефрижератор'), (2, 'Изотерм'), (15, 'Борт'), (0, 'Прочие'), (-1, 'Не указан')
ON CONFLICT (body_group_code) DO UPDATE SET body_group = EXCLUDED.body_group;

-- Дедуплицированный справочник городов: одна строка на gc_id (минимальный cities.id).
CREATE TABLE IF NOT EXISTS core.dim_city (
    gc_id           integer PRIMARY KEY,
    city_id         bigint  NOT NULL,
    city_name       text    NOT NULL,
    region_name     text,
    full_name_local text,
    latitude        double precision,
    longitude       double precision,
    source_rows     integer NOT NULL
);

CREATE TABLE IF NOT EXISTS core.fx_rate (
    currency_code varchar(3)       NOT NULL,
    rate_date     date             NOT NULL,
    rate          double precision NOT NULL,
    PRIMARY KEY (currency_code, rate_date)
);

-- ---------------------------------------------------------------- core: факты
-- Одна строка на сделку, прошедшую DQ. Сюда попадают и нефактические записи (торги)
-- и выбросы: фильтры рынка применяются в витринах, флаги сохраняются.
CREATE TABLE IF NOT EXISTS core.fact_price (
    price_id                bigint PRIMARY KEY,
    created_at              timestamp,
    loading_date            date             NOT NULL,
    end_loading_date        date,
    loading_city_gcid       integer          NOT NULL,
    unloading_city_gcid     integer          NOT NULL,
    subscriber_id           integer,
    organization_id         integer,
    price_type              smallint         NOT NULL,
    is_fact                 boolean          NOT NULL,
    complex                 boolean          NOT NULL,
    outlier_mark            boolean          NOT NULL,
    distance                double precision NOT NULL,
    total_weight            double precision NOT NULL,
    total_volume            double precision NOT NULL,
    body_type               integer,
    primary_body_group_code integer          NOT NULL REFERENCES core.dim_body_group (body_group_code),
    weight_category         varchar(4)       NOT NULL,
    weight_category_filled  boolean          NOT NULL,
    volume_category         varchar(4)       NOT NULL,
    volume_category_filled  boolean          NOT NULL,
    currency_code           varchar(3)       NOT NULL,
    price_source            double precision NOT NULL,
    fx_rate                 double precision NOT NULL,
    fx_rate_date            date,
    price_rub               numeric(14, 2)   NOT NULL,
    source                  text,
    run_id                  bigint           NOT NULL,
    loaded_at               timestamptz      NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS fact_price_loading_date_idx ON core.fact_price (loading_date);

-- Группы кузова сделки: одна строка на (сделка, группа), дубли дочерних кодов схлопнуты.
CREATE TABLE IF NOT EXISTS core.fact_price_body_group (
    price_id        bigint  NOT NULL REFERENCES core.fact_price (price_id) ON DELETE CASCADE,
    body_group_code integer NOT NULL REFERENCES core.dim_body_group (body_group_code),
    PRIMARY KEY (price_id, body_group_code)
);

-- Помесячная развёртка сделки (зерно витрины), из price_dates.
CREATE TABLE IF NOT EXISTS core.fact_price_date (
    price_id   bigint NOT NULL REFERENCES core.fact_price (price_id) ON DELETE CASCADE,
    price_date date   NOT NULL,
    PRIMARY KEY (price_id, price_date)
);
CREATE INDEX IF NOT EXISTS fact_price_date_date_idx ON core.fact_price_date (price_date);

-- Карантин DQ: одна строка на (сделка, причина), исходная строка сохраняется в jsonb.
CREATE TABLE IF NOT EXISTS core.dq_quarantine_prices (
    price_id       bigint      NOT NULL,
    reason_code    text        NOT NULL,
    reason_details text,
    loading_date   date        NOT NULL,
    raw_row        jsonb       NOT NULL,
    run_id         bigint      NOT NULL,
    quarantined_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (price_id, reason_code)
);
CREATE INDEX IF NOT EXISTS dq_quarantine_loading_date_idx ON core.dq_quarantine_prices (loading_date);

-- ---------------------------------------------------------------- mart
CREATE TABLE IF NOT EXISTS mart.mart_route_card (
    month               date        NOT NULL,
    loading_city_gcid   integer     NOT NULL,
    unloading_city_gcid integer     NOT NULL,
    body_group_code     integer     NOT NULL,
    weight_category     varchar(4)  NOT NULL,
    loading_city        text,
    loading_region      text,
    unloading_city      text,
    unloading_region    text,
    body_group          text        NOT NULL,
    p25_trip            numeric(14, 2),
    median_trip         numeric(14, 2),
    p75_trip            numeric(14, 2),
    avg_trip            numeric(14, 2),
    p25_km              numeric(12, 2),
    median_km           numeric(12, 2),
    p75_km              numeric(12, 2),
    trips_count         integer     NOT NULL,
    carriers_count      integer     NOT NULL,
    cv_rate             numeric(8, 4),
    mom_pct             numeric(10, 2),
    yoy_pct             numeric(10, 2),
    first_deal_date     date        NOT NULL,
    last_deal_date      date        NOT NULL,
    freshness_days      integer     NOT NULL,
    reliability_level   text        NOT NULL,
    as_of               date        NOT NULL,
    refreshed_at        timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (month, loading_city_gcid, unloading_city_gcid, body_group_code, weight_category)
);
CREATE INDEX IF NOT EXISTS mart_route_card_route_idx
    ON mart.mart_route_card (loading_city_gcid, unloading_city_gcid, body_group_code, weight_category, month);

CREATE TABLE IF NOT EXISTS mart.ml_model_runs (
    model_name     text        NOT NULL,
    as_of          date        NOT NULL,
    train_start    date        NOT NULL,
    train_end      date        NOT NULL,
    test_start     date        NOT NULL,
    test_end       date        NOT NULL,
    n_train        integer     NOT NULL,
    n_test         integer     NOT NULL,
    metrics        jsonb       NOT NULL,
    params         jsonb,
    features       jsonb       NOT NULL,
    trained_at     timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (model_name, as_of)
);

CREATE TABLE IF NOT EXISTS mart.ml_price_predictions (
    price_id            bigint         NOT NULL,
    model_name          text           NOT NULL,
    scoring_period      text           NOT NULL CHECK (scoring_period IN ('test', 'future')),
    loading_date        date           NOT NULL,
    loading_city_gcid   integer        NOT NULL,
    unloading_city_gcid integer        NOT NULL,
    body_group          text           NOT NULL,
    distance            double precision,
    outlier_mark        boolean        NOT NULL,
    actual_price_rub    numeric(14, 2),
    predicted_price_rub numeric(14, 2) NOT NULL,
    abs_error_rub       numeric(14, 2),
    ape_pct             numeric(10, 2),
    as_of               date           NOT NULL,
    scored_at           timestamptz    NOT NULL DEFAULT now(),
    PRIMARY KEY (price_id, model_name),
    CHECK (scoring_period = 'test' OR (actual_price_rub IS NULL AND abs_error_rub IS NULL AND ape_pct IS NULL))
);
