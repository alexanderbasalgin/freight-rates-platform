"""Пакетный ETL источника сделок в Core DWH за окно дат погрузки.

    python -m etl.run --start-date 2026-09-01 --end-date 2026-09-30

Идемпотентность: выгрузка raw, перезапись окна в core и карантин выполняются в одной транзакции DWH
(DELETE окна + INSERT). Повторный запуск за то же окно даёт те же строки и суммы, частичных состояний нет.
Перенос данных между базами идёт потоком COPY ... TO STDOUT -> COPY ... FROM STDIN, без построчных INSERT.
"""

from __future__ import annotations

import argparse
import json
import logging
import time
from datetime import date

import psycopg

from etl import config, db
from etl.logging_setup import setup_logging

log = logging.getLogger("etl.run")

PRICE_COLUMNS = (
    "id, created_at, updated_at, subscriber_id, organization_id, loading_date, loading_city_gcid, "
    "unloading_city_gcid, distance, total_weight, total_volume, body_type, price, currency_code, complex, "
    "price_type, is_fact, nearest_rate_date, end_loading_date, source, outlier_mark, weight_category, "
    "volume_category"
)

REFERENCE_TABLES = {
    # таблица DWH: (колонки DWH, SELECT из источника)
    "raw.cities": (
        "id, gc_id, name, full_name_local, latitude, longitude",
        "SELECT id, gc_id, name, full_name_local, latitude, longitude FROM cities",
    ),
    "raw.body_types": (
        "id, name, id_from_1c, std_id",
        'SELECT id, name, "id_from_1C", std_id FROM body_types',
    ),
    "raw.exchange_rates": (
        "id, num_code, value, setting_date",
        "SELECT id, num_code, value, setting_date FROM exchange_rates",
    ),
}


def _stream_copy(src: psycopg.Connection, dst: psycopg.Connection, select_sql: str,
                 target: str, columns: str, params: dict | None = None) -> int:
    """Потоковая копия результата запроса источника в таблицу DWH. Возвращает число строк."""
    with src.cursor() as scur, dst.cursor() as dcur:
        copy_out = scur.mogrify(f"COPY ({select_sql}) TO STDOUT", params) if params else f"COPY ({select_sql}) TO STDOUT"
        with scur.copy(copy_out) as out_copy, dcur.copy(f"COPY {target} ({columns}) FROM STDIN") as in_copy:
            for chunk in out_copy:
                in_copy.write(chunk)
        dcur.execute(f"SELECT count(*) FROM {target}")
        return dcur.fetchone()[0]


def _start_run(dwh_cfg: config.DbConfig, start: date, end: date) -> int:
    with db.connect(dwh_cfg) as conn:
        row = conn.execute(
            "INSERT INTO meta.etl_runs (pipeline, window_start, window_end) VALUES ('etl', %s, %s) RETURNING run_id",
            (start, end),
        ).fetchone()
        return row[0]


def _finish_run(dwh_cfg: config.DbConfig, run_id: int, status: str, stats: dict | None, error: str | None) -> None:
    with db.connect(dwh_cfg) as conn:
        conn.execute(
            "UPDATE meta.etl_runs SET status = %s, finished_at = now(), stats = %s, error = %s WHERE run_id = %s",
            (status, json.dumps(stats) if stats else None, error, run_id),
        )


def run_window(start: date, end: date) -> dict:
    if start > end:
        raise ValueError("start_date > end_date")
    src_cfg, dwh_cfg = config.source_db(), config.dwh_db()
    run_id = _start_run(dwh_cfg, start, end)
    t0 = time.monotonic()
    log.info("step=etl status=started run_id=%d window=%s..%s", run_id, start, end)
    params = {"start": start, "end": end, "run_id": run_id}
    try:
        with db.connect(src_cfg) as src, db.connect(dwh_cfg) as dwh:
            # Источник читаем одним снимком, чтобы факты и дочерние таблицы были согласованы.
            src.isolation_level = psycopg.IsolationLevel.REPEATABLE_READ
            src.read_only = True
            with dwh.transaction():
                dwh.execute("SELECT pg_advisory_xact_lock(hashtext('etl_window'))")
                stats = _load(src, dwh, params)
            src.rollback()
        stats["duration_sec"] = round(time.monotonic() - t0, 1)
        _finish_run(dwh_cfg, run_id, "success", stats, None)
        log.info("step=etl status=success run_id=%d stats=%s", run_id, json.dumps(stats, ensure_ascii=False))
        return stats
    except Exception as exc:
        _finish_run(dwh_cfg, run_id, "failed", None, repr(exc))
        log.exception("step=etl status=failed run_id=%d", run_id)
        raise


def _load(src: psycopg.Connection, dwh: psycopg.Connection, params: dict) -> dict:
    stats: dict = {"run_id": params["run_id"], "window": f"{params['start']}..{params['end']}"}

    # 1. Справочники: полная перезаливка raw.
    for table, (columns, select_sql) in REFERENCE_TABLES.items():
        dwh.execute(f"DELETE FROM {table}")
        stats[f"rows_{table.split('.')[1]}"] = _stream_copy(src, dwh, select_sql, table, columns)

    # 2. Факты окна: через временную staging-таблицу, затем перезапись окна в raw.
    dwh.execute("CREATE TEMP TABLE _stg_prices (LIKE raw.prices INCLUDING DEFAULTS) ON COMMIT DROP")
    dwh.execute("CREATE TEMP TABLE _stg_pbt (LIKE raw.price_body_types) ON COMMIT DROP")
    dwh.execute("CREATE TEMP TABLE _stg_pd (LIKE raw.price_dates) ON COMMIT DROP")
    window = "loading_date BETWEEN %(start)s AND %(end)s"
    stats["rows_extracted"] = _stream_copy(
        src, dwh, f"SELECT {PRICE_COLUMNS}, {int(params['run_id'])} FROM prices WHERE {window}",
        "_stg_prices", PRICE_COLUMNS + ", _run_id", params)
    stats["rows_extracted_body_types"] = _stream_copy(
        src, dwh,
        f"SELECT b.price_id, b.body_type_id FROM price_body_types b JOIN prices p ON p.id = b.price_id WHERE p.{window}",
        "_stg_pbt", "price_id, body_type_id", params)
    stats["rows_extracted_price_dates"] = _stream_copy(
        src, dwh,
        f"SELECT d.price_id, d.price_date FROM price_dates d JOIN prices p ON p.id = d.price_id WHERE p.{window}",
        "_stg_pd", "price_id, price_date", params)

    with dwh.cursor() as cur:
        cur.execute(
            f"DELETE FROM raw.prices WHERE {window} OR id IN (SELECT id FROM _stg_prices)", params)
        stats["rows_raw_replaced"] = cur.rowcount
        cur.execute("INSERT INTO raw.prices SELECT * FROM _stg_prices")
        cur.execute("INSERT INTO raw.price_body_types SELECT * FROM _stg_pbt")
        cur.execute("INSERT INTO raw.price_dates SELECT * FROM _stg_pd")

    # 3. raw -> core: справочники, DQ + карантин, нормализация.
    db.run_sql_file(dwh, config.SQL_DIR / "etl" / "010_refresh_dimensions.sql")
    db.run_sql_file(dwh, config.SQL_DIR / "etl" / "020_load_core_window.sql", params)

    with dwh.cursor() as cur:
        cur.execute(f"SELECT count(*) FROM core.fact_price WHERE {window}", params)
        stats["rows_loaded_core"] = cur.fetchone()[0]
        cur.execute(
            f"SELECT reason_code, count(*) FROM core.dq_quarantine_prices WHERE {window} GROUP BY 1 ORDER BY 1",
            params)
        stats["quarantine_by_reason"] = dict(cur.fetchall())
        cur.execute(f"SELECT count(DISTINCT price_id) FROM core.dq_quarantine_prices WHERE {window}", params)
        stats["rows_quarantined"] = cur.fetchone()[0]
        cur.execute(
            f"SELECT count(*) FILTER (WHERE weight_category_filled), count(*) FILTER (WHERE volume_category_filled) "
            f"FROM core.fact_price WHERE {window}", params)
        stats["weight_category_filled"], stats["volume_category_filled"] = cur.fetchone()

    # Сверка: каждая выгруженная строка либо в core, либо в карантине.
    if stats["rows_loaded_core"] + stats["rows_quarantined"] != stats["rows_extracted"]:
        raise RuntimeError(f"Не сходится баланс строк: {stats}")
    return stats


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--start-date", type=date.fromisoformat, required=True)
    parser.add_argument("--end-date", type=date.fromisoformat, required=True)
    args = parser.parse_args()
    setup_logging()
    run_window(args.start_date, args.end_date)


if __name__ == "__main__":
    main()
