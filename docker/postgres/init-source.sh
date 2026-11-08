#!/usr/bin/env bash
# Инициализация операционной БД-источника deals_bigdata: эталонная схема + детерминированный генератор.
# Выполняется entrypoint-ом postgres только при первом создании тома.
set -euo pipefail

psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" -f /source-sql/schema.sql
psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" -v n_prices="${N_PRICES:-300000}" \
     -f /source-sql/synthetic.sql
