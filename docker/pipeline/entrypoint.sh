#!/usr/bin/env bash
# Сквозной прогон: миграции DWH -> ETL окна -> витрины -> ML (обучение + batch scoring) -> автотесты.
# Каждый шаг идемпотентен, поэтому контейнер можно перезапускать сколько угодно раз.
set -euo pipefail

: "${ETL_START_DATE:=2023-01-01}"
: "${ETL_END_DATE:=2027-12-31}"
: "${RUN_TESTS:=1}"

case "${1:-all}" in
  all)
    python -m etl.migrate
    python -m etl.run --start-date "$ETL_START_DATE" --end-date "$ETL_END_DATE"
    python -m etl.marts
    python -m ml.train_score
    if [ "$RUN_TESTS" = "1" ]; then
      pytest
    fi
    echo "pipeline finished: DWH is available, see README for example queries"
    ;;
  migrate) python -m etl.migrate ;;
  etl)     shift; python -m etl.run "$@" ;;
  marts)   python -m etl.marts ;;
  ml)      python -m ml.train_score ;;
  test)    shift; pytest "$@" ;;
  *)       exec "$@" ;;
esac
