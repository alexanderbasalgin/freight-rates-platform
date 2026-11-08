"""Обучение модели тарифа и пакетный скоринг: python -m ml.train_score

1. Выборка: фактические прямые сделки из core (is_fact, NOT complex, price > 0, distance > 0).
2. Хронологическое разбиение от as_of (не от системной даты):
   train = [as_of - 3 мес.; as_of - 3 нед.), test = [as_of - 3 нед.; as_of].
3. Таргет очищается по outlier_mark (флаг не используется как признак), затем дополнительно по IQR
   ln(ставки за км) в группах кузов × вес: границы считаются только на train и только к train применяются.
4. CatBoostRegressor: число деревьев подбирается ранней остановкой на последних 2 неделях train,
   затем модель переобучается на всём train. Бейзлайн: Ridge (OneHot + Imputer + Scaler, fit только на train).
5. Метрики на test: R2, MAPE, RMSE, MAE. Скоринг test и будущих заявок (loading_date > as_of)
   с UPSERT в mart.ml_price_predictions. Для будущих заявок факт и ошибки = NULL.
"""

from __future__ import annotations

import json
import logging
import time
from datetime import timedelta

import numpy as np
import pandas as pd
from catboost import CatBoostRegressor, Pool
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.linear_model import Ridge
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler

from etl import config, db
from etl.logging_setup import setup_logging
from ml.features import (CAT_FEATURES, DATASET_SQL, FEATURES, NUM_FEATURES, TARGET, SplitBounds,
                         assign_period, build_features)
from ml.metrics import regression_metrics

log = logging.getLogger("ml.train_score")

MAIN_MODEL = "catboost_v1"
BASELINE_MODEL = "ridge_baseline_v1"
EARLY_STOPPING_DAYS = 14


def load_dataset(conn, bounds: SplitBounds) -> pd.DataFrame:
    with conn.cursor() as cur:
        cur.execute(DATASET_SQL, {"from_date": bounds.train_start})
        columns = [c.name for c in cur.description]
        df = pd.DataFrame(cur.fetchall(), columns=columns)
    df["period"] = assign_period(df["loading_date"], bounds)
    return build_features(df[df["period"].notna()].reset_index(drop=True))


def fit_train_iqr_filter(train: pd.DataFrame, k: float = 1.5) -> pd.DataFrame:
    """Границы IQR для ln(ставки за км) в группах кузов × весовая категория, считаются только по train.

    В источнике часть экстремальных ставок (~0.4%) не помечена outlier_mark. Ставка за км здесь
    используется только для отбора строк обучения и в признаки модели не попадает.
    """
    log_km = np.log(train[TARGET] / train["distance"])
    groups = train.assign(log_km=log_km).groupby(["body_group", "weight_category"])["log_km"]
    q1, q3 = groups.quantile(0.25), groups.quantile(0.75)
    iqr = q3 - q1
    return pd.DataFrame({"low": q1 - k * iqr, "high": q3 + k * iqr})


def apply_iqr_filter(frame: pd.DataFrame, bounds: pd.DataFrame) -> pd.Series:
    log_km = np.log(frame[TARGET] / frame["distance"])
    b = frame[["body_group", "weight_category"]].join(bounds, on=["body_group", "weight_category"])
    keep = (log_km >= b["low"]) & (log_km <= b["high"])
    return keep | b["low"].isna()


def catboost_params(seed: int) -> dict:
    return {
        "loss_function": "RMSE",
        "eval_metric": "RMSE",
        "iterations": 3000,
        "learning_rate": 0.05,
        "depth": 8,
        "l2_leaf_reg": 3.0,
        "random_seed": seed,
        "thread_count": -1,
        "allow_writing_files": False,
        "verbose": 0,
    }


def train_catboost(train: pd.DataFrame, bounds: SplitBounds, seed: int) -> tuple[CatBoostRegressor, dict]:
    es_start = bounds.test_start - timedelta(days=EARLY_STOPPING_DAYS)
    fit_part = train[train["loading_date"] < es_start]
    es_part = train[train["loading_date"] >= es_start]
    params = catboost_params(seed)

    probe = CatBoostRegressor(**params, od_type="Iter", od_wait=200, use_best_model=True)
    probe.fit(Pool(fit_part[FEATURES], fit_part[TARGET], cat_features=CAT_FEATURES),
              eval_set=Pool(es_part[FEATURES], es_part[TARGET], cat_features=CAT_FEATURES))
    best_iterations = int(probe.get_best_iteration()) + 1
    log.info("step=ml model=%s early_stopping_window=%s..%s best_iterations=%d",
             MAIN_MODEL, es_start, bounds.test_start - timedelta(days=1), best_iterations)

    params = {**params, "iterations": best_iterations}
    model = CatBoostRegressor(**params)
    model.fit(Pool(train[FEATURES], train[TARGET], cat_features=CAT_FEATURES))
    return model, params


def train_ridge(train: pd.DataFrame) -> Pipeline:
    pre = ColumnTransformer([
        ("num", Pipeline([("impute", SimpleImputer(strategy="median")), ("scale", StandardScaler())]), NUM_FEATURES),
        ("cat", Pipeline([("impute", SimpleImputer(strategy="most_frequent")),
                          ("onehot", OneHotEncoder(handle_unknown="ignore"))]), CAT_FEATURES),
    ])
    model = Pipeline([("pre", pre), ("ridge", Ridge(alpha=1.0))])
    model.fit(train[FEATURES], train[TARGET])
    return model


def save_run(conn, model_name: str, bounds: SplitBounds, n_train: int, n_test: int,
             metrics: dict, params: dict | None) -> None:
    conn.execute(
        """
        INSERT INTO mart.ml_model_runs (model_name, as_of, train_start, train_end, test_start, test_end,
                                        n_train, n_test, metrics, params, features)
        VALUES (%(model)s, %(as_of)s, %(train_start)s, %(train_end)s, %(test_start)s, %(as_of)s,
                %(n_train)s, %(n_test)s, %(metrics)s, %(params)s, %(features)s)
        ON CONFLICT (model_name, as_of) DO UPDATE SET
            train_start = EXCLUDED.train_start, train_end = EXCLUDED.train_end,
            test_start = EXCLUDED.test_start, test_end = EXCLUDED.test_end,
            n_train = EXCLUDED.n_train, n_test = EXCLUDED.n_test, metrics = EXCLUDED.metrics,
            params = EXCLUDED.params, features = EXCLUDED.features, trained_at = now()
        """,
        {"model": model_name, "as_of": bounds.as_of, "train_start": bounds.train_start,
         "train_end": bounds.test_start - timedelta(days=1), "test_start": bounds.test_start,
         "n_train": n_train, "n_test": n_test, "metrics": json.dumps(metrics),
         "params": json.dumps(params, default=str) if params else None,
         "features": json.dumps({"categorical": CAT_FEATURES, "numeric": NUM_FEATURES})},
    )


def save_predictions(conn, scored: pd.DataFrame, model_name: str, bounds: SplitBounds) -> int:
    columns = ["price_id", "model_name", "scoring_period", "loading_date", "loading_city_gcid",
               "unloading_city_gcid", "body_group", "distance", "outlier_mark", "actual_price_rub",
               "predicted_price_rub", "abs_error_rub", "ape_pct", "as_of"]
    rows = scored.assign(model_name=model_name, as_of=bounds.as_of)[columns]
    rows = rows.astype(object).where(rows.notna(), None)
    with conn.cursor() as cur:
        cur.execute("CREATE TEMP TABLE _pred (LIKE mart.ml_price_predictions INCLUDING DEFAULTS) ON COMMIT DROP")
        with cur.copy(f"COPY _pred ({', '.join(columns)}) FROM STDIN") as copy:
            for row in rows.itertuples(index=False, name=None):
                copy.write_row(row)
        # строки модели за окно скоринга, которых больше нет в батче, удаляем
        cur.execute(
            """
            DELETE FROM mart.ml_price_predictions p
            WHERE p.model_name = %(model)s AND p.loading_date >= %(test_start)s
              AND NOT EXISTS (SELECT 1 FROM _pred n WHERE n.price_id = p.price_id)
            """,
            {"model": model_name, "test_start": bounds.test_start},
        )
        cur.execute(
            f"""
            INSERT INTO mart.ml_price_predictions ({', '.join(columns)})
            SELECT {', '.join(columns)} FROM _pred
            ON CONFLICT (price_id, model_name) DO UPDATE SET
            {', '.join(f'{c} = EXCLUDED.{c}' for c in columns if c not in ('price_id', 'model_name'))},
            scored_at = now()
            """
        )
        return cur.rowcount


def score_frame(frame: pd.DataFrame, predicted: np.ndarray) -> pd.DataFrame:
    out = frame[["price_id", "period", "loading_date", "loading_city_gcid", "unloading_city_gcid",
                 "body_group", "distance", "outlier_mark", TARGET]].copy()
    out = out.rename(columns={"period": "scoring_period", TARGET: "actual_price_rub"})
    out["predicted_price_rub"] = np.round(predicted, 2)
    # для будущих заявок факта ещё нет, даже если price в источнике заполнен
    out.loc[out["scoring_period"] == "future", "actual_price_rub"] = np.nan
    out["abs_error_rub"] = (out["actual_price_rub"] - out["predicted_price_rub"]).abs().round(2)
    out["ape_pct"] = (out["abs_error_rub"] / out["actual_price_rub"] * 100).round(2)
    return out


def main() -> dict:
    t0 = time.monotonic()
    bounds = SplitBounds.from_as_of(config.as_of())
    seed = config.random_seed()
    np.random.seed(seed)

    with db.connect(config.dwh_db()) as conn:
        df = load_dataset(conn, bounds)
        train_marked = df[(df["period"] == "train") & ~df["outlier_mark"]]
        iqr_bounds = fit_train_iqr_filter(train_marked)
        train = train_marked[apply_iqr_filter(train_marked, iqr_bounds)]
        log.info("step=ml status=train_cleaning rows_after_outlier_mark=%d rows_removed_by_train_iqr=%d",
                 len(train_marked), len(train_marked) - len(train))
        test_all = df[df["period"] == "test"]
        test = test_all[~test_all["outlier_mark"]]
        future = df[df["period"] == "future"]
        log.info("step=ml status=dataset as_of=%s train=%s..%s test=%s..%s rows_train=%d rows_test=%d "
                 "rows_test_outliers_excluded=%d rows_future=%d",
                 bounds.as_of, bounds.train_start, bounds.test_start - timedelta(days=1),
                 bounds.test_start, bounds.as_of, len(train), len(test), len(test_all) - len(test), len(future))
        assert train["loading_date"].max() < bounds.test_start, "temporal leakage: train пересекает test"

        model, params = train_catboost(train, bounds, seed)
        ridge = train_ridge(train)

        last_day_unmarked = test["loading_date"] == bounds.as_of
        results = {}
        for name, predictor in ((MAIN_MODEL, model.predict), (BASELINE_MODEL, ridge.predict)):
            metrics = regression_metrics(test[TARGET], predictor(test[FEATURES]))
            sub = test[~last_day_unmarked]
            metrics["excluding_unmarked_as_of_day"] = regression_metrics(sub[TARGET], predictor(sub[FEATURES]))
            results[name] = metrics
            log.info("step=ml model=%s test_metrics=%s", name, json.dumps(metrics))

        save_run(conn, MAIN_MODEL, bounds, len(train), len(test), results[MAIN_MODEL], params)
        save_run(conn, BASELINE_MODEL, bounds, len(train), len(test), results[BASELINE_MODEL], {"alpha": 1.0})

        to_score = pd.concat([test_all, future])
        scored = score_frame(to_score, model.predict(to_score[FEATURES]))
        written = save_predictions(conn, scored, MAIN_MODEL, bounds)
        log.info("step=ml status=scored model=%s table=mart.ml_price_predictions rows_upserted=%d "
                 "test=%d future=%d", MAIN_MODEL, written, len(test_all), len(future))

        importances = dict(sorted(zip(FEATURES, model.get_feature_importance().round(2).tolist()),
                                  key=lambda kv: -kv[1]))
        log.info("step=ml model=%s feature_importance=%s", MAIN_MODEL, json.dumps(importances, ensure_ascii=False))

    out_dir = config.model_dir()
    model.save_model(str(out_dir / f"{MAIN_MODEL}_{bounds.as_of}.cbm"))
    summary = {"as_of": str(bounds.as_of), "metrics": results, "feature_importance": importances,
               "duration_sec": round(time.monotonic() - t0, 1)}
    (out_dir / f"metrics_{bounds.as_of}.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2))
    log.info("step=ml status=success duration_sec=%.1f artifacts=%s", summary["duration_sec"], out_dir)
    return summary


if __name__ == "__main__":
    setup_logging()
    main()
