"""Метрики регрессии для оценки на отложенном тесте."""

from __future__ import annotations

import numpy as np
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score


def regression_metrics(y_true, y_pred) -> dict[str, float]:
    y_true = np.asarray(y_true, dtype=float)
    y_pred = np.asarray(y_pred, dtype=float)
    nonzero = y_true != 0
    return {
        "r2": round(float(r2_score(y_true, y_pred)), 4),
        "mape_pct": round(float(np.mean(np.abs((y_true[nonzero] - y_pred[nonzero]) / y_true[nonzero])) * 100), 2),
        "rmse_rub": round(float(np.sqrt(mean_squared_error(y_true, y_pred))), 2),
        "mae_rub": round(float(mean_absolute_error(y_true, y_pred)), 2),
        "n": int(len(y_true)),
    }
