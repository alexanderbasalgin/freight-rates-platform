"""Feature pipeline для прогноза ставки перевозчика (target = price_rub, руб. без НДС).

Признаки строятся только из атрибутов заявки, известных до закрытия сделки:
география (регионы и города отправления/назначения, координаты), параметры груза и кузова,
календарь даты погрузки с циклическим кодированием. Производные целевой переменной запрещены.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta

import numpy as np
import pandas as pd

TARGET = "price_rub"

CAT_FEATURES = [
    "origin_region", "destination_region", "origin_city", "destination_city",
    "body_group", "weight_category", "volume_category", "price_type",
]
NUM_FEATURES = [
    "distance", "total_weight", "total_volume",
    "origin_lat", "origin_lon", "destination_lat", "destination_lon",
    "year", "month", "day_of_week", "day_of_year",
    "month_sin", "month_cos", "dow_sin", "dow_cos", "doy_sin", "doy_cos",
]
FEATURES = CAT_FEATURES + NUM_FEATURES

# Прямые и косвенные производные таргета и постфактум-агрегаты: в признаках их быть не должно.
FORBIDDEN_FEATURES = frozenset({
    "price", "price_source", "price_rub", "price_with_vat", "fx_rate",
    "price_km", "price_per_km", "price_per_ton",
    "customer_cost", "transportation_cost", "carrier_cost", "profit",
    "avg_price", "min_km_price", "max_km_price", "price_group_id",
    "outlier_mark",
})

DATASET_SQL = """
SELECT f.price_id, f.loading_date, f.loading_city_gcid, f.unloading_city_gcid,
       f.distance, f.total_weight, f.total_volume, f.price_type::text AS price_type,
       f.weight_category, f.volume_category, g.body_group,
       lc.region_name AS origin_region, lc.city_name AS origin_city,
       lc.latitude AS origin_lat, lc.longitude AS origin_lon,
       uc.region_name AS destination_region, uc.city_name AS destination_city,
       uc.latitude AS destination_lat, uc.longitude AS destination_lon,
       f.outlier_mark, f.price_rub::float8 AS price_rub
FROM core.fact_price f
JOIN core.dim_body_group g ON g.body_group_code = f.primary_body_group_code
JOIN core.dim_city lc ON lc.gc_id = f.loading_city_gcid
JOIN core.dim_city uc ON uc.gc_id = f.unloading_city_gcid
WHERE f.is_fact AND NOT f.complex AND f.price_source > 0 AND f.distance > 0
  AND f.loading_date >= %(from_date)s
ORDER BY f.loading_date, f.price_id
"""


@dataclass(frozen=True)
class SplitBounds:
    """train = [as_of - 3 мес.; as_of - 3 нед.), test = [as_of - 3 нед.; as_of], future = (as_of; ...)."""
    as_of: date
    train_start: date
    test_start: date

    @classmethod
    def from_as_of(cls, as_of: date) -> "SplitBounds":
        train_start = (pd.Timestamp(as_of) - pd.DateOffset(months=3)).date()
        return cls(as_of=as_of, train_start=train_start, test_start=as_of - timedelta(weeks=3))


def assign_period(loading_date: pd.Series, bounds: SplitBounds) -> pd.Series:
    d = pd.to_datetime(loading_date).dt.date
    period = pd.Series(pd.NA, index=loading_date.index, dtype="object")
    period[(d >= bounds.train_start) & (d < bounds.test_start)] = "train"
    period[(d >= bounds.test_start) & (d <= bounds.as_of)] = "test"
    period[d > bounds.as_of] = "future"
    return period


def add_calendar_features(df: pd.DataFrame) -> pd.DataFrame:
    """Год (тренд) + месяц, день недели и день года в виде sin/cos на единичной окружности."""
    out = df.copy()
    d = pd.to_datetime(out["loading_date"])
    out["year"] = d.dt.year
    out["month"] = d.dt.month
    out["day_of_week"] = d.dt.isocalendar().day.astype(int)  # 1 = понедельник
    out["day_of_year"] = d.dt.dayofyear
    for name, t, period in (("month", out["month"], 12.0),
                            ("dow", out["day_of_week"], 7.0),
                            ("doy", out["day_of_year"], 365.25)):
        angle = 2.0 * np.pi * t / period
        out[f"{name}_sin"] = np.sin(angle)
        out[f"{name}_cos"] = np.cos(angle)
    return out


def build_features(df: pd.DataFrame) -> pd.DataFrame:
    out = add_calendar_features(df)
    for col in CAT_FEATURES:
        out[col] = out[col].fillna("__NA__").astype(str)
    leaked = FORBIDDEN_FEATURES.intersection(FEATURES)
    if leaked:
        raise AssertionError(f"Утечка таргета в признаках: {sorted(leaked)}")
    return out
