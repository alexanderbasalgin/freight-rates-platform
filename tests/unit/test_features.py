from datetime import date

import numpy as np
import pandas as pd

from ml.features import (CAT_FEATURES, FEATURES, FORBIDDEN_FEATURES, NUM_FEATURES, SplitBounds,
                         add_calendar_features, assign_period)


def test_split_bounds_match_task():
    b = SplitBounds.from_as_of(date(2026, 10, 26))
    assert b.train_start == date(2026, 7, 26)
    assert b.test_start == date(2026, 10, 5)


def test_period_boundaries_are_strict():
    b = SplitBounds.from_as_of(date(2026, 10, 26))
    s = pd.Series([date(2026, 7, 25), date(2026, 7, 26), date(2026, 10, 4), date(2026, 10, 5),
                   date(2026, 10, 26), date(2026, 10, 27)])
    assert assign_period(s, b).tolist() == [pd.NA, "train", "train", "test", "test", "future"]


def test_no_target_leakage_in_features():
    assert not FORBIDDEN_FEATURES.intersection(FEATURES)
    assert "outlier_mark" not in FEATURES
    assert len(FEATURES) == len(set(FEATURES)) == len(CAT_FEATURES) + len(NUM_FEATURES)


def test_cyclical_encoding_keeps_year_boundary_close():
    df = add_calendar_features(pd.DataFrame({"loading_date": [date(2025, 12, 31), date(2026, 1, 1), date(2026, 7, 1)]}))
    dec31, jan1, jul1 = (np.array([r.doy_sin, r.doy_cos]) for r in df.itertuples())
    assert np.linalg.norm(dec31 - jan1) < 0.05
    assert np.linalg.norm(dec31 - jul1) > 1.9
    assert np.allclose(df["month_sin"] ** 2 + df["month_cos"] ** 2, 1.0)
    assert df["day_of_week"].tolist() == [3, 4, 3]  # ISO: 1 = понедельник


def test_train_iqr_filter_drops_extremes_and_keeps_unknown_groups():
    from ml.train_score import apply_iqr_filter, fit_train_iqr_filter

    rng = np.random.default_rng(0)
    n = 200
    train = pd.DataFrame({
        "body_group": "Тент", "weight_category": "FTL",
        "distance": 1000.0, "price_rub": 100_000 * np.exp(rng.normal(0, 0.1, n)),
    })
    train.loc[0, "price_rub"] = 400_000  # x4
    train.loc[1, "price_rub"] = 25_000   # x0.25
    bounds = fit_train_iqr_filter(train)
    keep = apply_iqr_filter(train, bounds)
    assert not keep[0] and not keep[1]
    assert keep.mean() > 0.95
    other = pd.DataFrame({"body_group": ["Борт"], "weight_category": ["FTL"], "distance": [10.0], "price_rub": [1e9]})
    assert apply_iqr_filter(other, bounds).tolist() == [True]
