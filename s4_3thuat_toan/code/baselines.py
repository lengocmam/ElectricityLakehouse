"""Mo hinh XGBoost va cac ham phu tro cho giao thuc danh gia cua so truot."""
import math
import numpy as np
import xgboost as xgb


def persistence(X, horizon):
    """Lap lai gia tri cuoi cung (hom nay) cho moi buoc du bao."""
    return np.repeat(X[:, -1, 0:1], horizon, axis=1)


def xgboost_fit_predict(X, Y, split, horizon, lags=14, seed=0, over=None, Fut=None):
    """XGBoost truc tiep tren `lags` thoi diem gan nhat cua moi dac trung.
    Du doan PHAN DU so voi gia tri cuoi (tranh lech ngoai suy phu tai).
    Early stopping theo MAE validation."""
    lags = min(lags, X.shape[1])
    Fm = X[:, -lags:, :].reshape(len(X), -1)
    if Fut is not None:
        Fm = np.concatenate([Fm, Fut.reshape(len(X), -1)], axis=1)
    base = X[:, -1, 0]
    tr, va = split == 0, split == 1
    
    params = dict(
        n_estimators=1000,
        learning_rate=0.03,
        max_depth=6,
        subsample=0.8,
        colsample_bytree=0.8,
        random_state=seed,
        n_jobs=-1,
        early_stopping_rounds=50,
        eval_metric="mae"
    )
    if over:
        params.update(over)
        
    pred = np.zeros_like(Y)
    for h in range(horizon):
        y = Y[:, h] - base
        reg = xgb.XGBRegressor(**params)
        reg.fit(Fm[tr], y[tr], eval_set=[(Fm[va], y[va])], verbose=False)
        pred[:, h] = reg.predict(Fm) + base
    return pred
