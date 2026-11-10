"""Cac baseline don gian de doi chung. Tat ca chay tren du lieu da chuan hoa toan cuc (kenh 0 = target)."""
import lightgbm as lgb
import math
import numpy as np


def persistence(X, horizon):
    """Lap lai gia tri cuoi cung (hom nay) cho moi buoc du bao."""
    return np.repeat(X[:, -1, 0:1], horizon, axis=1)


def seasonal_naive(X, horizon, period=7):
    """Lay gia tri cung thu cua tuan truoc: y(t+h) = y(t+h-7k)."""
    L = X.shape[1]
    cols = []
    for h in range(1, horizon + 1):
        k = math.ceil(h / period)
        src = (L - 1) + h - period * k
        if src < 0:
            raise ValueError("lookback qua ngan cho seasonal naive")
        cols.append(X[:, src, 0])
    return np.stack(cols, axis=1)


def ridge_fit_predict(X, Y, split, horizon, lags=14, alphas=(0.1, 1, 10, 100, 1000, 10000), Fut=None):
    """Hoi quy ridge truc tiep tren `lags` ngay gan nhat cua moi dac trung -> H dau ra.
    Chon alpha bang MAE tren tap validation. Tra ve (du bao cho moi cua so, alpha tot nhat)."""
    lags = min(lags, X.shape[1])
    Fx = X[:, -lags:, :].reshape(len(X), -1)
    F = Fx if Fut is None else np.concatenate([Fx, Fut.reshape(len(X), -1)], axis=1)
    tr, va = split == 0, split == 1
    mu = F[tr].mean(0)
    ym = Y[tr].mean(0)
    A = F[tr] - mu
    best = None
    for a in alphas:
        W = np.linalg.solve(A.T @ A + a * np.eye(A.shape[1]), A.T @ (Y[tr] - ym))
        pred_va = (F[va] - mu) @ W + ym
        mae = np.abs(pred_va - Y[va]).mean()
        if best is None or mae < best[0]:
            best = (mae, a, W)
    _, a, W = best
    return (F - mu) @ W + ym, a


def lightgbm_fit_predict(X, Y, split, horizon, lags=14, seed=0, over=None, Fut=None):
    """LightGBM truc tiep tren `lags` ngay gan nhat cua moi dac trung (API goc, khong can scikit-learn).
    Du doan PHAN DU so voi gia tri cuoi (cay quyet dinh khong ngoai suy duoc xu huong tang cua phu tai).
    Early stopping theo MAE validation."""
    lags = min(lags, X.shape[1])
    Fm = X[:, -lags:, :].reshape(len(X), -1)
    if Fut is not None:
        Fm = np.concatenate([Fm, Fut.reshape(len(X), -1)], axis=1)
    base = X[:, -1, 0]
    tr, va = split == 0, split == 1
    params = dict(objective="regression", metric="l1", learning_rate=0.03, num_leaves=15, min_data_in_leaf=15,
                  feature_fraction=0.8, bagging_fraction=0.8, bagging_freq=1, lambda_l2=1.0, seed=seed,
                  num_threads=1, verbose=-1)
    params.update(over or {})
    pred = np.zeros_like(Y)
    for h in range(horizon):
        y = Y[:, h] - base
        dtr = lgb.Dataset(Fm[tr], y[tr], params={"num_threads": 1})
        dva = lgb.Dataset(Fm[va], y[va], reference=dtr, params={"num_threads": 1})
        bst = lgb.train(params, dtr, num_boost_round=1500, valid_sets=[dva],
                        callbacks=[lgb.early_stopping(60, verbose=False)])
        pred[:, h] = bst.predict(Fm, num_iteration=bst.best_iteration) + base
    return pred
