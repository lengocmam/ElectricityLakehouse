"""Mo hinh GHEP LightGBM + S4D cho phu tai 7 ngay, danh gia KHONG ro ri.

Quy tac ghep chi duoc hoc tren cac fold KHAC fold dang do (khong hoc tren tap dang test):
  - Trung binh 50/50            : khong hoc gi
  - Blend theo buoc h           : trong so cua S4D cho tung buoc du bao h=1..7, chon tren cac fold hoc (luoi 0..1, buoc 0,1)
  - Dinh tuyen theo (h, loai ngay): moi o (buoc h, ngay thuong/ngay le) chon mo hinh co MAE nho hon tren cac fold hoc
Hai cach danh gia:
  (A) theo thoi gian: hoc tren fold 1+2 (2025) -> do tren fold 3 (2026)
  (B) leave-one-fold-out: hoc tren 2 fold con lai -> do tren fold bi bo (khong theo thu tu thoi gian, dung de kiem tra do vung)
Chay:  python hybrid.py results/wf_fut/F1 results/wf_fut/F2 results/wf_fut/F3 [bang_ngay=unified_daily_v2.csv]
"""
import glob
import sys
import numpy as np
import pandas as pd
from scipy import stats

dirs = [a for a in sys.argv[1:] if not a.endswith(".csv")] or ["results/wf_fut/F1", "results/wf_fut/F2", "results/wf_fut/F3"]
daily = pd.read_csv(([a for a in sys.argv[1:] if a.endswith(".csv")] or ["unified_daily_v2.csv"])[0], parse_dates=["date"]).set_index("date")


def dm(p1, p2, y, H=7):
    d = (np.abs(p1 - y) - np.abs(p2 - y)).mean(axis=1)
    n, db = len(d), d.mean()
    g = lambda k: np.mean((d[k:] - db) * (d[:n - k] - db))
    var = max(g(0) + 2 * sum(g(k) for k in range(1, H)), 1e-12)
    st = db / np.sqrt(var / n) * np.sqrt((n + 1 - 2 * H + H * (H - 1) / n) / n)
    return st, 2 * (1 - stats.t.cdf(abs(st), df=n - 1))


folds = []
for d in dirs:
    z = np.load(glob.glob(f"{d}/stage3_preds_*.npz")[0])
    dates = pd.to_datetime(z["dates"])
    tgt = np.stack([dates + pd.Timedelta(days=h) for h in range(7)], axis=1)          # (n, 7) ngay muc tieu
    hol = daily["is_holiday_break"].reindex(pd.to_datetime(tgt.ravel())).fillna(0).values.reshape(tgt.shape).astype(int)
    folds.append(dict(name=d.split("/")[-1], y=z["true"], L=z["LightGBM_to_hop"], S=z["S4D_to_hop"], hol=hol))
W = np.round(np.arange(0, 1.01, 0.1), 1)


def fit(train):
    """Hoc quy tac tren cac fold `train`. Tra ve (w_h: (7,), route: (7, 2) in {0=LightGBM, 1=S4D})."""
    y = np.concatenate([f["y"] for f in train]); L = np.concatenate([f["L"] for f in train])
    S = np.concatenate([f["S"] for f in train]); hol = np.concatenate([f["hol"] for f in train])
    w_h = np.array([W[np.argmin([np.abs(w * S[:, h] + (1 - w) * L[:, h] - y[:, h]).mean() for w in W])] for h in range(7)])
    route = np.zeros((7, 2), dtype=int)
    for h in range(7):
        for c in (0, 1):
            m = hol[:, h] == c
            if m.sum() >= 5:
                route[h, c] = int(np.abs(S[m, h] - y[m, h]).mean() < np.abs(L[m, h] - y[m, h]).mean())
            else:
                route[h, c] = int(np.abs(S[:, h] - y[:, h]).mean() < np.abs(L[:, h] - y[:, h]).mean())
    return w_h, route


def apply(f, w_h, route):
    out = {"LightGBM": f["L"], "S4D": f["S"], "Trung binh 50/50": 0.5 * f["L"] + 0.5 * f["S"],
           "Blend theo buoc h": w_h[None, :] * f["S"] + (1 - w_h[None, :]) * f["L"]}
    r = route[np.arange(7)[None, :], f["hol"]]                                         # (n, 7) 0/1
    out["Dinh tuyen theo (h, loai ngay)"] = np.where(r == 1, f["S"], f["L"])
    return out


def report(title, test_idx, train_idx):
    train = [folds[i] for i in train_idx]
    w_h, route = fit(train)
    print(f"\n### {title}")
    print("  hoc tren:", [folds[i]["name"] for i in train_idx], "| w(S4D) theo buoc h:", list(w_h),
          "| dinh tuyen (hang=h, cot=[thuong, le], 1=S4D):", route.tolist())
    res = {}
    for i in test_idx:
        res[i] = apply(folds[i], w_h, route)
    names = list(res[test_idx[0]])
    y = np.concatenate([folds[i]["y"] for i in test_idx])
    P = {k: np.concatenate([res[i][k] for i in test_idx]) for k in names}
    print(f"  {'Mo hinh':<34}" + "".join(f"{folds[i]['name']:>8}" for i in test_idx) + f"{'Gop':>9}{'Bias%':>8}")
    for k in names:
        per = [np.abs(res[i][k] - folds[i]["y"]).mean() for i in test_idx]
        print(f"  {k:<34}" + "".join(f"{v:>8.0f}" for v in per) + f"{np.abs(P[k] - y).mean():>9.0f}{(P[k] - y).mean() / y.mean() * 100:>8.2f}")
    for k in names[2:]:
        st, p = dm(P[k], P["S4D"], y)
        st2, p2 = dm(P[k], P["LightGBM"], y)
        print(f"    DM {k} vs S4D: {st:>5.2f} p={p:.3f} | vs LightGBM: {st2:>5.2f} p={p2:.3f}   (am = ghep tot hon)")
    return P, y, test_idx


report("(A) THEO THOI GIAN: hoc tren fold 1+2, do tren fold 3 (2026)", [2], [0, 1])
# (B) leave-one-fold-out, gop ket qua cac fold bi bo
allP, ally = {}, []
for k in range(3):
    P, y, _ = report(f"(B) Bo fold {folds[k]['name']}: hoc tren hai fold con lai", [k], [i for i in range(3) if i != k])
    for n_, v in P.items():
        allP.setdefault(n_, []).append(v)
    ally.append(y)
print("\n### (B) GOP 3 LAN BO (643 cua so, moi cua so duoc du bao boi quy tac hoc tu 2 fold con lai)")
y = np.concatenate(ally)
G = {k: np.concatenate(v) for k, v in allP.items()}
for k in G:
    print(f"  {k:<34}{np.abs(G[k] - y).mean():>8.0f}   Bias% {(G[k] - y).mean() / y.mean() * 100:>6.2f}")
for k in list(G)[2:]:
    for ref in ("S4D", "LightGBM"):
        st, p = dm(G[k], G[ref], y)
        print(f"    DM {k} vs {ref}: {st:>5.2f} p={p:.3f}")
