"""Gop ket qua 4 mo hinh (LightGBM, TimeXer, S4D tu results/timexer + S-Mamba tu results/smamba) va so sanh tren cung cua so test.
Chay:  python compare4.py"""
import glob, json, os
import numpy as np
from scipy import stats

TASKS = [("Phu tai 1 ngay (MW)", "load_H1"), ("Phu tai 7 ngay (MW)", "load_H7"), ("Thuy dien 1 ngay (MWh)", "hydro_H1"), ("Thuy dien 7 ngay (MWh)", "hydro_H7")]
NAMES = [("LightGBM_to_hop", "LightGBM"), ("TimeXer_to_hop", "TimeXer"), ("S4D_to_hop", "S4D"), ("S_Mamba_to_hop", "S-Mamba")]


def dm(p1, p2, y, H):
    d = (np.abs(p1 - y) - np.abs(p2 - y)).mean(axis=1); n = len(d); db = d.mean()
    g = lambda k: np.mean((d[k:] - db) * (d[:n - k] - db))
    var = max(g(0) + 2 * sum(g(k) for k in range(1, H)), 1e-12)
    st = db / np.sqrt(var / n) * np.sqrt((n + 1 - 2 * H + H * (H - 1) / n) / n)
    return st, 2 * (1 - stats.t.cdf(abs(st), df=n - 1))


print(f"{'Bai toan':<24}" + "".join(f"{n:>11}" for _, n in NAMES) + "   | S4D vs S-Mamba (DM p) | S-Mamba mean5 (+-)")
for label, t in TASKS:
    f_sm = glob.glob(f"results/smamba/{t}/stage3_preds_*.npz")
    if not f_sm:
        print(f"{label:<24}  (S-Mamba chua chay xong)"); continue
    zs = np.load(f_sm[0]); zo = np.load(glob.glob(f"results/timexer/{t}/stage3_preds_*.npz")[0])
    assert (zs["dates"] == zo["dates"]).all(), "cua so khong khop"
    y = zo["true"]; H = y.shape[1]
    P = {n: (zo[k] if k in zo.files else zs[k]) for k, n in NAMES if (k in zo.files or k in zs.files)}
    P["S-Mamba"] = zs["S_Mamba_to_hop"]
    mae = {n: np.abs(P[n] - y).mean() for n in P}
    best = min(mae, key=mae.get)
    cells = "".join(f"{(str(round(mae[n])) + ('*' if n == best else '')):>11}" for _, n in NAMES)
    st, p = dm(P["S4D"], P["S-Mamba"], y, H)
    j = json.load(open(glob.glob(f"results/smamba/{t}/stage3_*_fut.json")[0]))["results"]
    r = j.get("S-Mamba (TB 5 seed)", {})
    print(f"{label:<24}{cells}   | {st:>6.2f}  p={p:.3f}          | {r.get('MAE', float('nan')):.0f} (+-{r.get('MAE_std', float('nan')):.0f})")
