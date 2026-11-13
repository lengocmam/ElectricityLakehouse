"""Tong hop (ensemble) cac mo hinh tu bai bao: trung binh du bao cua S4D, TimeXer, S-Mamba (khong hoc them tham so nao), so voi tung mo hinh don le va LightGBM.
Chay:  python ensemble_paper.py"""
import glob, os
import numpy as np
from scipy import stats

TASKS = [("Phu tai 1 ngay (MW)", "load_H1"), ("Phu tai 7 ngay (MW)", "load_H7"), ("Thuy dien 1 ngay (MWh)", "hydro_H1"), ("Thuy dien 7 ngay (MWh)", "hydro_H7")]


def dm(p1, p2, y, H):
    d = (np.abs(p1 - y) - np.abs(p2 - y)).mean(axis=1); n = len(d); db = d.mean()
    g = lambda k: np.mean((d[k:] - db) * (d[:n - k] - db))
    var = max(g(0) + 2 * sum(g(k) for k in range(1, H)), 1e-12)
    st = db / np.sqrt(var / n) * np.sqrt((n + 1 - 2 * H + H * (H - 1) / n) / n)
    return st, 2 * (1 - stats.t.cdf(abs(st), df=n - 1))


rows = []
print(f"{'Bai toan':<24}{'LightGBM':>10}{'S4D':>9}{'TimeXer':>9}{'S-Mamba':>9}{'TB 3 bai bao':>14}{'TB 2 tot nhat':>15}   DM TB3 vs LightGBM | vs model don tot nhat")
for label, t in TASKS:
    zo = np.load(glob.glob(f"results/timexer/{t}/stage3_preds_*.npz")[0]); zs = np.load(glob.glob(f"results/smamba/{t}/stage3_preds_*.npz")[0])
    y = zo["true"]; H = y.shape[1]
    P = {"LightGBM": zo["LightGBM_to_hop"], "S4D": zo["S4D_to_hop"], "TimeXer": zo["TimeXer_to_hop"], "S-Mamba": zs["S_Mamba_to_hop"]}
    mae = {k: np.abs(v - y).mean() for k, v in P.items()}
    avg3 = (P["S4D"] + P["TimeXer"] + P["S-Mamba"]) / 3
    top2 = sorted(["S4D", "TimeXer", "S-Mamba"], key=lambda k: mae[k])[:2]
    avg2 = (P[top2[0]] + P[top2[1]]) / 2
    m3, m2 = np.abs(avg3 - y).mean(), np.abs(avg2 - y).mean()
    best_single = min(["S4D", "TimeXer", "S-Mamba"], key=lambda k: mae[k])
    s1, p1 = dm(avg3, P["LightGBM"], y, H); s2, p2 = dm(avg3, P[best_single], y, H)
    print(f"{label:<24}{mae['LightGBM']:>10.0f}{mae['S4D']:>9.0f}{mae['TimeXer']:>9.0f}{mae['S-Mamba']:>9.0f}{m3:>14.0f}{m2:>11.0f} ({'+'.join(top2)})   {s1:>5.2f} p={p1:.3f} | {s2:>5.2f} p={p2:.3f} (vs {best_single})")
