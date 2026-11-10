"""Tong hop ket qua walk-forward (cac thu muc results/wf_F*).

Moi fold: tinh chinh rieng tren validation cua fold do, danh gia tren khoang test cua fold (khong chong lan nhau).
In: MAE theo fold, MAE trung binh, so fold ma moi mo hinh la tot nhat, va kiem dinh Diebold-Mariano GOP chung cac fold.

Chay:  python wf_summary.py results/wf_F1 results/wf_F2 results/wf_F3
"""
import glob
import json
import sys
import numpy as np
from scipy import stats

dirs = sys.argv[1:] or ["results/wf_F1", "results/wf_F2", "results/wf_F3"]
MODELS = ["LightGBM_to_hop", "DLinear_to_hop", "TimeXer_to_hop", "S4D_to_hop"]   # chi dung mo hinh co mat trong moi fold
NAMES = {"TimeXer_to_hop": "TimeXer (to hop)", "persistence": "Persistence", "seasonal_naive_7": "Seasonal naive 7", "Ridge_to_hop": "Ridge",
         "LightGBM_to_hop": "LightGBM (to hop)", "DLinear_to_hop": "DLinear (to hop)",
         "PatchTST_to_hop": "PatchTST (to hop)", "S4D_to_hop": "S4D (to hop)"}


def dm_test(p1, p2, y, H):
    d = (np.abs(p1 - y) - np.abs(p2 - y)).mean(axis=1)
    n, db = len(d), d.mean()
    g = lambda k: np.mean((d[k:] - db) * (d[:n - k] - db))
    var = max(g(0) + 2 * sum(g(k) for k in range(1, H)), 1e-12)
    st = db / np.sqrt(var / n) * np.sqrt((n + 1 - 2 * H + H * (H - 1) / n) / n)
    return float(st), float(2 * (1 - stats.t.cdf(abs(st), df=n - 1)))


folds, rows = [], {}
for d in dirs:
    f = glob.glob(f"{d}/stage3_preds_*.npz")[0]
    z = np.load(f)
    j = json.load(open(glob.glob(f"{d}/stage3_*.json")[0]))
    folds.append({"name": d.split("/")[-1], "z": {k: z[k] for k in z.files}, "args": j["args"], "n": len(z["dates"])})

MODELS = [m for m in MODELS if all(m in fd["z"] for fd in folds)]
H = folds[0]["z"]["true"].shape[1]
print(f"Walk-forward, horizon {H}, {len(folds)} fold\n")
for fd in folds:
    a = fd["args"]
    print(f"{fd['name']}: train den {a['train_end']}, validation den {a['val_end']}, test {a['test_end'] or 'het du lieu'}  ({fd['n']} cua so)")
print(f"\n{'mo hinh':<22}" + "".join(f"{fd['name']:>10}" for fd in folds) + f"{'TB':>10}{'#fold tot nhat':>16}")
mae = {m: [np.abs(fd["z"][m] - fd["z"]["true"]).mean() for fd in folds] for m in MODELS}
best_count = {m: 0 for m in MODELS}
for i in range(len(folds)):
    best_count[min(MODELS, key=lambda m: mae[m][i])] += 1
for m in MODELS:
    print(f"{NAMES[m]:<22}" + "".join(f"{v:>10.0f}" for v in mae[m]) + f"{np.mean(mae[m]):>10.0f}{best_count[m]:>16}")

pooled = {m: np.concatenate([fd["z"][m] for fd in folds]) for m in MODELS}
y = np.concatenate([fd["z"]["true"] for fd in folds])
print(f"\nDiebold-Mariano gop {len(y)} cua so (thong ke am: S4D tot hon):")
for m in MODELS:
    if m == "S4D_to_hop":
        continue
    st, p = dm_test(pooled["S4D_to_hop"], pooled[m], y, H)
    print(f"  S4D vs {NAMES[m]:<20} DM={st:>6.2f}  p={p:.3f}" + ("  *" if p < 0.05 else ""))
print("\nTheo tung fold, S4D vs mo hinh tot nhat con lai:")
for fd in folds:
    others = [m for m in MODELS if m != "S4D_to_hop"]
    bm = min(others, key=lambda m: np.abs(fd["z"][m] - fd["z"]["true"]).mean())
    st, p = dm_test(fd["z"]["S4D_to_hop"], fd["z"][bm], fd["z"]["true"], H)
    print(f"  {fd['name']}: doi thu tot nhat = {NAMES[bm]}, DM={st:.2f}, p={p:.3f}")
