"""Tong hop ablation nhom dac trung (results/abl/<target>/<cau_hinh>).

In: MAE tung mo hinh theo tung tap dac trung; do thay doi so voi tap day du (full); kiem dinh Diebold-Mariano
cap doi cho tung buoc cong don va cho viec bo tung nhom khoi tap day du.
Chay:  python ablation_summary.py results/abl/load [results/abl/hydro]
"""
import glob
import json
import os
import sys
import numpy as np
from scipy import stats

MODELS = [("LightGBM_to_hop", "LightGBM"), ("S4D_to_hop", "S4D (to hop)")]
CUM = ["base", "cal", "cal_hol", "cal_hol_wx", "cal_hol_wx_hydro", "full"]
LOO = ["no_cal", "no_hol", "no_wx", "no_hydro"]
LABEL = {"base": "chi target", "cal": "+ lich", "cal_hol": "+ ngay le", "cal_hol_wx": "+ thoi tiet",
         "cal_hol_wx_hydro": "+ thuy van", "full": "+ san luong cac nguon (day du)",
         "no_cal": "day du tru lich", "no_hol": "day du tru ngay le", "no_wx": "day du tru thoi tiet",
         "no_hydro": "day du tru thuy van", "no_mix": "day du tru san luong (= + thuy van)"}


def dm_test(p1, p2, y, H):
    d = (np.abs(p1 - y) - np.abs(p2 - y)).mean(axis=1)
    n, db = len(d), d.mean()
    g = lambda k: np.mean((d[k:] - db) * (d[:n - k] - db))
    var = max(g(0) + 2 * sum(g(k) for k in range(1, H)), 1e-12)
    st = db / np.sqrt(var / n) * np.sqrt((n + 1 - 2 * H + H * (H - 1) / n) / n)
    return float(st), float(2 * (1 - stats.t.cdf(abs(st), df=n - 1)))


def load(d):
    z = np.load(glob.glob(f"{d}/stage3_preds_*.npz")[0])
    return {k: z[k] for k in z.files}


def align(a, b):
    """Giu cac cua so co chung ngay (phong khi hai tap dac trung co tap cua so khac nhau)."""
    da, db = a["dates"], b["dates"]
    common = np.intersect1d(da, db)
    ia, ib = np.isin(da, common), np.isin(db, common)
    return ia, ib


def summarize(root):
    target = os.path.basename(root.rstrip("/"))
    runs = {n: load(f"{root}/{n}") for n in CUM + LOO if os.path.isdir(f"{root}/{n}") and glob.glob(f"{root}/{n}/*.npz")}
    H = runs["full"]["true"].shape[1]
    mae = {n: {m: float(np.abs(r[m] - r["true"]).mean()) for m, _ in MODELS} for n, r in runs.items()}
    print(f"\n######## {target}  (horizon {H}, {len(runs['full']['true'])} cua so test)")
    print(f"{'tap dac trung':<38}" + "".join(f"{lab:>14}" for _, lab in MODELS))
    for n in CUM + LOO:
        if n in mae:
            print(f"{LABEL[n]:<38}" + "".join(f"{mae[n][m]:>14.0f}" for m, _ in MODELS))
    print(f"{LABEL['no_mix']:<38}" + "".join(f"{mae['cal_hol_wx_hydro'][m]:>14.0f}" for m, _ in MODELS))

    out = {"mae": mae, "dm_cumulative": {}, "dm_leave_one_out": {}}
    print("\nBuoc cong don: them nhom nay co giam sai so khong? (DM am = tap MOI tot hon; * = p < 0,05)")
    for prev, cur in zip(CUM[:-1], CUM[1:]):
        if prev not in runs or cur not in runs:
            continue
        line = f"{LABEL[cur]:<38}"
        ia, ib = align(runs[prev], runs[cur])
        for m, _ in MODELS:
            st, p = dm_test(runs[cur][m][ib], runs[prev][m][ia], runs[cur]["true"][ib], H)
            delta = (mae[cur][m] / mae[prev][m] - 1) * 100
            line += f"{delta:>+8.1f}% p={p:.2f}{'*' if p < 0.05 else ' '}"
            out["dm_cumulative"][f"{cur}|{m}"] = {"delta_pct": delta, "stat": st, "p": p}
        print(line)
    print("\nBo tung nhom khoi tap day du: bo nhom nay co lam te di khong? (DM duong = tap day du tot hon)")
    for n in LOO + ["no_mix"]:
        if n == "no_mix":
            other = "cal_hol_wx_hydro"
        else:
            other = n
        if other not in runs:
            continue
        ia, ib = align(runs["full"], runs[other])
        line = f"{LABEL[n]:<38}"
        for m, _ in MODELS:
            st, p = dm_test(runs["full"][m][ia], runs[other][m][ib], runs["full"]["true"][ia], H)
            delta = (mae[other][m] / mae["full"][m] - 1) * 100
            line += f"{delta:>+8.1f}% p={p:.2f}{'*' if p < 0.05 else ' '}"
            out["dm_leave_one_out"][f"{n}|{m}"] = {"delta_pct": delta, "stat": st, "p": p}
        print(line)
    json.dump(out, open(f"{root}/ablation_summary.json", "w"), indent=2)


if __name__ == "__main__":
    for r in sys.argv[1:] or ["results/abl/load"]:
        summarize(r)
