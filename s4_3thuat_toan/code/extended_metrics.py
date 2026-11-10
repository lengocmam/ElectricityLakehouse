"""Bo chi so MO RONG de kiem tra chinh xac hon (doc du bao da luu trong results/timexer/*/stage3_preds_*.npz).

Cho tung bai toan va tung mo hinh (S4D, TimeXer, S-Mamba + 2 moc tham chieu):
  - Do chinh xac:   MAE, RMSE, MAPE, sMAPE, WAPE, MASE, R2, he so tuong quan
  - Thien lech:     MBE (sai so trung binh, duong = du bao CAO hon thuc te), Bias %, ty le du bao cao
  - Sai so duoi:    MedAE, P95 sai so tuyet doi, P95 sai so %, sai so lon nhat, ty le ngay co sai so % vuot nguong
  - So voi moc:     Theil U2 (so voi persistence), skill so voi seasonal naive 7 ngay
  - Khoang tin cay: bootstrap theo khoi (khoi 7 cua so, 2000 lan) cho MAE va cho CHENH LECH MAE giua cac cap mo hinh
  - Theo horizon:   MAE tung buoc du bao (horizon 7)
  - Theo loai ngay: ngay thuong, cuoi tuan thuong, ky nghi le, ky nghi Tet

Chay:  python extended_metrics.py [thu_muc_ket_qua=results/timexer] [bang_ngay=unified_daily_v2.csv] [thu_muc_ra=results/extended]
"""
import glob
import json
import os
import sys
import numpy as np
import pandas as pd

ROOT = sys.argv[1] if len(sys.argv) > 1 else "results/timexer"
DAILY = sys.argv[2] if len(sys.argv) > 2 else "unified_daily_v2.csv"
OUT = sys.argv[3] if len(sys.argv) > 3 else "results/extended"
TASKS = [("Phụ tải 1 ngày (MW)", "load_H1", 5.0), ("Phụ tải 7 ngày (MW)", "load_H7", 5.0),
         ("Thủy điện 1 ngày (MWh)", "hydro_H1", 20.0), ("Thủy điện 7 ngày (MWh)", "hydro_H7", 20.0)]
MODELS = [("LightGBM_to_hop", "LightGBM"), ("TimeXer_to_hop", "TimeXer"), ("S4D_to_hop", "S4D"), ("S_Mamba_to_hop", "S-Mamba")]
REFS = [("persistence", "Persistence"), ("seasonal_naive_7", "Seasonal naive 7")]
B, BLOCK = 2000, 7


def num(x, d=0):
    s = f"{x:,.{d}f}"
    return s.replace(",", "§").replace(".", ",").replace("§", ".")


def point_metrics(p, y, scale, thr, p_pers, p_sn):
    e = p - y
    ae = np.abs(e)
    ape = ae / np.maximum(np.abs(y), 1e-9) * 100
    mae = ae.mean()
    mae_sn = np.abs(p_sn - y).mean()
    return {
        "MAE": mae, "RMSE": np.sqrt((e ** 2).mean()), "MAPE%": ape.mean(),
        "sMAPE%": (2 * ae / np.maximum(np.abs(p) + np.abs(y), 1e-9)).mean() * 100,
        "WAPE%": ae.sum() / np.abs(y).sum() * 100, "MASE": mae / scale,
        "R2": 1 - (e ** 2).sum() / ((y - y.mean()) ** 2).sum(), "Corr": np.corrcoef(p.ravel(), y.ravel())[0, 1],
        "MedAE": np.median(ae), "P95_AE": np.percentile(ae, 95), "Max_AE": ae.max(),
        "P95_APE%": np.percentile(ape, 95), f"APE>{thr:g}%": (ape > thr).mean() * 100,
        "MBE": e.mean(), "Bias%": e.mean() / y.mean() * 100, "Over%": (e > 0).mean() * 100,
        "TheilU2": np.sqrt((e ** 2).sum()) / np.sqrt(((p_pers - y) ** 2).sum()),
        "Skill_vs_SN%": (1 - mae / mae_sn) * 100,
    }


def block_idx(n, rng):
    k = int(np.ceil(n / BLOCK))
    starts = rng.integers(0, max(n - BLOCK + 1, 1), size=k)
    return np.concatenate([np.arange(s, min(s + BLOCK, n)) for s in starts])[:n]


daily = pd.read_csv(DAILY, parse_dates=["date"]).set_index("date")
os.makedirs(OUT, exist_ok=True)
md, long_rows = ["# Chỉ số mở rộng (test 2026, dự báo tổ hợp 5 seed)\n",
                 "Sinh tự động bởi `extended_metrics.py`. Với horizon 7, mỗi cửa sổ cho 7 dự báo; các chỉ số gộp trên mọi cặp (cửa sổ, bước dự báo). "
                 "Bias dương nghĩa là dự báo cao hơn thực tế. Khoảng tin cậy là bootstrap theo khối 7 cửa sổ (2000 lần).\n"], []
summary = {}
for label, tag, thr in TASKS:
    d = f"{ROOT}/{tag}"
    z = np.load(glob.glob(f"{d}/stage3_preds_*.npz")[0])
    zs = np.load(glob.glob(f"{ROOT}/../smamba/{tag}/stage3_preds_*.npz")[0])      # S-Mamba nam o results/smamba
    scale = json.load(open(glob.glob(f"{d}/stage3_*_fut.json")[0]))["mase_scale"]
    y, dates = z["true"], pd.to_datetime(z["dates"])
    n, H = y.shape
    P = {name: (z[k] if k in z.files else zs[k]) for k, name in MODELS + REFS}
    rows = {name: point_metrics(P[name], y, scale, thr, P["Persistence"], P["Seasonal naive 7"]) for _, name in MODELS + REFS}
    for name, r in rows.items():
        for k, v in r.items():
            long_rows.append({"task": tag, "model": name, "metric": k, "value": v})
    md.append(f"\n## {label}  ({n} cửa sổ, {n * H} cặp dự báo)\n")
    df = pd.DataFrame(rows).T
    t1 = ["MAE", "RMSE", "MAPE%", "sMAPE%", "WAPE%", "MASE", "R2", "Corr"]
    t2 = ["MedAE", "P95_AE", "Max_AE", "P95_APE%", f"APE>{thr:g}%", "MBE", "Bias%", "Over%", "TheilU2", "Skill_vs_SN%"]
    dec = {"MAE": 0, "RMSE": 0, "MedAE": 0, "P95_AE": 0, "Max_AE": 0, "MBE": 0, "R2": 3, "Corr": 3, "MASE": 3, "TheilU2": 3}
    for title, cols in [("Độ chính xác", t1), ("Thiên lệch và sai số đuôi", t2)]:
        md.append(f"**{title}**\n")
        md.append("| Mô hình | " + " | ".join(cols) + " |")
        md.append("|---|" + "---|" * len(cols))
        for name in df.index:
            md.append(f"| {name} | " + " | ".join(num(df.loc[name, c], dec.get(c, 2)) for c in cols) + " |")
        md.append("")

    # --- bootstrap theo khoi: khoang tin cay cua MAE va cua chenh lech MAE
    loss = {name: np.abs(P[name] - y).mean(axis=1) for _, name in MODELS}
    rng = np.random.default_rng(0)
    idxs = [block_idx(n, rng) for _ in range(B)]
    ci = lambda f: np.percentile([f(ix) for ix in idxs], [2.5, 97.5])
    md.append("**Khoảng tin cậy 95% (bootstrap theo khối)**\n")
    md.append("| Mô hình | MAE | KTC 95% |")
    md.append("|---|---|---|")
    for _, name in MODELS:
        lo, hi = ci(lambda ix, nm=name: loss[nm][ix].mean())
        md.append(f"| {name} | {num(loss[name].mean())} | [{num(lo)} ; {num(hi)}] |")
    md.append("\n| Chênh lệch MAE (A − B, âm = A tốt hơn) | Giá trị | KTC 95% | Kết luận |")
    md.append("|---|---|---|---|")
    for a, b in [("S4D", "LightGBM"), ("S4D", "TimeXer"), ("S4D", "S-Mamba"), ("TimeXer", "S-Mamba")]:
        diff = loss[a] - loss[b]
        lo, hi = ci(lambda ix, df_=diff: df_[ix].mean())
        verdict = f"{a} tốt hơn" if hi < 0 else (f"{b} tốt hơn" if lo > 0 else "KTC chứa 0, chưa phân biệt được")
        md.append(f"| {a} − {b} | {num(diff.mean())} | [{num(lo)} ; {num(hi)}] | {verdict} |")
        summary[(tag, a, b)] = (diff.mean(), lo, hi)
    md.append("")

    # --- MAE theo buoc du bao
    if H > 1:
        md.append("**MAE theo bước dự báo (ngày thứ h kể từ hôm nay)**\n")
        md.append("| Mô hình | " + " | ".join(f"h={h + 1}" for h in range(H)) + " |")
        md.append("|---|" + "---|" * H)
        for _, name in MODELS + REFS:
            md.append(f"| {name} | " + " | ".join(num(np.abs(P[name][:, h] - y[:, h]).mean()) for h in range(H)) + " |")
        md.append("")

    # --- theo loai ngay (ngay cua muc tieu, tinh theo tung buoc du bao)
    tgt = pd.DataFrame({h: dates + pd.Timedelta(days=h) for h in range(H)}).values  # (n, H) ngay muc tieu
    flat_dates = pd.to_datetime(tgt.ravel())
    fl = daily.reindex(flat_dates)
    tet = fl["is_tet_break"].fillna(0).values == 1
    brk = fl["is_holiday_break"].fillna(0).values == 1
    wkd = fl["is_weekend"].fillna(0).values == 1
    cat = np.where(tet, "Kỳ nghỉ Tết", np.where(brk, "Kỳ nghỉ lễ khác", np.where(wkd, "Cuối tuần thường", "Ngày thường")))
    md.append("**Theo loại ngày dự báo** (MAE / MAPE %)\n")
    cats = ["Ngày thường", "Cuối tuần thường", "Kỳ nghỉ lễ khác", "Kỳ nghỉ Tết"]
    md.append("| Mô hình | " + " | ".join(f"{c} (n={int((cat == c).sum())})" for c in cats) + " |")
    md.append("|---|" + "---|" * len(cats))
    for _, name in MODELS + REFS:
        ae = np.abs(P[name] - y).ravel()
        ap = ae / np.maximum(np.abs(y).ravel(), 1e-9) * 100
        cells = [f"{num(ae[cat == c].mean())} / {num(ap[cat == c].mean(), 1)}" if (cat == c).any() else "-" for c in cats]
        md.append(f"| {name} | " + " | ".join(cells) + " |")
        for c in cats:
            if (cat == c).any():
                long_rows.append({"task": tag, "model": name, "metric": f"MAE[{c}]", "value": ae[cat == c].mean()})
    md.append("")

pd.DataFrame(long_rows).to_csv(f"{OUT}/extended_metrics_long.csv", index=False)
open(f"{OUT}/EXTENDED_METRICS.md", "w", encoding="utf-8").write("\n".join(md))
print("da ghi", f"{OUT}/EXTENDED_METRICS.md", "va", f"{OUT}/extended_metrics_long.csv")
for k, (m, lo, hi) in summary.items():
    print(k, round(m), [round(lo), round(hi)])
