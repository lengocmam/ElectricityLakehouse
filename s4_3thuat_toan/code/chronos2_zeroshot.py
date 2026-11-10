"""Chronos-2 zero-shot (mo hinh nen 120M tham so, Amazon) tren CUNG cac cua so test 2026 cua cac thi nghiem khac.

CHAY TREN MAY BAN (can internet de tai trong so tu Hugging Face):
    pip install "chronos-forecasting>=2.0" "pandas[pyarrow]"
    python chronos2_zeroshot.py --target load_mean_mw --horizon 7           # CPU cung chay duoc, GPU nhanh hon
    python chronos2_zeroshot.py --target gen_hydro --horizon 1 --features cal,hydro

TRANG THAI: phan dung du lieu (--dry_run) toi da kiem tra trong moi truong cua toi; phan GOI MO HINH (Chronos2Pipeline.predict_df)
toi CHUA chay duoc vi moi truong cua toi khong tai duoc trong so, nen viet theo tai lieu chinh thuc cua Chronos-2 va chua kiem chung.
Neu ten cot ket qua khac, sua ham `pick_median` ben duoi.

Thong tin dua cho mo hinh (giong cac mo hinh khac): lich su cua target va cac bien qua khu (thoi tiet, ngay le...), cong voi
dac trung BIET TRUOC (lich, ngay le) cua cac ngay can du bao. Khong tinh chinh gi (zero-shot).
Ket qua: results/chronos2/<tag>.npz (cung dinh dang voi cac thi nghiem khac, khoa Chronos2_to_hop) va in MAE/MASE de so voi bang chinh.
"""
import argparse
import os
import numpy as np
import pandas as pd

from data import FEATURE_GROUPS, FUTURE_COLS, prepare


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", default="unified_daily_v2.csv")
    ap.add_argument("--target", default="load_mean_mw")
    ap.add_argument("--features", default="cal,wx,hol", help="nhom bien QUA KHU dua vao lam covariate ('' = chi target)")
    ap.add_argument("--horizon", type=int, default=7)
    ap.add_argument("--train_end", default="2025-06-30")
    ap.add_argument("--val_end", default="2025-12-31")
    ap.add_argument("--max_context", type=int, default=730, help="so ngay lich su toi da dua vao mo hinh")
    ap.add_argument("--batch_ids", type=int, default=32)
    ap.add_argument("--device", default="cpu")
    ap.add_argument("--out", default="results/chronos2")
    ap.add_argument("--dry_run", action="store_true", help="chi dung va kiem tra du lieu, khong goi mo hinh")
    a = ap.parse_args()

    feats = [f for f in a.features.split(",") if f]
    H = a.horizon
    prep = prepare(a.csv, a.target, feats, 56, H, a.train_end, a.val_end)   # cung cua so test voi cac thi nghiem khac
    W = prep.win
    test_dates = pd.to_datetime(W.dates[W.split == 2])
    y_true = W.Y[W.split == 2] * prep.t_std + prep.t_mean
    df = prep.df.copy()
    df["date"] = pd.to_datetime(df["date"])
    past_cols = [c for g in feats for c in FEATURE_GROUPS[g] if c in df.columns and c != a.target]
    fut_cols = [c for c in FUTURE_COLS if c in df.columns]
    past_cols = [c for c in dict.fromkeys(past_cols) if c not in fut_cols or True]
    df[past_cols] = df[past_cols].ffill().bfill()
    df[fut_cols] = df[fut_cols].ffill().bfill()
    df[a.target] = df[a.target].ffill()                       # ngay thieu: dien tien de co chuoi lien tuc
    print(f"cua so test: {len(test_dates)}  horizon={H}  covariate qua khu: {len(past_cols)}  covariate biet truoc: {len(fut_cols)}")

    ctx_parts, fut_parts = [], []
    for i, d0 in enumerate(test_dates):
        hist = df[(df.date < d0) & (df.date >= d0 - pd.Timedelta(days=a.max_context))]
        c = hist[["date", a.target] + [x for x in past_cols if x not in fut_cols] + fut_cols].copy()
        c.insert(0, "id", i)
        ctx_parts.append(c)
        fdates = pd.date_range(d0, periods=H)
        f = df.set_index("date").reindex(fdates)[fut_cols].reset_index().rename(columns={"index": "date"})
        f.insert(0, "id", i)
        fut_parts.append(f)
    ctx = pd.concat(ctx_parts, ignore_index=True).rename(columns={"date": "timestamp", a.target: "target"})
    fut = pd.concat(fut_parts, ignore_index=True).rename(columns={"date": "timestamp"})
    print("context_df:", ctx.shape, "| future_df:", fut.shape, "| NaN trong future:", int(fut.isna().sum().sum()))
    if a.dry_run:
        print("--dry_run: du lieu da dung xong, khong goi mo hinh.")
        return

    from chronos import Chronos2Pipeline                       # pip install "chronos-forecasting>=2.0"
    pipe = Chronos2Pipeline.from_pretrained("amazon/chronos-2", device_map=a.device)

    def pick_median(pred_df):
        for name in ("0.5", 0.5, "predictions", "mean"):
            if name in pred_df.columns:
                return pred_df[name].values
        cand = [c for c in pred_df.columns if c not in ("id", "timestamp", "target")]
        return pred_df[cand[len(cand) // 2]].values

    preds = np.zeros((len(test_dates), H))
    ids = np.arange(len(test_dates))
    for s in range(0, len(ids), a.batch_ids):
        b = ids[s:s + a.batch_ids]
        out = pipe.predict_df(ctx[ctx.id.isin(b)], future_df=fut[fut.id.isin(b)], prediction_length=H,
                              quantile_levels=[0.1, 0.5, 0.9], id_column="id", timestamp_column="timestamp", target="target")
        out = out.sort_values(["id", "timestamp"])
        preds[b] = pick_median(out).reshape(len(b), H)
        print(f"  {min(s + a.batch_ids, len(ids))}/{len(ids)} cua so", flush=True)

    mae = np.abs(preds - y_true).mean()
    print(f"\nChronos-2 zero-shot: MAE = {mae:.1f}  (so voi bang chinh cung bai toan/horizon)")
    os.makedirs(a.out, exist_ok=True)
    tag = f"{a.target}_H{H}"
    np.savez(f"{a.out}/{tag}.npz", dates=test_dates.values.astype("datetime64[D]"), true=y_true, Chronos2_to_hop=preds)
    print("da ghi", f"{a.out}/{tag}.npz")


if __name__ == "__main__":
    main()
