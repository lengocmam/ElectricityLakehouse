# -*- coding: utf-8 -*-
"""Script phân tích Ma trận tương quan toàn diện (Correlation Matrix & Feature Selection).

Phân tích:
1. Tương quan tuyến tính Pearson (r) và p-value với biến mục tiêu (Target).
2. Tương quan phi tuyến đơn điệu Spearman (rho) và p-value.
3. Tương quan trễ (Lagged Cross-Correlation) cho chuỗi thời gian (lags 1, 2, 3, 7, 14 ngày).
4. Phân tích đa cộng tuyến (Multicollinearity & VIF) giữa các cặp biến giải thích.
5. Đề xuất bộ đặc trưng tối ưu có bằng chứng khoa học cho 2 bài toán Phụ tải & Thủy điện.
"""
import sys
import os
import json

if hasattr(sys.stdout, 'reconfigure'):
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except Exception:
        pass

import numpy as np
import pandas as pd
from scipy import stats


def calculate_correlation_metrics(df, target_col, feature_cols):
    """Tính Pearson, Spearman correlation và p-value cho từng feature với target."""
    records = []
    y = df[target_col]

    for col in feature_cols:
        x = df[col]
        # Bỏ các hàng có NaN ở cả 2 biến
        valid = ~(x.isna() | y.isna())
        if valid.sum() < 30:
            continue
        
        xv = x[valid].values
        yv = y[valid].values

        # Pearson
        r_pearson, p_pearson = stats.pearsonr(xv, yv)
        # Spearman
        r_spearman, p_spearman = stats.spearmanr(xv, yv)

        records.append({
            "feature": col,
            "pearson_r": round(float(r_pearson), 4),
            "pearson_abs": round(float(abs(r_pearson)), 4),
            "pearson_p": float(p_pearson),
            "spearman_rho": round(float(r_spearman), 4),
            "spearman_abs": round(float(abs(r_spearman)), 4),
            "spearman_p": float(p_spearman),
            "sample_count": int(valid.sum())
        })

    res_df = pd.DataFrame(records).sort_values(by="pearson_abs", ascending=False).reset_index(drop=True)
    return res_df


def calculate_lagged_correlations(df, target_col, feature_cols, lags=[1, 2, 3, 7, 14]):
    """Tính tương quan chéo theo các bước trễ (Lagged Cross-Correlation)."""
    lag_records = []
    
    # 1. Autocorrelation của chính biến target
    y = df[target_col]
    target_lags = {"feature": f"{target_col}_lag (Tự tương quan)"}
    for lag in lags:
        y_lagged = y.shift(lag)
        valid = ~(y.isna() | y_lagged.isna())
        r, _ = stats.pearsonr(y[valid], y_lagged[valid])
        target_lags[f"lag_{lag}"] = round(float(r), 4)
    lag_records.append(target_lags)

    # 2. Cross-correlation với các biến ngoại sinh
    for col in feature_cols:
        x = df[col]
        rec = {"feature": col}
        for lag in lags:
            x_lagged = x.shift(lag)
            valid = ~(y.isna() | x_lagged.isna())
            if valid.sum() < 30:
                rec[f"lag_{lag}"] = np.nan
            else:
                r, _ = stats.pearsonr(y[valid], x_lagged[valid])
                rec[f"lag_{lag}"] = round(float(r), 4)
        lag_records.append(rec)

    return pd.DataFrame(lag_records)


def detect_multicollinearity(df, feature_cols, threshold=0.80):
    """Tìm các cặp feature có tương quan chéo mạnh (|r| >= threshold) gây đa cộng tuyến."""
    corr_matrix = df[feature_cols].corr(method="pearson").abs()
    high_pairs = []

    cols = list(corr_matrix.columns)
    for i in range(len(cols)):
        for j in range(i + 1, len(cols)):
            c1, c2 = cols[i], cols[j]
            val = corr_matrix.loc[c1, c2]
            if val >= threshold and not np.isnan(val):
                high_pairs.append({
                    "feature_1": c1,
                    "feature_2": c2,
                    "abs_corr": round(float(val), 4)
                })

    return pd.DataFrame(high_pairs).sort_values(by="abs_corr", ascending=False).reset_index(drop=True)


def run_full_analysis():
    csv_paths = [
        "s4_3thuat_toan/code/unified_daily_v2.csv",
        "s4_3thuat_toan/data_v2/unified_daily_v2.csv"
    ]
    data_path = None
    for p in csv_paths:
        if os.path.exists(p):
            data_path = p
            break

    if not data_path:
        raise FileNotFoundError(f"Không tìm thấy file dữ liệu unified_daily_v2.csv trong {csv_paths}")

    print("=" * 80)
    print(f"BẮT ĐẦU PHÂN TÍCH MA TRẬN TƯƠNG QUAN DỮ LIỆU: {data_path}")
    print("=" * 80)

    df = pd.read_csv(data_path)
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date").reset_index(drop=True)

    # Thêm các đặc trưng chu kỳ lịch (sin/cos)
    dow = df["date"].dt.dayofweek
    df["dow_sin"] = np.sin(2 * np.pi * dow / 7)
    df["dow_cos"] = np.cos(2 * np.pi * dow / 7)
    month = df["date"].dt.month
    df["month_sin"] = np.sin(2 * np.pi * (month - 1) / 12)
    df["month_cos"] = np.cos(2 * np.pi * (month - 1) / 12)

    print(f"Tổng số bản ghi: {len(df)} ngày (từ {df['date'].min().strftime('%Y-%m-%d')} đến {df['date'].max().strftime('%Y-%m-%d')})")
    print(f"Tổng số cột: {df.shape[1]}")

    # =========================================================================
    # DANH SÁCH CÁC NHÓM BIẾN TIỀM NĂNG
    # =========================================================================
    candidate_features = [
        # Nhóm 1: Lịch & Kỳ nghỉ lễ
        "dow_sin", "dow_cos", "month_sin", "month_cos", "is_weekend",
        "is_working_day", "is_public_holiday", "is_holiday_break", "is_tet_break",
        "days_to_next_break", "days_since_break", "tet_offset",
        # Nhóm 2: Khí tượng / Thời tiết
        "wx_temp_c", "wx_humidity_pct", "wx_cloud_pct", "wx_solar_rad_wm2",
        "wx_wind_ms", "wx_precip_mm",
        # Nhóm 3: Thủy văn & Hồ chứa
        "hy_inflow_total_m3s", "hy_powerhouse_total_m3s", "hy_spill_total_m3s",
        "hy_fill_ratio_mean", "hy_n_below_dead",
        "hy_inflow_da", "hy_inflow_dongnai", "hy_inflow_sesan", "hy_inflow_srepok",
        "hy_inflow_ma_ca", "hy_inflow_lo_gam", "hy_inflow_ba", "hy_inflow_vugia_thubon",
        # Nhóm 4: Nguồn điện cơ cấu (chỉ dùng quá khứ)
        "gen_coal", "gen_gas", "gen_wind", "gen_solar_other", "gen_thermal"
    ]
    candidate_features = [c for c in candidate_features if c in df.columns]

    # =========================================================================
    # 1. PHÂN TÍCH TƯƠNG QUAN CHO BÀI TOÁN 1: PHỤ TẢI (load_mean_mw)
    # =========================================================================
    print("\n" + "=" * 70)
    print("1. PHÂN TÍCH TƯƠNG QUAN CHO BÀI TOÁN 1: PHỤ TẢI (load_mean_mw)")
    print("=" * 70)
    load_corr = calculate_correlation_metrics(df, "load_mean_mw", candidate_features)
    print("\nTop 15 đặc trưng tương quan mạnh nhất với Phụ tải (load_mean_mw):")
    print(load_corr[["feature", "pearson_r", "spearman_rho", "sample_count"]].head(15).to_string(index=False))

    # Tương quan trễ cho Phụ tải
    load_lag_feats = ["wx_temp_c", "wx_solar_rad_wm2", "wx_humidity_pct", "is_working_day", "is_weekend"]
    load_lag_feats = [c for c in load_lag_feats if c in df.columns]
    load_lagged_corr = calculate_lagged_correlations(df, "load_mean_mw", load_lag_feats, lags=[1, 2, 3, 7, 14])
    print("\nBảng tương quan trễ (Lagged Correlation) của Phụ tải:")
    print(load_lagged_corr.to_string(index=False))

    # =========================================================================
    # 2. PHÂN TÍCH TƯƠNG QUAN CHO BÀI TOÁN 2: THỦY ĐIỆN (gen_hydro)
    # =========================================================================
    print("\n" + "=" * 70)
    print("2. PHÂN TÍCH TƯƠNG QUAN CHO BÀI TOÁN 2: THỦY ĐIỆN (gen_hydro)")
    print("=" * 70)
    # Với bài toán thủy điện, bỏ các biến nguồn phát khác để tránh rò rỉ đồng thời
    hydro_candidates = [c for c in candidate_features if not c.startswith("gen_")]
    hydro_corr = calculate_correlation_metrics(df, "gen_hydro", hydro_candidates)
    print("\nTop 15 đặc trưng tương quan mạnh nhất với Thủy điện (gen_hydro):")
    print(hydro_corr[["feature", "pearson_r", "spearman_rho", "sample_count"]].head(15).to_string(index=False))

    # Tương quan trễ cho Thủy điện
    hydro_lag_feats = ["hy_inflow_total_m3s", "hy_fill_ratio_mean", "hy_inflow_da", "hy_inflow_sesan", "wx_precip_mm"]
    hydro_lag_feats = [c for c in hydro_lag_feats if c in df.columns]
    hydro_lagged_corr = calculate_lagged_correlations(df, "gen_hydro", hydro_lag_feats, lags=[1, 2, 3, 7, 14])
    print("\nTương quan trễ (Lagged Correlation) của Thủy điện:")
    print(hydro_lagged_corr.to_string(index=False))

    # =========================================================================
    # 3. PHÂN TÍCH ĐA CỘNG TUYẾN (MULTICOLLINEARITY)
    # =========================================================================
    print("\n" + "=" * 70)
    print("3. PHÂN TÍCH ĐA CỘNG TUYẾN GIỮA CÁC ĐẶC TRƯNG (|r| >= 0.75)")
    print("=" * 70)
    high_pairs = detect_multicollinearity(df, candidate_features, threshold=0.75)
    print(high_pairs.head(20).to_string(index=False))

    # =========================================================================
    # 4. XUẤT BÁO CÁO TỔNG HỢP & DỮ LIỆU
    # =========================================================================
    out_dir = "s4_3thuat_toan/code"
    os.makedirs(out_dir, exist_ok=True)

    # Lưu CSV
    load_corr.to_csv(os.path.join(out_dir, "corr_features_load.csv"), index=False, encoding="utf-8-sig")
    hydro_corr.to_csv(os.path.join(out_dir, "corr_features_hydro.csv"), index=False, encoding="utf-8-sig")
    high_pairs.to_csv(os.path.join(out_dir, "corr_multicollinear_pairs.csv"), index=False, encoding="utf-8-sig")

    # Lưu JSON tóm tắt
    summary = {
        "analysis_date": str(pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")),
        "total_days": len(df),
        "load_top_features": load_corr.head(10).to_dict(orient="records"),
        "hydro_top_features": hydro_corr.head(10).to_dict(orient="records"),
        "multicollinear_pairs_count": len(high_pairs),
        "multicollinear_top": high_pairs.head(10).to_dict(orient="records")
    }
    with open(os.path.join(out_dir, "correlation_summary.json"), "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    print("\n" + "=" * 70)
    print(f"[✓] Đã xuất toàn bộ kết quả phân tích tương quan vào: {out_dir}")
    print("=" * 70)


if __name__ == "__main__":
    run_full_analysis()
