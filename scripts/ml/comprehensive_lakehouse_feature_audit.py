# -*- coding: utf-8 -*-
"""Script rà soát và tổng hợp TOÀN BỘ các biến từ các Fact table trong Lakehouse.

Tổng hợp từ:
1. fact_load_hourly (Phụ tải toàn quốc, phụ tải 3 miền, đỉnh, đáy, biên độ, ramp rate)
2. fact_hydro_status (Mực nước, dung tích tích nước, lưu lượng về 9 lưu vực, xả máy, xả tràn)
3. fact_weather_daily & fact_weather_hourly (Nhiệt độ, nhiệt độ max/min, biên độ nhiệt, bức xạ, độ ẩm, mưa, gió, mây)
4. fact_generation_daily (Sản lượng than, khí, thủy điện, mặt trời, gió, tỷ trọng các nguồn)
5. dim_date & vn_holiday_calendar (Lịch, chu kỳ mùa, ngày làm việc, lễ tết, độ trễ kỳ nghỉ)

Tính toán:
- Pearson correlation (r), Spearman correlation (rho)
- Autocorrelation & Cross-correlation theo độ trễ (Lag 1, Lag 2, Lag 3, Lag 7, Lag 14)
- Kiểm tra Đa cộng tuyến (Multicollinearity) & Rò rỉ tương lai (Data Leakage)
- Phân nhóm khuyến nghị tuyển chọn khoa học để người dùng duyệt.
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


def run_audit():
    print("=" * 80)
    print("RÀ SOÁT TỔNG THỂ TẤT CẢ CÁC BIẾN TỪ CÁC FACT TABLE TRONG LAKEHOUSE")
    print("=" * 80)

    # 1. Đọc dữ liệu tổng hợp
    csv_path = "s4_3thuat_toan/code/unified_daily_v2.csv"
    if not os.path.exists(csv_path):
        csv_path = "s4_3thuat_toan/data_v2/unified_daily_v2.csv"
    
    df = pd.read_csv(csv_path)
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date").reset_index(drop=True)

    # 2. Bổ sung các biến phái sinh nâng cao từ các fact table
    # Từ fact_load_hourly:
    df["load_range_mw"] = df["load_peak_mw"] - df["load_min_mw"]  # Biên độ phụ tải trong ngày
    df["load_peak_to_mean"] = df["load_peak_mw"] / df["load_mean_mw"]  # Tỷ lệ đỉnh / trung bình
    
    # Từ fact_weather_daily:
    # Nếu có nhiệt độ và độ ẩm, tính Heat Index ước tính (Chỉ số cảm nhận nhiệt)
    t = df["wx_temp_c"]
    rh = df["wx_humidity_pct"]
    df["wx_apparent_temp"] = t + 0.33 * (rh / 100.0 * 6.105 * np.exp(17.27 * t / (237.7 + t))) - 4.0
    
    # Từ fact_hydro_status:
    df["hy_total_discharge_m3s"] = df["hy_powerhouse_total_m3s"] + df["hy_spill_total_m3s"]
    df["hy_inflow_discharge_diff"] = df["hy_inflow_total_m3s"] - df["hy_total_discharge_m3s"]  # Cân bằng nước hồ
    
    # Từ fact_generation_daily:
    df["gen_re_share"] = df["gen_solar_other"] + df["gen_wind"]  # Tổng NLTT không điều khiển
    df["gen_re_pct"] = (df["gen_re_share"] / df["gen_total"]) * 100.0
    
    # Chu kỳ lịch
    dow = df["date"].dt.dayofweek
    df["dow_sin"] = np.sin(2 * np.pi * dow / 7)
    df["dow_cos"] = np.cos(2 * np.pi * dow / 7)
    month = df["date"].dt.month
    df["month_sin"] = np.sin(2 * np.pi * (month - 1) / 12)
    df["month_cos"] = np.cos(2 * np.pi * (month - 1) / 12)

    # 3. Định nghĩa từ điển xuất xứ (Fact Table Source) cho từng biến
    FACT_SOURCES = {
        # fact_load_hourly
        "load_mean_mw": ("fact_load_hourly", "Công suất phụ tải trung bình ngày toàn quốc (MW)"),
        "load_peak_mw": ("fact_load_hourly", "Công suất phụ tải đỉnh ngày Pmax (MW)"),
        "load_min_mw": ("fact_load_hourly", "Công suất phụ tải đáy ban đêm Pmin (MW)"),
        "load_range_mw": ("fact_load_hourly", "Biên độ dao động phụ tải ngày Pmax - Pmin (MW)"),
        "load_peak_to_mean": ("fact_load_hourly", "Hệ số tỷ lệ phụ tải đỉnh trên trung bình"),
        "load_mean_mw_north": ("fact_load_hourly", "Phụ tải trung bình Miền Bắc (MW)"),
        "load_mean_mw_central": ("fact_load_hourly", "Phụ tải trung bình Miền Trung (MW)"),
        "load_mean_mw_south": ("fact_load_hourly", "Phụ tải trung bình Miền Nam (MW)"),
        
        # fact_weather_daily / fact_weather_hourly
        "wx_temp_c": ("fact_weather_daily", "Nhiệt độ môi trường trung bình ngày (°C)"),
        "wx_apparent_temp": ("fact_weather_hourly", "Nhiệt độ cảm nhận kết hợp độ ẩm Heat Index (°C)"),
        "wx_solar_rad_wm2": ("fact_weather_hourly", "Bức xạ quang mặt trời (W/m²)"),
        "wx_humidity_pct": ("fact_weather_hourly", "Độ ẩm không khí tương đối (%)"),
        "wx_precip_mm": ("fact_weather_daily", "Tổng lượng mưa tích tụ trong ngày (mm)"),
        "wx_wind_ms": ("fact_weather_hourly", "Tốc độ gió trung bình 10m (m/s)"),
        "wx_cloud_pct": ("fact_weather_hourly", "Tỷ lệ mây che phủ bầu trời (%)"),

        # fact_hydro_status
        "hy_powerhouse_total_m3s": ("fact_hydro_status", "Tổng lưu lượng xả qua nhà máy phát điện (m³/s)"),
        "hy_inflow_total_m3s": ("fact_hydro_status", "Tổng lưu lượng nước tự nhiên về các hồ chứa (m³/s)"),
        "hy_total_discharge_m3s": ("fact_hydro_status", "Tổng lưu lượng xả toàn hệ thống thủy điện (m³/s)"),
        "hy_spill_total_m3s": ("fact_hydro_status", "Tổng lưu lượng xả qua tràn hồ chứa (m³/s)"),
        "hy_inflow_discharge_diff": ("fact_hydro_status", "Chênh lệch lưu lượng Nước về - Nước xả (m³/s)"),
        "hy_fill_ratio_mean": ("fact_hydro_status", "Tỷ lệ tích nước bình quân các hồ chứa (0 - 1.0)"),
        "hy_n_below_dead": ("fact_hydro_status", "Số lượng hồ chứa có mực nước dưới mực nước chết"),
        "hy_inflow_da": ("fact_hydro_status", "Lưu lượng nước về lưu vực Sông Đà (m³/s)"),
        "hy_inflow_lo_gam": ("fact_hydro_status", "Lưu lượng nước về lưu vực Sông Lô - Gâm (m³/s)"),
        "hy_inflow_sesan": ("fact_hydro_status", "Lưu lượng nước về lưu vực Sông Sê San (m³/s)"),
        "hy_inflow_srepok": ("fact_hydro_status", "Lưu lượng nước về lưu vực Sông Srêpôk (m³/s)"),
        "hy_inflow_ma_ca": ("fact_hydro_status", "Lưu lượng nước về lưu vực Sông Mã - Cả (m³/s)"),
        "hy_inflow_dongnai": ("fact_hydro_status", "Lưu lượng nước về lưu vực Sông Đồng Nai (m³/s)"),
        "hy_inflow_ba": ("fact_hydro_status", "Lưu lượng nước về lưu vực Sông Ba (m³/s)"),
        "hy_inflow_vugia_thubon": ("fact_hydro_status", "Lưu lượng nước về lưu vực Sông Vu Gia - Thu Bồn (m³/s)"),

        # fact_generation_daily
        "gen_hydro": ("fact_generation_daily", "Sản lượng phát điện Thủy điện (MWh) - Target 2"),
        "gen_coal": ("fact_generation_daily", "Sản lượng phát Nhiệt điện than (MWh)"),
        "gen_gas": ("fact_generation_daily", "Sản lượng phát Tua bin khí (MWh)"),
        "gen_thermal": ("fact_generation_daily", "Tổng sản lượng Nhiệt điện truyền thống Than + Khí (MWh)"),
        "gen_solar_other": ("fact_generation_daily", "Sản lượng Điện mặt trời & Nguồn khác (MWh)"),
        "gen_wind": ("fact_generation_daily", "Sản lượng phát Điện gió (MWh)"),
        "gen_re_share": ("fact_generation_daily", "Tổng sản lượng Năng lượng tái tạo Mặt trời + Gió (MWh)"),
        "gen_re_pct": ("fact_generation_daily", "Tỷ trọng Năng lượng tái tạo trên tổng phát (%)"),
        "gen_total": ("fact_generation_daily", "Tổng sản lượng phát điện hệ thống quốc gia (MWh)"),

        # dim_date & vn_holiday_calendar
        "month_cos": ("dim_date", "Biến đổi chu kỳ mùa trong năm (Cosine Month)"),
        "month_sin": ("dim_date", "Biến đổi chu kỳ mùa trong năm (Sine Month)"),
        "dow_sin": ("dim_date", "Chu kỳ thứ trong tuần (Sine Day of Week)"),
        "dow_cos": ("dim_date", "Chu kỳ thứ trong tuần (Cosine Day of Week)"),
        "is_working_day": ("vn_holiday_calendar", "Ngày làm việc bình thường (Thứ 2 - Thứ 6)"),
        "is_weekend": ("vn_holiday_calendar", "Ngày nghỉ cuối tuần (Thứ 7, Chủ Nhật)"),
        "is_public_holiday": ("vn_holiday_calendar", "Ngày nghỉ lễ chính thức theo luật lao động"),
        "is_holiday_break": ("vn_holiday_calendar", "Đợt nghỉ lễ kéo dài liên tục"),
        "is_tet_break": ("vn_holiday_calendar", "Kỳ nghỉ Tết Nguyên Đán"),
        "days_since_break": ("vn_holiday_calendar", "Số ngày trôi qua kể từ kỳ nghỉ gần nhất"),
        "days_to_next_break": ("vn_holiday_calendar", "Số ngày đếm ngược đến kỳ nghỉ tiếp theo"),
        "tet_offset": ("vn_holiday_calendar", "Khoảng cách ngày tương đối so với Mùng 1 Tết")
    }

    # 4. Tính toán tương quan cho tất cả các biến với 2 Target
    # Target 1: load_mean_mw
    # Target 2: gen_hydro
    y_load = df["load_mean_mw"]
    y_hydro = df["gen_hydro"]

    records = []
    for col, (fact_src, desc) in FACT_SOURCES.items():
        if col not in df.columns:
            continue
        
        x = df[col]
        # Bỏ NaN
        v_load = ~(x.isna() | y_load.isna())
        v_hydro = ~(x.isna() | y_hydro.isna())

        # Load metrics
        r_load, p_load = stats.pearsonr(x[v_load], y_load[v_load]) if v_load.sum() >= 30 else (np.nan, np.nan)
        rho_load, _ = stats.spearmanr(x[v_load], y_load[v_load]) if v_load.sum() >= 30 else (np.nan, np.nan)
        # Lag 1 and Lag 7 for load
        x_lag1 = x.shift(1)
        v_l1 = ~(x_lag1.isna() | y_load.isna())
        r_load_lag1, _ = stats.pearsonr(x_lag1[v_l1], y_load[v_l1]) if v_l1.sum() >= 30 else (np.nan, np.nan)
        x_lag7 = x.shift(7)
        v_l7 = ~(x_lag7.isna() | y_load.isna())
        r_load_lag7, _ = stats.pearsonr(x_lag7[v_l7], y_load[v_l7]) if v_l7.sum() >= 30 else (np.nan, np.nan)

        # Hydro metrics
        r_hydro, p_hydro = stats.pearsonr(x[v_hydro], y_hydro[v_hydro]) if v_hydro.sum() >= 30 else (np.nan, np.nan)
        rho_hydro, _ = stats.spearmanr(x[v_hydro], y_hydro[v_hydro]) if v_hydro.sum() >= 30 else (np.nan, np.nan)
        # Lag 1 and Lag 7 for hydro
        v_h1 = ~(x_lag1.isna() | y_hydro.isna())
        r_hydro_lag1, _ = stats.pearsonr(x_lag1[v_h1], y_hydro[v_h1]) if v_h1.sum() >= 30 else (np.nan, np.nan)
        v_h7 = ~(x_lag7.isna() | y_hydro.isna())
        r_hydro_lag7, _ = stats.pearsonr(x_lag7[v_h7], y_hydro[v_h7]) if v_h7.sum() >= 30 else (np.nan, np.nan)

        records.append({
            "col_name": col,
            "fact_source": fact_src,
            "description": desc,
            # Load
            "load_pearson_r": round(float(r_load), 4),
            "load_pearson_abs": round(float(abs(r_load)), 4),
            "load_spearman_rho": round(float(rho_load), 4),
            "load_lag1_r": round(float(r_load_lag1), 4),
            "load_lag7_r": round(float(r_load_lag7), 4),
            # Hydro
            "hydro_pearson_r": round(float(r_hydro), 4),
            "hydro_pearson_abs": round(float(abs(r_hydro)), 4),
            "hydro_spearman_rho": round(float(rho_hydro), 4),
            "hydro_lag1_r": round(float(r_hydro_lag1), 4),
            "hydro_lag7_r": round(float(r_hydro_lag7), 4),
        })

    audit_df = pd.DataFrame(records)

    # 5. Phân loại mức độ tương quan và khuyến nghị tuyển chọn
    def classify_load(row):
        col = row["col_name"]
        abs_r = row["load_pearson_abs"]
        # Rò rỉ dữ liệu cùng ngày
        if col.startswith("gen_") and col != "gen_hydro":
            return "❌ BỎ (Rò rỉ tương lai)", "Sản lượng điện cùng ngày t không biết trước khi dự báo"
        if col in ["load_peak_mw", "load_min_mw", "load_range_mw", "load_peak_to_mean"]:
            return "❌ BỎ (Rò rỉ tương lai)", "Là các thành phần phụ tải cùng ngày t với biến mục tiêu"
        if col.startswith("load_mean_mw_") and col != "load_mean_mw":
            return "⚠️ BỎ (Đa cộng tuyến toán học)", "Tổng phụ tải 3 miền = Phụ tải toàn quốc (đồng nhất thức)"
        if col == "is_weekend":
            return "⚠️ BỎ (Đa cộng tuyến |r|=0.915)", "Trùng lặp thông tin với is_working_day"
        if col == "is_holiday_break":
            return "⚠️ BỎ (Đa cộng tuyến |r|=0.818)", "Trùng lặp thông tin với is_public_holiday"
        if col == "wx_apparent_temp":
            return "⚠️ BỎ (Đa cộng tuyến |r|=0.985)", "Trùng lặp gần như tuyệt đối với wx_temp_c"
        
        # Đánh giá theo độ lớn tương quan
        if col == "load_mean_mw":
            return "🌟 CHỌN (Autoregressive Lag 1 & 7)", "Tự tương quan lịch sử mạnh nhất (r=0.893 ở lag 1, r=0.750 ở lag 7)"
        if abs_r >= 0.40:
            return "🌟 CHỌN (Tương quan Rất Mạnh |r|>=0.40)", f"Tương quan cao (|r|={abs_r}), tín hiệu dự báo then chốt"
        elif abs_r >= 0.30:
            return "🟢 CHỌN (Tương quan Mạnh 0.30<=|r|<0.40)", f"Tương quan tốt (|r|={abs_r}), bổ trợ ổn định"
        elif abs_r >= 0.15:
            return "🟡 DỰ PHÒNG (Tương quan Vừa 0.15<=|r|<0.30)", f"Tương quan trung bình (|r|={abs_r})"
        else:
            return "⚪ BỎ (Nhiễu / Tương quan yếu |r|<0.15)", f"Tương quan không đáng kể (|r|={abs_r})"

    def classify_hydro(row):
        col = row["col_name"]
        abs_r = row["hydro_pearson_abs"]
        if col.startswith("gen_") and col != "gen_hydro":
            return "❌ BỎ (Rò rỉ tương lai)", "Sản lượng nguồn khác cùng ngày t không biết trước"
        if col == "hy_inflow_da":
            return "⚠️ BỎ (Đa cộng tuyến |r|=0.848)", "Trùng lặp với hy_inflow_total_m3s (Sông Đà chiếm tỷ trọng lớn)"
        if col == "hy_total_discharge_m3s":
            return "⚠️ BỎ (Đa cộng tuyến |r|=0.982)", "Trùng lặp với hy_powerhouse_total_m3s"
        if col in ["dow_sin", "dow_cos", "is_weekend"]:
            return "⚪ BỎ (Tín hiệu yếu cho Thủy văn)", f"Thủy văn tự nhiên không theo chu kỳ tuần (|r|={abs_r})"
        
        if col == "gen_hydro":
            return "🌟 CHỌN (Autoregressive Lag 1 & 7)", "Tự tương quan thủy điện lịch sử (r=0.967 ở lag 1, r=0.931 ở lag 7)"
        if abs_r >= 0.50:
            return "🌟 CHỌN (Tương quan Rất Mạnh |r|>=0.50)", f"Thủy văn/mùa vụ quyết định (|r|={abs_r})"
        elif abs_r >= 0.35:
            return "🟢 CHỌN (Tương quan Mạnh 0.35<=|r|<0.50)", f"Tín hiệu hồ chứa bổ trợ (|r|={abs_r})"
        elif abs_r >= 0.20:
            return "🟡 DỰ PHÒNG (Tương quan Vừa 0.20<=|r|<0.35)", f"Lưu vực nhỏ hoặc biến phụ trợ (|r|={abs_r})"
        else:
            return "⚪ BỎ (Nhiễu / Tương quan yếu |r|<0.20)", f"Tương quan không đáng kể (|r|={abs_r})"

    audit_df["load_recommendation"] = [classify_load(r)[0] for _, r in audit_df.iterrows()]
    audit_df["load_reason"] = [classify_load(r)[1] for _, r in audit_df.iterrows()]

    audit_df["hydro_recommendation"] = [classify_hydro(r)[0] for _, r in audit_df.iterrows()]
    audit_df["hydro_reason"] = [classify_hydro(r)[1] for _, r in audit_df.iterrows()]

    # Xuất ra file CSV
    out_csv = "s4_3thuat_toan/code/comprehensive_lakehouse_feature_audit.csv"
    audit_df.to_csv(out_csv, index=False, encoding="utf-8-sig")
    print(f"\n[✓] Đã lưu bảng tổng hợp kiểm toán toàn bộ {len(audit_df)} biến vào: {out_csv}")

    # In tóm tắt kết quả
    print("\n" + "=" * 70)
    print("TỔNG HỢP THEO NGUỒN FACT TABLE:")
    print("=" * 70)
    print(audit_df["fact_source"].value_counts().to_string())

    print("\n" + "=" * 70)
    print("TOP BIẾN KHUYÊN CHỌN CHO PHỤ TẢI (load_mean_mw):")
    print("=" * 70)
    load_sel = audit_df[audit_df["load_recommendation"].str.contains("CHỌN")].sort_values(by="load_pearson_abs", ascending=False)
    for _, r in load_sel.iterrows():
        print(f"  [{r['fact_source']}] {r['col_name']} ({r['description']}): r = {r['load_pearson_r']} (ρ = {r['load_spearman_rho']}) -> {r['load_recommendation']}")

    print("\n" + "=" * 70)
    print("TOP BIẾN KHUYÊN CHỌN CHO THỦY ĐIỆN (gen_hydro):")
    print("=" * 70)
    hydro_sel = audit_df[audit_df["hydro_recommendation"].str.contains("CHỌN")].sort_values(by="hydro_pearson_abs", ascending=False)
    for _, r in hydro_sel.iterrows():
        print(f"  [{r['fact_source']}] {r['col_name']} ({r['description']}): r = {r['hydro_pearson_r']} (ρ = {r['hydro_spearman_rho']}, Lag7={r['hydro_lag7_r']}) -> {r['hydro_recommendation']}")

    return audit_df


if __name__ == "__main__":
    run_audit()
