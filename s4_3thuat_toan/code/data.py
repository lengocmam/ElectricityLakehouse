"""Doc bang hop nhat theo ngay, tao dac trung, cat cua so truot, chia theo thoi gian.

Quy tac chong ro ri tuong lai:
  - Dau vao cua cua so ket thuc o ngay t chi gom du lieu cua ngay <= t.
  - Muc tieu la ngay t+1 ... t+H.
  - O khuyet: dien tien (ffill) roi moi dien trung vi cua tap train. Khong noi suy (se nhin vao tuong lai).
"""
from dataclasses import dataclass
import numpy as np
import pandas as pd

# Cac nhom dac trung (target luon la kenh so 0)
FEATURE_GROUPS = {
    # 1. Nhóm truyền thống đầy đủ
    "cal": ["dow_sin", "dow_cos", "month_sin", "month_cos", "is_weekend"],
    "wx": ["wx_temp_c", "wx_humidity_pct", "wx_precip_mm", "wx_solar_rad_wm2", "wx_wind_ms"],
    "hydro": ["hy_inflow_total_m3s", "hy_fill_ratio_mean", "hy_powerhouse_total_m3s"],
    "mix": ["gen_hydro", "gen_thermal", "gen_wind", "gen_solar_other", "gen_import"],
    "hol": ["is_public_holiday", "is_holiday_break", "is_tet_break", "days_to_next_break",
            "days_since_break", "tet_offset", "is_working_day"],

    # 2. Nhóm ĐẶC TRƯNG TƯƠNG QUAN CAO (Đã thanh lọc bằng Ma trận tương quan, loại bỏ biến rác và đa cộng tuyến)
    "load_high_corr": [
        "wx_temp_c",          # r = 0.5533 (Nhiệt độ - nhu cầu điều hòa)
        "wx_solar_rad_wm2",   # r = 0.4392 (Bức xạ mặt trời)
        "month_cos",          # r = -0.5285 (Chu kỳ mùa trong năm)
        "is_tet_break",       # r = -0.4447 (Kỳ nghỉ Tết sụt giảm tải)
        "is_public_holiday",  # r = -0.4325 (Ngày lễ quốc gia)
        "is_working_day",     # r = 0.3710 (Ngày làm việc - loại bỏ is_weekend đa cộng tuyến)
        "days_since_break",   # r = 0.3562 (Hiệu ứng phục hồi sau kỳ nghỉ)
    ],
    "hydro_high_corr": [
        "hy_powerhouse_total_m3s", # r = 0.8622 (Lưu lượng xả qua nhà máy)
        "hy_inflow_total_m3s",     # r = 0.8577, rho = 0.9156 (Tổng lưu lượng nước về hồ)
        "hy_inflow_da",            # r = 0.7767 (Lưu vực Sông Đà trọng điểm)
        "hy_inflow_lo_gam",        # r = 0.6375 (Lưu vực Sông Lô - Gâm)
        "hy_inflow_sesan",         # r = 0.5985 (Lưu vực Sông Sê San)
        "month_cos",               # r = -0.6401 (Chu kỳ mùa khô / mùa lũ)
        "month_sin",               # r = -0.5936 (Pha mùa vụ)
        "wx_humidity_pct",         # r = 0.6236 (Độ ẩm mùa mưa)
        "wx_precip_mm",            # r = 0.5565 -> 0.6066 ở trễ 7 ngày (Lượng mưa tích tụ)
        "hy_fill_ratio_mean",      # r = -0.4094 (Tỷ lệ tích nước hồ chứa)
    ]
}


def add_calendar(df: pd.DataFrame) -> pd.DataFrame:
    d = pd.to_datetime(df["date"])
    dow = d.dt.dayofweek  # 0 = Thu Hai
    df["dow_sin"] = np.sin(2 * np.pi * dow / 7)
    df["dow_cos"] = np.cos(2 * np.pi * dow / 7)
    df["month_sin"] = np.sin(2 * np.pi * (d.dt.month - 1) / 12)
    df["month_cos"] = np.cos(2 * np.pi * (d.dt.month - 1) / 12)
    df["is_weekend"] = (dow >= 5).astype(float)
    return df


# Dac trung BIET TRUOC cua cac ngay can du bao (lich + ngay le): dua vao mo hinh nhu "future covariates"
FUTURE_COLS = ["dow_sin", "dow_cos", "month_sin", "month_cos", "is_weekend", "is_public_holiday",
               "is_holiday_break", "is_tet_break", "days_to_next_break", "days_since_break",
               "tet_offset", "is_working_day"]


@dataclass
class Windows:
    X: np.ndarray        # (N, L, F) da chuan hoa toan cuc
    Y: np.ndarray        # (N, H)    muc tieu (kenh 0) da chuan hoa toan cuc
    dates: np.ndarray    # (N,)      ngay dau tien cua muc tieu
    split: np.ndarray    # (N,)      0 train / 1 val / 2 test
    F: np.ndarray = None  # (N, H, Ff) dac trung biet truoc cua cac ngay t+1..t+H (da chuan hoa), hoac None


@dataclass
class Prepared:
    win: Windows
    feature_names: list
    t_mean: float        # trung binh / do lech chuan cua target tren tap train (de doi nguoc don vi)
    t_std: float
    df: pd.DataFrame


def prepare(csv_path, target, features, lookback, horizon,
            train_end="2025-06-30", val_end="2025-12-31",
            start="2023-06-01", end=None, test_end=None) -> Prepared:
    df = pd.read_csv(csv_path)
    df = add_calendar(df)
    df["date"] = pd.to_datetime(df["date"])
    df = df[df.date >= pd.Timestamp(start)]
    if end:
        df = df[df.date <= pd.Timestamp(end)]
    df = df.sort_values("date").reset_index(drop=True)

    cols = [target]
    for g in features:
        if g not in FEATURE_GROUPS:
            raise ValueError(f"nhom dac trung khong hop le: {g}  (chon trong {list(FEATURE_GROUPS)})")
        cols += [c for c in FEATURE_GROUPS[g] if c not in cols]
    # Tranh ro ri: neu target la gen_*, khong dung nhom 'mix' lam dau vao cung ngay -> van hop le vi chi dung ngay <= t
    raw = df[cols].astype(float).copy()
    target_raw = raw[target].values.copy()          # giu NaN that cua target de loai mau
    raw = raw.ffill()

    # Thong ke chuan hoa chi tren tap train
    date = df["date"].values
    is_train_row = date <= np.datetime64(train_end)
    med = raw[is_train_row].median()
    raw = raw.fillna(med)
    mean = raw[is_train_row].mean()
    std = raw[is_train_row].std().replace(0, 1.0)
    Z = ((raw - mean) / std).values.astype(np.float32)
    tz = ((pd.Series(target_raw) - mean[target]) / std[target]).values.astype(np.float32)  # co NaN

    # dac trung biet truoc (lich + ngay le), chuan hoa theo tap train
    if any("high_corr" in g for g in features):
        fcols = [c for c in ["month_cos", "month_sin", "is_tet_break", "is_public_holiday", "is_working_day", "days_since_break"] if c in df.columns]
    else:
        fcols = [c for c in FUTURE_COLS if c in df.columns]
    fraw = df[fcols].astype(float).ffill().bfill()
    fz = ((fraw - fraw[is_train_row].mean()) / fraw[is_train_row].std().replace(0, 1.0)).values.astype(np.float32)

    T = len(df)
    Xs, Ys, Ds, Ss, Fs = [], [], [], [], []
    for t in range(lookback - 1, T - horizon):
        y = tz[t + 1: t + 1 + horizon]
        if np.isnan(y).any():
            continue
        d0 = date[t + 1]
        s = 0 if d0 <= np.datetime64(train_end) else (1 if d0 <= np.datetime64(val_end) else 2)
        if s == 2 and test_end is not None and d0 > np.datetime64(test_end):
            continue   # cat cuoi tap test (dung cho walk-forward)
        Xs.append(Z[t - lookback + 1: t + 1])
        Ys.append(y)
        Fs.append(fz[t + 1: t + 1 + horizon])
        Ds.append(d0)
        Ss.append(s)
    win = Windows(np.stack(Xs), np.stack(Ys).astype(np.float32), np.array(Ds), np.array(Ss), np.stack(Fs))
    return Prepared(win, cols, float(mean[target]), float(std[target]), df)
