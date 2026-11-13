"""Bang phu tai THEO GIO v2 (dang dai: moi dong = 1 vung x 1 gio).

Sua cac loi cua bang goc:
  1. Moi ngay chi co 23 gio (nhan hour = 1..23, thieu o gio nua dem). Day la thieu 1 o thoi gian, nen cac cot lag/target
     goc (tinh bang dich dong) khong con dung 24h/168h. Ban v2 dung truc gio DEU (hour-ending):
       ts = full_date + hour gio  ->  o thieu luon la 00:00 (nua dem).
     O nua dem duoc noi suy tuyen tinh tu hai gio ke ben va danh dau is_imputed = 1.
  2. 8 dong load_mw = 0 (2 thoi diem, ca 4 vung) la loi nhan du lieu -> noi suy, danh dau.
  3. Chi noi suy khi o thieu la DON LE. Khoang thieu dai (vd ngay 12/09/2024) de NaN.
  4. lag / rolling / target tinh tren truc thoi gian that:  lag_24h = tai luc ts-24h,  target_lead_24h = tai luc ts+24h.
     rolling_24h = 24 gio TRUOC gio hien tai (khong gom gio hien tai).
  5. Bo cac cot daily_avg/peak/min (tinh tren ca ngay -> ro ri tuong lai neu dung lam dau vao).
  6. Thoi tiet cua dong `national` = trung binh co trong so (theo phu tai) cua 3 mien.
  7. Them lich ngay le Viet Nam (vn_holiday_calendar.csv).

Chay:  python build_hourly_v2.py <thu_muc_3_csv_goc> vn_holiday_calendar.csv hourly_v2.csv
"""
import sys
import numpy as np
import pandas as pd

src = sys.argv[1] if len(sys.argv) > 1 else "/mnt/user-data/uploads"
cal_path = sys.argv[2] if len(sys.argv) > 2 else "vn_holiday_calendar.csv"
out = sys.argv[3] if len(sys.argv) > 3 else "hourly_v2.csv"

WX = {"weather_temp_c": "temp_c", "weather_apparent_temp_c": "apparent_temp_c",
      "weather_humidity_pct": "humidity_pct", "weather_precipitation_mm": "precip_mm",
      "weather_cloud_cover_pct": "cloud_pct", "weather_solar_radiation_w_m2": "solar_rad_wm2",
      "weather_wind_speed_m_s": "wind_ms"}
REGIONS = ["north", "central", "south"]


def fill_single_gaps(s: pd.Series):
    """Noi suy tuyen tinh CHI cho cac o thieu don le (do dai chuoi NaN = 1). Tra ve (chuoi, mat na da dien)."""
    na = s.isna()
    run = (na != na.shift()).cumsum()
    single = na & (na.groupby(run).transform("size") == 1)
    mid = (s.shift(1) + s.shift(-1)) / 2
    filled = s.where(~single, mid)
    return filled, (single & filled.notna())


load = pd.read_csv(f"{src}/ml_load_forecasting_hourly.csv", parse_dates=["full_date"])
load["ts"] = load.full_date + pd.to_timedelta(load.hour, unit="h")
zero_keys = set(zip(load.loc[load.load_mw <= 0, "region_name"], load.loc[load.load_mw <= 0, "ts"]))
load.loc[load.load_mw <= 0, "load_mw"] = np.nan
grid = pd.date_range(load.ts.min(), load.ts.max(), freq="h")

frames = {}
for r in REGIONS + ["national"]:
    d = load[load.region_name == r].set_index("ts")[["load_mw"] + list(WX)].reindex(grid)
    d = d.rename(columns=WX)
    s, imp = fill_single_gaps(d["load_mw"])
    d["load_mw"] = s
    d["is_imputed"] = imp.astype(int)
    reason = np.where(~imp, "", np.where([(r, t) in zero_keys for t in d.index], "zero_value",
                                         np.where(d.index.hour == 0, "midnight_slot", "other_gap")))
    d["imputed_reason"] = reason
    for c in WX.values():
        d[c], _ = fill_single_gaps(d[c])
    frames[r] = d

# thoi tiet quoc gia = trung binh co trong so theo phu tai 3 mien (chi khi du ca 3 mien)
w = pd.concat({r: frames[r]["load_mw"] for r in REGIONS}, axis=1)
for c in WX.values():
    x = pd.concat({r: frames[r][c] for r in REGIONS}, axis=1)
    ok = x.notna().all(axis=1) & w.notna().all(axis=1)
    frames["national"][c] = ((x * w).sum(axis=1) / w.sum(axis=1)).where(ok)

cal = pd.read_csv(cal_path, parse_dates=["date"], keep_default_na=False, na_values=[]).set_index("date")
cal_cols = ["is_public_holiday", "holiday_group", "is_holiday_break", "is_tet_break", "break_day_index",
            "days_to_next_break", "days_since_break", "tet_offset", "is_makeup_workday", "is_working_day"]

parts = []
for r, d in frames.items():
    s = d["load_mw"]
    d = d.copy()
    d["load_lag_1h"] = s.shift(1)
    d["load_lag_2h"] = s.shift(2)
    d["load_lag_24h"] = s.shift(24)
    d["load_lag_48h"] = s.shift(48)
    d["load_lag_168h"] = s.shift(168)
    prev = s.shift(1).rolling(24, min_periods=24)
    d["load_rolling_mean_24h"] = prev.mean()
    d["load_rolling_std_24h"] = prev.std()
    d["load_rolling_min_24h"] = prev.min()
    d["load_rolling_max_24h"] = prev.max()
    d["load_target_lead_24h"] = s.shift(-24)
    d.insert(0, "region", r)
    d.index.name = "ts"
    parts.append(d.reset_index())
df = pd.concat(parts, ignore_index=True)

df["date"] = (df.ts - pd.Timedelta(hours=1)).dt.normalize()          # ngay theo nhan goc (hour-ending)
df["hour"] = df.ts.dt.hour                                           # 0..23
df["hour_ending"] = df.hour.replace(0, 24)                           # 1..24
df["hour_sin"] = np.sin(2 * np.pi * df.hour / 24)
df["hour_cos"] = np.cos(2 * np.pi * df.hour / 24)
dow = df.date.dt.day_of_week
df["day_of_week"] = dow + 1                                          # 1 = Thu Hai ... 7 = Chu nhat
df["dow_sin"] = np.sin(2 * np.pi * dow / 7)
df["dow_cos"] = np.cos(2 * np.pi * dow / 7)
df["month_sin"] = np.sin(2 * np.pi * (df.date.dt.month - 1) / 12)
df["month_cos"] = np.cos(2 * np.pi * (df.date.dt.month - 1) / 12)
df["is_weekend"] = (dow >= 5).astype(int)
df = df.join(cal[cal_cols], on="date")

order = (["ts", "date", "hour_ending", "hour", "region", "load_mw", "is_imputed", "imputed_reason"]
         + list(WX.values())
         + ["day_of_week", "is_weekend", "hour_sin", "hour_cos", "dow_sin", "dow_cos", "month_sin", "month_cos"]
         + cal_cols
         + ["load_lag_1h", "load_lag_2h", "load_lag_24h", "load_lag_48h", "load_lag_168h",
            "load_rolling_mean_24h", "load_rolling_std_24h", "load_rolling_min_24h", "load_rolling_max_24h",
            "load_target_lead_24h"])
df = df[order].sort_values(["region", "ts"])
df["ts"] = df.ts.dt.strftime("%Y-%m-%d %H:%M:%S")
df["date"] = df.date.dt.strftime("%Y-%m-%d")
df.round(4).to_csv(out, index=False)
print(df.shape, "->", out)
