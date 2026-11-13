"""Bang hop nhat THEO NGAY v2 (cap quoc gia).

Khac ban v1 (build_unified.py):
  - Phu tai lay tu hourly_v2 (truc gio deu, 24 o/ngay, o nua dem da noi suy) thay vi trung binh 23 gio.
  - SUA LOI LOAI NHAM: v1 loai 30 ngay co tong san luong < 600k MWh, nhung phan lon la NGAY LE/TET THAT
    (vd 8-14/02/2024, 27/01-02/02/2025, 17-20/02/2026). Ban v2 kiem tra tung nguon thay vi loai ca ngay:
      * import   > 300.000 MWh  -> dat NaN rieng cot import (lo nhap lieu), giu cac nguon khac
      * solar+other > 400.000 MWh -> dat NaN rieng cot solar_other
      * tong tho < 300.000 MWh (chi 31/05/2023, so lieu lam tron, ngay chua day du) -> loai ca ngay
      * ty le san luong/phu tai ngoai [0,90 ; 1,15] (4 ngay: 22/06/2025, 20/09/2025, 31/12/2025, 15/03/2026) -> loai ca ngay
  - Them lich ngay le, co phat hien gia tri thuy dien lap lai (stale), do tre cua gia tri duoi muc chet.
  - Khoang ngay = hop cua 3 bang (2023-01-01 .. 2026-10-15), co cac co has_* va all3.

Chay:  python build_daily_v2.py <thu_muc_3_csv_goc> hourly_v2.csv vn_holiday_calendar.csv unified_daily_v2.csv
"""
import sys
import numpy as np
import pandas as pd

src = sys.argv[1] if len(sys.argv) > 1 else "/mnt/user-data/uploads"
hourly_path = sys.argv[2] if len(sys.argv) > 2 else "hourly_v2.csv"
cal_path = sys.argv[3] if len(sys.argv) > 3 else "vn_holiday_calendar.csv"
out = sys.argv[4] if len(sys.argv) > 4 else "unified_daily_v2.csv"

IMPORT_MAX, SOLAR_OTHER_MAX, DAY_MIN_TOTAL = 300_000, 400_000, 300_000
RATIO_LO, RATIO_HI = 0.90, 1.15
SOURCES = ["biomass", "coal", "gas", "hydro", "import", "oil", "wind", "solar_other"]

# ------------------------------------------------------------------ 1. PHU TAI + THOI TIET (tu hourly_v2)
h = pd.read_csv(hourly_path, parse_dates=["ts", "date"], keep_default_na=False, na_values=[""])
nat = h[h.region == "national"]
g = nat.groupby("date")
d_load = pd.DataFrame({
    "load_mean_mw": g.load_mw.mean(), "load_peak_mw": g.load_mw.max(), "load_min_mw": g.load_mw.min(),
    "load_valid_hours": g.load_mw.count(), "load_imputed_hours": g.is_imputed.sum(),
})
bad_day = d_load.load_valid_hours < 20            # ngay thieu gan het gio (vd 12/09/2024)
d_load.loc[bad_day, ["load_mean_mw", "load_peak_mw", "load_min_mw"]] = np.nan
d_reg = (h[h.region != "national"].pivot_table(index="date", columns="region", values="load_mw", aggfunc="mean")
         .add_prefix("load_mean_mw_"))
wx = nat.groupby("date")[["temp_c", "humidity_pct", "cloud_pct", "solar_rad_wm2", "wind_ms"]].mean().add_prefix("wx_")
wx["wx_precip_mm"] = nat.groupby("date").precip_mm.mean() * 24          # tong luong mua uoc tinh trong ngay (mm/ngay)
wx.loc[nat.groupby("date").temp_c.count() < 20, :] = np.nan
wx = wx.rename(columns={"wx_temp_c": "wx_temp_c"})

# ------------------------------------------------------------------ 2. THUY DIEN
hyd = pd.read_csv(f"{src}/ml_hydro_inflow_daily.csv", parse_dates=["full_date"])
hyd["plant"] = hyd.plant_name.str.replace(r"\s*Đồng bộ lúc.*$", "", regex=True).str.strip().str.lower()
hyd = hyd.drop_duplicates(["plant", "full_date"], keep="last").sort_values(["plant", "full_date"])
hyd.loc[hyd.reservoir_inflow_m3_per_s > 20000, "reservoir_inflow_m3_per_s"] = np.nan
span = (hyd.normal_water_level_m - hyd.dead_water_level_m).replace(0, np.nan)
hyd["below_dead"] = (hyd.upstream_water_level_m < hyd.dead_water_level_m).astype(int)
hyd["fill"] = ((hyd.upstream_water_level_m - hyd.dead_water_level_m) / span).clip(0, 1.1)
prev = hyd.groupby("plant").reservoir_inflow_m3_per_s.shift(1)
gap = hyd.groupby("plant").full_date.diff().dt.days
hyd["stale"] = ((hyd.reservoir_inflow_m3_per_s == prev) & (prev > 0) & (gap == 1)).astype(float)
hyd.loc[~((prev > 0) & (gap == 1)), "stale"] = np.nan
basin = {"Sông Đà": "da", "Sông Mã - Cả": "ma_ca", "Sông Lô - Gâm": "lo_gam", "Sông Vu Gia - Thu Bồn": "vugia_thubon",
         "Sông Ba": "ba", "Sông Sê San": "sesan", "Sông Srêpôk": "srepok", "Sông Đồng Nai": "dongnai", "Lưu vực khác": "khac"}
hyd["basin"] = hyd.river_basin.map(basin)
d_h = hyd.groupby("full_date").agg(
    hy_inflow_total_m3s=("reservoir_inflow_m3_per_s", "sum"), hy_powerhouse_total_m3s=("powerhouse_discharge_m3_per_s", "sum"),
    hy_spill_total_m3s=("spillway_discharge_m3_per_s", "sum"), hy_fill_ratio_mean=("fill", "mean"),
    hy_n_plants=("plant", "nunique"), hy_stale_share=("stale", "mean"), hy_n_below_dead=("below_dead", "sum"))
d_hb = (hyd.pivot_table(index="full_date", columns="basin", values="reservoir_inflow_m3_per_s", aggfunc="sum")
        .add_prefix("hy_inflow_"))

# ------------------------------------------------------------------ 3. SAN LUONG THEO NGUON (sua loi loai nham)
mix = pd.read_csv(f"{src}/ml_generation_mix_daily.csv", parse_dates=["full_date"])
p = mix.pivot_table(index="full_date", columns="source_code", values="generation_mwh", aggfunc="sum")
p["solar_other"] = p[[c for c in ["solar", "other"] if c in p.columns]].sum(axis=1, min_count=1)
p = p[SOURCES].copy()
raw_total = p.sum(axis=1)
day_invalid = raw_total < DAY_MIN_TOTAL
fix_import = (p["import"] > IMPORT_MAX) & ~day_invalid
fix_so = (p["solar_other"] > SOLAR_OTHER_MAX) & ~day_invalid
p.loc[fix_import, "import"] = np.nan
p.loc[fix_so, "solar_other"] = np.nan
p.loc[day_invalid, :] = np.nan
p = p.add_prefix("gen_")
p["gen_total"] = p[[f"gen_{c}" for c in SOURCES]].sum(axis=1, min_count=len(SOURCES))
p["gen_thermal"] = p[["gen_coal", "gen_gas", "gen_oil"]].sum(axis=1, min_count=3)
p["gen_fix_import"] = fix_import.astype(int)
p["gen_fix_solar_other"] = fix_so.astype(int)
p["gen_day_invalid"] = day_invalid.astype(int)

# ------------------------------------------------------------------ 4. GHEP
cal = pd.read_csv(cal_path, parse_dates=["date"], keep_default_na=False, na_values=[]).set_index("date")
start = min(d_load.index.min(), d_h.index.min(), p.index.min())
end = max(d_load.index.max(), d_h.index.max(), p.index.max())
df = pd.DataFrame(index=pd.date_range(start, end, name="date"))
df = df.join(cal).join(d_load).join(d_reg).join(wx).join(d_h).join(d_hb).join(p)
df["day_of_week"] = df.index.day_of_week + 1
df["load_energy_est_mwh"] = df.load_mean_mw * 24
df["balance_ratio"] = df.gen_total / df.load_energy_est_mwh
# Kiem tra cheo san luong <-> phu tai: ty le gen_total / (phu tai x 24) tap trung trong 0,96-1,09 (p0.5-p99.5).
# Ngay lech xa (< 0,90 hoac > 1,15) la so lieu mix khong day du/trung lap -> loai ca ngay, danh dau gen_inconsistent.
incons = (df.balance_ratio < RATIO_LO) | (df.balance_ratio > RATIO_HI)
gen_cols = [c for c in df.columns if c.startswith("gen_") and c not in ("gen_fix_import", "gen_fix_solar_other", "gen_day_invalid")]
df.loc[incons, gen_cols] = np.nan
df["gen_inconsistent"] = incons.astype(int)
df["balance_ratio"] = df.gen_total / df.load_energy_est_mwh
df["has_load"] = df.load_mean_mw.notna().astype(int)
df["has_hydro"] = df.hy_inflow_total_m3s.notna().astype(int)
df["has_mix"] = df.gen_total.notna().astype(int)
df["all3"] = ((df.has_load + df.has_hydro + df.has_mix) == 3).astype(int)
df["year"], df["month"] = df.index.year, df.index.month
df = df.reset_index()
df["date"] = df.date.dt.strftime("%Y-%m-%d")
df = df[df.date <= "2026-10-15"]
df.round(4).to_csv(out, index=False)
print(df.shape, "->", out)
