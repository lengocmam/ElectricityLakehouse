"""Kiem toan bang v2: chay cac phep kiem tra tu dong va ghi audit_summary.json.

Tieu chi 'xong' cua Giai doan 1: moi kiem tra PASS.
Chay:  python audit_data.py <thu_muc_3_csv_goc> hourly_v2.csv unified_daily_v2.csv
"""
import json
import sys
import numpy as np
import pandas as pd

src = sys.argv[1] if len(sys.argv) > 1 else "/mnt/user-data/uploads"
hp = sys.argv[2] if len(sys.argv) > 2 else "hourly_v2.csv"
dp = sys.argv[3] if len(sys.argv) > 3 else "unified_daily_v2.csv"

h = pd.read_csv(hp, parse_dates=["ts", "date"], keep_default_na=False, na_values=[""])
d = pd.read_csv(dp, parse_dates=["date"], keep_default_na=False, na_values=[""])
raw = pd.read_csv(f"{src}/ml_load_forecasting_hourly.csv", parse_dates=["full_date"])

checks, summary = [], {}


def check(name, ok, detail=""):
    checks.append((name, bool(ok), detail))
    print(("PASS " if ok else "FAIL ") + name + (f"  [{detail}]" if detail else ""))


# ---------------------------------------------------------------- A. truc thoi gian gio
n = h[h.region == "national"].set_index("ts")
step = n.index.to_series().diff().dropna()
check("A1 truc gio deu 1h, khong trung", step.eq(pd.Timedelta("1h")).all() and not n.index.duplicated().any(),
      f"{len(n)} gio, {n.index.min()} .. {n.index.max()}")
check("A2 moi vung co cung so dong", h.groupby("region").size().nunique() == 1)
raw_n = raw[raw.region_name == "national"]
summary["raw_hours_per_day"] = int(raw_n.groupby("full_date").size().mode()[0])
summary["raw_missing_days"] = [str(x.date()) for x in pd.date_range(raw_n.full_date.min(), raw_n.full_date.max())
                               .difference(raw_n.full_date.unique())]

# ---------------------------------------------------------------- B. lag / target dung moc thoi gian that
for col, off in [("load_lag_1h", 1), ("load_lag_2h", 2), ("load_lag_24h", 24), ("load_lag_48h", 48),
                 ("load_lag_168h", 168), ("load_target_lead_24h", -24)]:
    true = n.load_mw.reindex(n.index - pd.Timedelta(hours=off))
    true.index = n.index
    ok = n[col].notna() & true.notna()
    rate = float(np.isclose(n.loc[ok, col], true[ok]).mean())
    check(f"B {col} khop moc thoi gian that", rate == 1.0, f"{rate:.4f} tren {int(ok.sum())} dong")
    summary[f"match_{col}"] = rate

# cac cot lag/target GOC co khop khong? (de bao cao muc do sai cua ban goc)
r = raw_n.copy()
r["ts"] = r.full_date + pd.to_timedelta(r.hour, unit="h")
r = r.set_index("ts").sort_index()
for col, off in [("load_lag_24h", 24), ("load_lag_168h", 168), ("load_target_lead_24h", -24)]:
    true = r.load_mw.reindex(r.index - pd.Timedelta(hours=off)); true.index = r.index
    ok = r[col].notna() & true.notna()
    summary[f"raw_match_{col}"] = float(np.isclose(r.loc[ok, col], true[ok]).mean())

# ---------------------------------------------------------------- C. noi suy
imp = h.groupby(["region", "imputed_reason"]).size().rename("n").reset_index()
imp = imp[imp.imputed_reason != ""]
summary["imputed"] = {f"{a}/{b}": int(c) for a, b, c in zip(imp.region, imp.imputed_reason, imp.n)}
check("C1 o noi suy chi la nua dem hoac gia tri 0", set(imp.imputed_reason) <= {"midnight_slot", "zero_value"})
check("C2 so o thieu con lai = 1 ngay thieu (25 o) moi vung", (h.groupby("region").load_mw.apply(lambda s: s.isna().sum()) == 25).all())
check("C3 khong con gia tri <= 0", (h.load_mw.dropna() > 0).all())
p = h.pivot_table(index="ts", columns="region", values="load_mw")
ratio = (p["national"] / p[["north", "central", "south"]].sum(axis=1)).dropna()
summary["national_vs_sum_regions"] = {"min": float(ratio.min()), "max": float(ratio.max())}
check("C4 national ~ tong 3 mien (>=0,98)", ratio.min() >= 0.98, f"min {ratio.min():.4f}")

# ---------------------------------------------------------------- D. ngay le
dd = d.set_index("date")
tet = dd[dd.is_tet_break == 1]
summary["tet_break_days"] = int(len(tet))
summary["load_mean_break_vs_other"] = {"break": float(dd[dd.is_holiday_break == 1].load_mean_mw.mean()),
                                       "other": float(dd[dd.is_holiday_break == 0].load_mean_mw.mean()),
                                       "tet": float(tet.load_mean_mw.mean())}
check("D1 co 4 ky nghi Tet (2023-2026)", dd[dd.is_tet_break == 1].index.year.nunique() == 4)
check("D2 ngay Tet thuc su thap hon ngay thuong", summary["load_mean_break_vs_other"]["tet"] < 0.7 * summary["load_mean_break_vs_other"]["other"],
      f"Tet {summary['load_mean_break_vs_other']['tet']:.0f} MW vs khac {summary['load_mean_break_vs_other']['other']:.0f} MW")

# ---------------------------------------------------------------- E. san luong theo nguon
ok = d.dropna(subset=["load_energy_est_mwh", "gen_total"])
summary["gen_vs_load"] = {"n": int(len(ok)), "corr": float(ok.load_energy_est_mwh.corr(ok.gen_total)),
                          "ratio_min": float(ok.balance_ratio.min()), "ratio_median": float(ok.balance_ratio.median()),
                          "ratio_max": float(ok.balance_ratio.max())}
check("E1 ty le san luong/phu tai trong [0,90 ; 1,15] sau khi loc", ok.balance_ratio.between(0.90, 1.15).all(),
      f"{ok.balance_ratio.min():.3f} .. {ok.balance_ratio.max():.3f}")
check("E2 tuong quan san luong-phu tai >= 0,98", summary["gen_vs_load"]["corr"] >= 0.98, f"{summary['gen_vs_load']['corr']:.4f}")
summary["gen_flags"] = {"day_invalid": int(d.gen_day_invalid.sum()), "fix_import": int(d.gen_fix_import.sum()),
                        "fix_solar_other": int(d.gen_fix_solar_other.sum()), "inconsistent": int(d.gen_inconsistent.sum()),
                        "inconsistent_days": [str(x.date()) for x in d[d.gen_inconsistent == 1].date]}
holiday_kept = d[(d.is_holiday_break == 1) & d.gen_total.notna()]
check("E3 cac ngay le/Tet thuc khong bi loai nham", len(holiday_kept) >= 60, f"{len(holiday_kept)} ngay trong ky nghi con so lieu")
summary["holiday_days_with_mix"] = int(len(holiday_kept))

# ---------------------------------------------------------------- F. thuy dien
summary["hydro"] = {"stale_share_mean": float(d.hy_stale_share.mean()), "days": int(d.has_hydro.sum()),
                    "plants_median": float(d.hy_n_plants.median()), "below_dead_rows": int(d.hy_n_below_dead.sum())}
check("F1 co 39 nha may o hau het cac ngay", d.hy_n_plants.max() <= 39 and d.hy_n_plants.median() >= 35,
      f"median {d.hy_n_plants.median():.0f}, max {d.hy_n_plants.max():.0f}")

# ---------------------------------------------------------------- G. bang ngay
summary["daily"] = {"rows": int(len(d)), "range": [str(d.date.min().date()), str(d.date.max().date())],
                    "has_load": int(d.has_load.sum()), "has_hydro": int(d.has_hydro.sum()),
                    "has_mix": int(d.has_mix.sum()), "all3": int(d.all3.sum())}
check("G1 khong co ngay trung", not d.date.duplicated().any())
check("G2 load_valid_hours <= 24", d.load_valid_hours.max() <= 24)

failed = [c for c in checks if not c[1]]
summary["checks"] = [{"name": n_, "pass": ok_, "detail": det} for n_, ok_, det in checks]
with open("audit_summary.json", "w") as f:
    json.dump(summary, f, indent=2, ensure_ascii=False)
print(f"\n{len(checks) - len(failed)}/{len(checks)} kiem tra PASS")
sys.exit(1 if failed else 0)
