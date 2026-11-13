#!/usr/bin/env python3
# ==============================================================================
# SCRIPT: ingest_forecasts_to_lakehouse.py
# Purpose: Extract ML/DL model predictions (S4D, TimeXer, S-Mamba, LightGBM, Ensemble)
#          from s4_3thuat_toan results and ingest into Iceberg Gold tables in Trino.
# ==============================================================================
import os
import sys
import glob
import json
import subprocess
from datetime import datetime, timedelta

if sys.stdout.encoding != 'utf-8':
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except Exception:
        pass
import numpy as np
import pandas as pd
from scipy import stats

CODE_DIR = "s4_3thuat_toan/code"
DATA_DIR = "s4_3thuat_toan/data_v2"


def run_trino_sql(sql_query: str) -> str:
    """Execute SQL statement against lakehouse-trino-1 container via Trino CLI using stdin."""
    cmd = [
        "docker", "exec", "-i", "lakehouse-trino-1",
        "trino"
    ]
    res = subprocess.run(cmd, input=sql_query, capture_output=True, text=True, encoding="utf-8")
    if res.returncode != 0:
        print(f"[!] Trino SQL Error: {res.stderr}")
        raise RuntimeError(res.stderr)
    return res.stdout.strip()


def calculate_dm(p1, p2, y, H):
    """Diebold-Mariano test between predictions p1 and p2 against ground truth y."""
    d = (np.abs(p1 - y) - np.abs(p2 - y)).mean(axis=1)
    n = len(d)
    db = d.mean()
    def gamma(k):
        return np.mean((d[k:] - db) * (d[:n - k] - db))
    var = max(gamma(0) + 2 * sum(gamma(k) for k in range(1, H)), 1e-12)
    stat = db / np.sqrt(var / n) * np.sqrt((n + 1 - 2 * H + H * (H - 1) / n) / n)
    pval = 2 * (1 - stats.t.cdf(abs(stat), df=n - 1))
    return float(stat), float(pval)


def main():
    print("=" * 70)
    print("INGESTING S4D & ML BENCHMARK FORECASTS INTO LAKEHOUSE (ICEBERG GOLD)")
    print("=" * 70)

    # 1. Load Holidays
    hols = set()
    hol_file = os.path.join(DATA_DIR, "vn_holiday_calendar.csv")
    if os.path.exists(hol_file):
        df_hol = pd.read_csv(hol_file)
        hols = set(df_hol[df_hol["is_holiday_break"] == 1]["date"].tolist())
        print(f"[✓] Loaded {len(hols)} holiday dates from {hol_file}")

    # 2. Extract 7-day Load and Hydro NPZs
    f_load_t = os.path.join(CODE_DIR, "results/timexer/load_H7/stage3_preds_load_mean_mw_cal+wx+hol_H7_fut.npz")
    f_load_s = os.path.join(CODE_DIR, "results/smamba/load_H7/stage3_preds_load_mean_mw_cal+wx+hol_H7_fut.npz")
    f_hydro_t = os.path.join(CODE_DIR, "results/timexer/hydro_H7/stage3_preds_gen_hydro_cal+hydro_H7_fut.npz")
    f_hydro_s = os.path.join(CODE_DIR, "results/smamba/hydro_H7/stage3_preds_gen_hydro_cal+hydro_H7_fut.npz")

    zt_load, zs_load = np.load(f_load_t), np.load(f_load_s)
    zt_hydro, zs_hydro = np.load(f_hydro_t), np.load(f_hydro_s)

    # 3. Build Detailed Forecast Records (fact_forecast_daily)
    records = []
    tasks = [
        ("load_mean_mw", zt_load, zs_load),
        ("gen_hydro", zt_hydro, zs_hydro),
    ]

    for task_name, zt, zs in tasks:
        dates = zt["dates"]
        true = zt["true"]
        lgb = zt["LightGBM_to_hop"]
        s4d = zt["S4D_to_hop"]
        timexer = zt["TimeXer_to_hop"]
        smamba = zs["S_Mamba_to_hop"]
        pers = zt["persistence"]
        snaive = zt["seasonal_naive_7"]

        for i, d_str in enumerate(dates):
            f_date = pd.to_datetime(d_str)
            f_key = int(f_date.strftime("%Y%m%d"))
            for h in range(7):
                t_date = f_date + timedelta(days=h + 1)
                t_str = t_date.strftime("%Y-%m-%d")
                t_key = int(t_date.strftime("%Y%m%d"))

                y = float(true[i, h])
                p_s = float(s4d[i, h])
                p_t = float(timexer[i, h])
                p_m = float(smamba[i, h])
                p_l = float(lgb[i, h])
                p_e = round((p_s + p_t + p_m) / 3.0, 1)
                p_p = float(pers[i, h])
                p_n = float(snaive[i, h])

                is_hol = t_str in hols
                records.append((
                    d_str, f_key, task_name, h + 1, t_str, t_key,
                    round(y, 1), round(p_s, 1), round(p_t, 1), round(p_m, 1),
                    round(p_l, 1), p_e, round(p_p, 1), round(p_n, 1),
                    round(abs(y - p_s), 1), round(abs(y - p_e), 1), round(abs(y - p_l), 1),
                    is_hol
                ))

    print(f"[✓] Prepared {len(records)} detailed daily forecast rows")

    # 4. Re-create fact_forecast_daily in Trino
    print("\n[+] Creating / Refreshing iceberg.gold.fact_forecast_daily in Trino...")
    run_trino_sql("DROP TABLE IF EXISTS iceberg.gold.fact_forecast_daily")
    ddl_forecast = """
    CREATE TABLE iceberg.gold.fact_forecast_daily (
        forecast_date VARCHAR,
        forecast_date_key INTEGER,
        target_name VARCHAR,
        horizon_step INTEGER,
        target_date VARCHAR,
        target_date_key INTEGER,
        actual_value DOUBLE,
        s4d_pred DOUBLE,
        timexer_pred DOUBLE,
        smamba_pred DOUBLE,
        lightgbm_pred DOUBLE,
        ensemble_paper_pred DOUBLE,
        persistence_pred DOUBLE,
        seasonal_naive_pred DOUBLE,
        error_s4d DOUBLE,
        error_ensemble DOUBLE,
        error_lightgbm DOUBLE,
        is_holiday_break BOOLEAN
    ) WITH (
        format = 'PARQUET'
    )
    """
    run_trino_sql(ddl_forecast)

    # Ingest in batches of 250 rows
    batch_size = 250
    total = len(records)
    print(f"[+] Ingesting {total} rows into fact_forecast_daily in batches of {batch_size}...")
    for idx in range(0, total, batch_size):
        batch = records[idx:idx + batch_size]
        val_strs = []
        for r in batch:
            hol_str = "true" if r[17] else "false"
            val_strs.append(
                f"('{r[0]}', {r[1]}, '{r[2]}', {r[3]}, '{r[4]}', {r[5]}, {r[6]}, "
                f"{r[7]}, {r[8]}, {r[9]}, {r[10]}, {r[11]}, {r[12]}, {r[13]}, "
                f"{r[14]}, {r[15]}, {r[16]}, {hol_str})"
            )
        insert_sql = f"INSERT INTO iceberg.gold.fact_forecast_daily VALUES\n" + ",\n".join(val_strs)
        run_trino_sql(insert_sql)
        print(f"  [✓] Inserted {min(idx + batch_size, total)} / {total} rows")

    # 5. Build and Ingest Benchmark Metrics (fact_forecast_benchmark)
    print("\n[+] Creating / Refreshing iceberg.gold.fact_forecast_benchmark in Trino...")
    run_trino_sql("DROP TABLE IF EXISTS iceberg.gold.fact_forecast_benchmark")
    ddl_benchmark = """
    CREATE TABLE iceberg.gold.fact_forecast_benchmark (
        task_code VARCHAR,
        task_name VARCHAR,
        unit VARCHAR,
        model_name VARCHAR,
        model_type VARCHAR,
        mae DOUBLE,
        rmse DOUBLE,
        mape_pct DOUBLE,
        wape_pct DOUBLE,
        accuracy_pct DOUBLE,
        r2_score DOUBLE,
        dm_stat_vs_lgb DOUBLE,
        dm_p_value_vs_lgb DOUBLE,
        rank_order INTEGER
    ) WITH (
        format = 'PARQUET'
    )
    """
    run_trino_sql(ddl_benchmark)

    # Benchmark stats for all 4 tasks
    benchmark_tasks = [
        ("load_H1", "Phụ tải điện 1 ngày", "MW", zt_load["true"][:, :1], {
            "LightGBM": zt_load["LightGBM_to_hop"][:, :1],
            "S4D": zt_load["S4D_to_hop"][:, :1],
            "TimeXer": zt_load["TimeXer_to_hop"][:, :1],
            "S-Mamba": zs_load["S_Mamba_to_hop"][:, :1],
            "Ensemble 3 bài báo": (zt_load["S4D_to_hop"][:, :1] + zt_load["TimeXer_to_hop"][:, :1] + zs_load["S_Mamba_to_hop"][:, :1]) / 3.0,
            "Persistence": zt_load["persistence"][:, :1],
            "Seasonal Naive": zt_load["seasonal_naive_7"][:, :1]
        }),
        ("load_H7", "Phụ tải điện 7 ngày", "MW", zt_load["true"], {
            "LightGBM": zt_load["LightGBM_to_hop"],
            "S4D": zt_load["S4D_to_hop"],
            "TimeXer": zt_load["TimeXer_to_hop"],
            "S-Mamba": zs_load["S_Mamba_to_hop"],
            "Ensemble 3 bài báo": (zt_load["S4D_to_hop"] + zt_load["TimeXer_to_hop"] + zs_load["S_Mamba_to_hop"]) / 3.0,
            "Persistence": zt_load["persistence"],
            "Seasonal Naive": zt_load["seasonal_naive_7"]
        }),
        ("hydro_H1", "Sản lượng thủy điện 1 ngày", "MWh", zt_hydro["true"][:, :1], {
            "LightGBM": zt_hydro["LightGBM_to_hop"][:, :1],
            "S4D": zt_hydro["S4D_to_hop"][:, :1],
            "TimeXer": zt_hydro["TimeXer_to_hop"][:, :1],
            "S-Mamba": zs_hydro["S_Mamba_to_hop"][:, :1],
            "Ensemble 3 bài báo": (zt_hydro["S4D_to_hop"][:, :1] + zt_hydro["TimeXer_to_hop"][:, :1] + zs_hydro["S_Mamba_to_hop"][:, :1]) / 3.0,
            "Persistence": zt_hydro["persistence"][:, :1],
            "Seasonal Naive": zt_hydro["seasonal_naive_7"][:, :1]
        }),
        ("hydro_H7", "Sản lượng thủy điện 7 ngày", "MWh", zt_hydro["true"], {
            "LightGBM": zt_hydro["LightGBM_to_hop"],
            "S4D": zt_hydro["S4D_to_hop"],
            "TimeXer": zt_hydro["TimeXer_to_hop"],
            "S-Mamba": zs_hydro["S_Mamba_to_hop"],
            "Ensemble 3 bài báo": (zt_hydro["S4D_to_hop"] + zt_hydro["TimeXer_to_hop"] + zs_hydro["S_Mamba_to_hop"]) / 3.0,
            "Persistence": zt_hydro["persistence"],
            "Seasonal Naive": zt_hydro["seasonal_naive_7"]
        })
    ]

    bench_rows = []
    for t_code, t_name, unit, y, pred_map in benchmark_tasks:
        H = y.shape[1]
        lgb_p = pred_map["LightGBM"]
        
        # Calculate metric for each model
        model_scores = []
        for m_name, p in pred_map.items():
            diff = p - y
            mae = float(np.abs(diff).mean())
            rmse = float(np.sqrt((diff ** 2).mean()))
            denom = np.maximum(np.abs(y), 1e-9)
            mape = float((np.abs(diff) / denom).mean() * 100.0)
            wape = float(np.abs(diff).sum() / denom.sum() * 100.0)
            acc = round(max(0.0, 100.0 - wape), 2)
            
            # R2 score
            ss_tot = float(((y - y.mean()) ** 2).sum())
            ss_res = float((diff ** 2).sum())
            r2 = round(max(-1.0, 1.0 - ss_res / ss_tot), 4)

            # Diebold Mariano test vs LightGBM
            dm_stat, dm_p = calculate_dm(p, lgb_p, y, H) if m_name != "LightGBM" else (0.0, 1.0)
            
            m_type = "Baseline" if "Naive" in m_name or "Persistence" in m_name or "LightGBM" in m_name else ("Ensemble" if "Ensemble" in m_name else "Deep Learning")
            model_scores.append((m_name, m_type, round(mae, 1), round(rmse, 1), round(mape, 2), round(wape, 2), acc, r2, round(dm_stat, 2), round(dm_p, 4)))

        # Sort by MAE ascending to determine rank
        model_scores.sort(key=lambda x: x[2])
        for rank, item in enumerate(model_scores, 1):
            bench_rows.append((t_code, t_name, unit, item[0], item[1], item[2], item[3], item[4], item[5], item[6], item[7], item[8], item[9], rank))

    print(f"[+] Ingesting {len(bench_rows)} benchmark rows into fact_forecast_benchmark...")
    b_val_strs = [
        f"('{r[0]}', '{r[1]}', '{r[2]}', '{r[3]}', '{r[4]}', {r[5]}, {r[6]}, {r[7]}, {r[8]}, {r[9]}, {r[10]}, {r[11]}, {r[12]}, {r[13]})"
        for r in bench_rows
    ]
    run_trino_sql(f"INSERT INTO iceberg.gold.fact_forecast_benchmark VALUES\n" + ",\n".join(b_val_strs))
    print("  [✓] Inserted all benchmark metrics!")

    # 6. Ingest Horizon-by-Horizon Breakdown (fact_forecast_horizon_mae)
    print("\n[+] Creating / Refreshing iceberg.gold.fact_forecast_horizon_mae in Trino...")
    run_trino_sql("DROP TABLE IF EXISTS iceberg.gold.fact_forecast_horizon_mae")
    ddl_horizon = """
    CREATE TABLE iceberg.gold.fact_forecast_horizon_mae (
        task_code VARCHAR,
        horizon_step INTEGER,
        horizon_name VARCHAR,
        lightgbm_mae DOUBLE,
        s4d_mae DOUBLE,
        timexer_mae DOUBLE,
        smamba_mae DOUBLE,
        ensemble_mae DOUBLE
    ) WITH (
        format = 'PARQUET'
    )
    """
    run_trino_sql(ddl_horizon)

    h_rows = []
    for t_code, zt, zs in [("load_H7", zt_load, zs_load), ("hydro_H7", zt_hydro, zs_hydro)]:
        y = zt["true"]
        lgb = zt["LightGBM_to_hop"]
        s4d = zt["S4D_to_hop"]
        timexer = zt["TimeXer_to_hop"]
        smamba = zs["S_Mamba_to_hop"]
        ens = (s4d + timexer + smamba) / 3.0

        for h in range(7):
            step = h + 1
            h_name = f"Ngày +{step}"
            mae_l = round(float(np.abs(lgb[:, h] - y[:, h]).mean()), 1)
            mae_s = round(float(np.abs(s4d[:, h] - y[:, h]).mean()), 1)
            mae_t = round(float(np.abs(timexer[:, h] - y[:, h]).mean()), 1)
            mae_m = round(float(np.abs(smamba[:, h] - y[:, h]).mean()), 1)
            mae_e = round(float(np.abs(ens[:, h] - y[:, h]).mean()), 1)
            h_rows.append(f"('{t_code}', {step}, '{h_name}', {mae_l}, {mae_s}, {mae_t}, {mae_m}, {mae_e})")

    run_trino_sql(f"INSERT INTO iceberg.gold.fact_forecast_horizon_mae VALUES\n" + ",\n".join(h_rows))
    print("  [✓] Inserted all horizon MAE records!")

    print("\n" + "=" * 70)
    print("INGESTION COMPLETE: Lakehouse now stores full ML predictions & benchmarks!")
    print("=" * 70)


if __name__ == "__main__":
    main()
