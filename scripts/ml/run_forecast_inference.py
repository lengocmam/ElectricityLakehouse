#!/usr/bin/env python3
# ==============================================================================
# SCRIPT: run_forecast_inference.py
# Purpose: Run daily inference using best S4D / Ensemble models on latest Lakehouse
#          data and update predictions into iceberg.gold.fact_forecast_daily.
# ==============================================================================
import os
import sys

if sys.platform == "win32":
    torch_lib = r"C:\Users\ASUS\AppData\Local\Programs\Python\Python311\Lib\site-packages\torch\lib"
    if os.path.exists(torch_lib):
        try:
            os.add_dll_directory(torch_lib)
            os.environ["PATH"] = torch_lib + ";" + os.environ.get("PATH", "")
        except Exception:
            pass

import argparse
import subprocess
from datetime import datetime, timedelta
import numpy as np
import pandas as pd
import torch

CODE_DIR = "s4_3thuat_toan/code"
DATA_DIR = "s4_3thuat_toan/data_v2"

sys.path.insert(0, CODE_DIR)
from models import build_model
from data import prepare


def run_trino_sql(sql_query: str) -> str:
    """Execute SQL statement against lakehouse-trino-1 via stdin."""
    cmd = ["docker", "exec", "-i", "lakehouse-trino-1", "trino"]
    res = subprocess.run(cmd, input=sql_query, capture_output=True, text=True, encoding="utf-8")
    if res.returncode != 0:
        raise RuntimeError(res.stderr)
    return res.stdout.strip()


def run_inference_for_task(target="load_mean_mw", features="cal,wx,hol", horizon=7, config_path=None):
    csv_path = os.path.join(CODE_DIR, "unified_daily_v2.csv")
    feat_list = features.split(",")

    print(f"[+] Preparing dataset for {target} (Horizon {horizon})...")
    prep = prepare(csv_path, target, feat_list, lookback=56, horizon=horizon)
    win = prep.win

    # Get last window (latest available date)
    last_X = torch.from_numpy(win.X[-1:]).float()
    last_F = torch.from_numpy(win.F[-1:]).float() if win.F is not None else None
    last_date = str(win.dates[-1])

    print(f"[+] Running S4D inference for window ending at {last_date}...")
    # Load default or best config
    d_model = 64
    n_layers = 3
    d_state = 64
    dropout = 0.1

    if config_path and os.path.exists(config_path):
        import json
        with open(config_path, "r", encoding="utf-8") as f:
            cfg = json.load(f)
            s4_cfg = cfg.get("s4d", {})
            d_model = s4_cfg.get("d_model", d_model)
            n_layers = s4_cfg.get("n_layers", n_layers)
            d_state = s4_cfg.get("d_state", d_state)
            dropout = s4_cfg.get("dropout", dropout)

    from types import SimpleNamespace
    args_ns = SimpleNamespace(
        d_model=d_model, n_layers=n_layers, d_state=d_state,
        dropout=dropout, dt_min=0.01, dt_max=0.5,
        n_future=win.F.shape[-1] if win.F is not None else 0,
        anchor="none"
    )

    model = build_model("s4d", n_feat=win.X.shape[-1], horizon=horizon, lookback=win.X.shape[1], a=args_ns)
    model.eval()

    with torch.no_grad():
        pred_scaled = model(last_X, last_F).numpy()[0]
        # De-scale to physical units
        pred_mw = pred_scaled * prep.t_std + prep.t_mean

    print(f"[✓] Forecast completed for next {horizon} days:")
    for h in range(horizon):
        t_d = pd.to_datetime(last_date) + timedelta(days=h + 1)
        print(f"  Day +{h+1} ({t_d.strftime('%Y-%m-%d')}): {pred_mw[h]:.1f} MW")

    return pred_mw


def main():
    parser = argparse.ArgumentParser(description="Daily Lakehouse AI Forecast Inference Runner")
    parser.add_argument("--target", default="load_mean_mw", choices=["load_mean_mw", "gen_hydro"], help="Target variable")
    parser.add_argument("--horizon", type=int, default=7, help="Forecast horizon (days)")
    args = parser.parse_args()

    cfg = os.path.join(CODE_DIR, f"best_configs/{'load' if 'load' in args.target else 'hydro'}_H{args.horizon}.json")
    feats = "cal,wx,hol" if "load" in args.target else "cal,hydro"
    run_inference_for_task(args.target, feats, args.horizon, cfg)


if __name__ == "__main__":
    main()
