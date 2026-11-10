"""Giai doan 2 (hoan thien): tinh chinh CONG BANG + danh gia cuoi.

Moi mo hinh (S4D, DLinear, PatchTST, LightGBM, ridge) duoc tim kiem ngau nhien voi CUNG so lan thu (--trials,
lan thu dau luon la cau hinh mac dinh), chon theo MAE tren tap VALIDATION. Sau do chay lai cau hinh tot nhat voi
nhieu seed MOI (100..) va do tren tap TEST. Co kiem dinh Diebold-Mariano va du bao to hop.

Vi du:
  python stage3.py --target load_mean_mw --features cal,wx,hol --trials 6
  python stage3.py --target load_mean_mw --features cal,wx,hol --horizon 7 --load_config results/stage3_load_mean_mw_cal+wx+hol_H1.json
"""
import argparse, json, os, random, re, time
from types import SimpleNamespace
import numpy as np
import pandas as pd
import torch

from data import prepare
from baselines import persistence, seasonal_naive, ridge_fit_predict, lightgbm_fit_predict
from train2 import run_nn, metrics, dm_test, mase_scale

SPACE = {
    "s4d": dict(lookback=[28, 56, 112, 224], d_model=[32, 64, 128], n_layers=[2, 3, 4], d_state=[32, 64],
                dropout=[0.0, 0.1, 0.2, 0.3], lr=[5e-4, 1e-3, 2e-3, 5e-3], wd=[0.01, 0.05, 0.1],
                dt_min=[0.001, 0.01], dt_max=[0.1, 0.5, 1.0]),
    "dlinear": dict(lookback=[28, 56, 112, 224], lr=[1e-3, 5e-3, 1e-2], wd=[0.0, 0.05], kernel=[13, 25]),
    "patchtst": dict(lookback=[56, 112, 224], d_model=[32, 64, 128], ptst_layers=[1, 2, 3], dropout=[0.1, 0.2, 0.3],
                     lr=[5e-4, 1e-3, 2e-3], wd=[0.01, 0.05], patch=[(8, 4), (16, 8)]),
    "smamba": dict(lookback=[28, 56, 112, 224], d_model=[32, 64, 128], n_layers=[1, 2, 3], d_state=[8, 16],
                   dropout=[0.0, 0.1, 0.2, 0.3], lr=[5e-4, 1e-3, 2e-3, 5e-3], wd=[0.01, 0.05, 0.1]),
    "timexer": dict(lookback=[28, 56, 112, 224], d_model=[32, 64, 128], ptst_layers=[1, 2, 3], dropout=[0.0, 0.1, 0.2, 0.3],
                    lr=[5e-4, 1e-3, 2e-3, 5e-3], wd=[0.01, 0.05, 0.1], patch_len=[7, 14]),
    "lightgbm": dict(lags=[7, 14, 28], num_leaves=[7, 15, 31], learning_rate=[0.02, 0.05],
                     min_data_in_leaf=[10, 20, 40], feature_fraction=[0.5, 0.8, 1.0]),
    "ridge": dict(lags=[7, 14, 28]),
}
DEFAULT = {
    "s4d": dict(lookback=56, d_model=64, n_layers=3, d_state=64, dropout=0.1, lr=2e-3, wd=0.05, dt_min=0.01, dt_max=0.5),
    "dlinear": dict(lookback=56, lr=2e-3, wd=0.05, kernel=25),
    "patchtst": dict(lookback=56, d_model=64, ptst_layers=2, dropout=0.1, lr=2e-3, wd=0.05, patch=(8, 4)),
    "smamba": dict(lookback=56, d_model=64, n_layers=2, d_state=16, dropout=0.1, lr=2e-3, wd=0.05),
    "timexer": dict(lookback=56, d_model=64, ptst_layers=2, dropout=0.1, lr=2e-3, wd=0.05, patch_len=7),
    "lightgbm": dict(lags=14, num_leaves=15, learning_rate=0.03, min_data_in_leaf=15, feature_fraction=0.8),
    "ridge": dict(lags=14),
}
LABEL = {"s4d": "S4D", "dlinear": "DLinear", "patchtst": "PatchTST", "timexer": "TimeXer", "smamba": "S-Mamba", "lightgbm": "LightGBM", "ridge": "Ridge"}


def sample(model, rng, with_anchor=False):
    c = {k: rng.choice(v) for k, v in SPACE[model].items()}
    if with_anchor and model in ("s4d", "timexer", "smamba"):
        c["anchor"] = rng.choice(["none", "last", "week"])
    return c


def tupleize(cfg):
    return {k: (tuple(v) if isinstance(v, list) else v) for k, v in cfg.items()}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", default="unified_daily_v2.csv")
    ap.add_argument("--target", default="load_mean_mw")
    ap.add_argument("--features", default="cal,wx,hol")
    ap.add_argument("--horizon", type=int, default=1)
    ap.add_argument("--models", default="lightgbm,timexer,smamba,s4d")
    ap.add_argument("--trials", type=int, default=6)
    ap.add_argument("--n_seeds", type=int, default=5)
    ap.add_argument("--epochs", type=int, default=60)
    ap.add_argument("--patience", type=int, default=10)
    ap.add_argument("--batch", type=int, default=64)
    ap.add_argument("--train_end", default="2025-06-30")
    ap.add_argument("--val_end", default="2025-12-31")
    ap.add_argument("--test_end", default="", help="cat cuoi tap test (walk-forward)")
    ap.add_argument("--future", type=int, default=1, help="1 = dua lich/ngay le cua cac ngay can du bao vao mo hinh (S4D, ridge, LightGBM)")
    ap.add_argument("--anchor", default="", choices=["", "none", "last", "week"], help="ep tat ca mo hinh mang hoc phan lech so voi moc neo (rong = theo cau hinh)")
    ap.add_argument("--search_anchor", type=int, default=0, help="1 = them anchor vao khong gian tim kiem cua cac mo hinh mang")
    ap.add_argument("--load_config", default="", help="dung lai cau hinh tot nhat tu file JSON, khong tinh chinh lai")
    ap.add_argument("--out", default="results")
    args = ap.parse_args()

    feats = [f for f in args.features.split(",") if f]
    models = [m for m in args.models.split(",") if m]
    H, device = args.horizon, ("cuda" if torch.cuda.is_available() else "cpu")
    base = dict(horizon=H, epochs=args.epochs, patience=args.patience, batch=args.batch,
                d_model=64, n_layers=3, d_state=64, dropout=0.1, dt_min=0.01, dt_max=0.5, lr=2e-3, wd=0.05)
    cache = {}

    def get_prep(lb):
        if lb not in cache:
            cache[lb] = prepare(args.csv, args.target, feats, lb, H, args.train_end, args.val_end,
                                test_end=args.test_end or None)
        return cache[lb]

    def nn_cfg(model, cfg, lb=None):
        d = {**base, **cfg}
        d["future"] = args.future
        if args.anchor:
            d["anchor"] = args.anchor
        if "patch" in d:
            d["patch_len"], d["stride"] = d["patch"]
        d["lookback"] = lb or d["lookback"]
        return SimpleNamespace(**d)

    def run_cfg(model, cfg, seed):
        """Tra ve (MAE validation, du bao test z-space, ngay test, tham so)."""
        if model in ("s4d", "dlinear", "patchtst", "timexer", "smamba"):
            prep = get_prep(cfg["lookback"])
            pte, v, ep, npar = run_nn(prep, nn_cfg(model, cfg), model, seed, device)
            return v, pte, prep.win.dates[prep.win.split == 2]
        prep = get_prep(56)
        W = prep.win
        if model == "ridge":
            pred, _ = ridge_fit_predict(W.X, W.Y, W.split, H, lags=cfg["lags"], Fut=W.F if args.future else None)
        else:
            over = {k: v for k, v in cfg.items() if k != "lags"}
            pred = lightgbm_fit_predict(W.X, W.Y, W.split, H, lags=cfg["lags"], seed=seed, over=over,
                                        Fut=W.F if args.future else None)
        v = float(np.abs(pred[W.split == 1] - W.Y[W.split == 1]).mean() * prep.t_std)
        return v, pred[W.split == 2], W.dates[W.split == 2]

    # ---------------- 1. tinh chinh
    t0 = time.time()
    tuning, best = {}, {}
    rng = random.Random(0)
    if args.load_config:
        saved = json.load(open(args.load_config))
        best = {m: tupleize(c) for m, c in saved["best_config"].items() if m in models}
        print("dung lai cau hinh tot nhat tu", args.load_config, "->", sorted(best))
    for m in models:
        if m in best:
            continue                     # da co cau hinh (nap tu file), chi tinh chinh mo hinh con thieu
        trials = [DEFAULT[m]] + [sample(m, rng, bool(args.search_anchor)) for _ in range(args.trials - 1)]
        log = []
        os.makedirs(args.out, exist_ok=True)
        prog_path = os.path.join(args.out, "tuning_progress.json")
        prog = json.load(open(prog_path)) if os.path.exists(prog_path) else {}
        for i, cfg in enumerate(trials):
            cfg_json = {k: (list(x) if isinstance(x, tuple) else x) for k, x in cfg.items()}
            key = f"{m}:{i}"
            if key in prog and prog[key]["cfg"] == cfg_json:          # nap lai lan thu da xong (neu tien trinh tung bi ngat)
                v = prog[key]["val_MAE"]
            else:
                v, *_ = run_cfg(m, cfg, 0)
                prog[key] = {"cfg": cfg_json, "val_MAE": float(v)}
                json.dump(prog, open(prog_path, "w"))
            log.append({"cfg": cfg_json, "val_MAE": float(v)})
            print(f"  [{LABEL[m]}] thu {i + 1}/{len(trials)}: val MAE={v:.1f}  {cfg}", flush=True)
        tuning[m] = log
        j = int(np.argmin([x["val_MAE"] for x in log]))
        best[m] = tupleize(trials[j])
        print(f"  -> {LABEL[m]} tot nhat: lan thu {j + 1}, val MAE={log[j]['val_MAE']:.1f} (mac dinh: {log[0]['val_MAE']:.1f})", flush=True)

    os.makedirs(args.out, exist_ok=True)
    json.dump({"best_config": {m: {k: (list(x) if isinstance(x, tuple) else x) for k, x in c.items()} for m, c in best.items()}},
              open(os.path.join(args.out, "best_config_tuned.json"), "w"), indent=1)

    # ---------------- 2. danh gia cuoi tren TEST
    ref = get_prep(56)
    W = ref.win
    te_dates = pd.to_datetime(W.dates[W.split == 2])
    Y_ref = pd.DataFrame(W.Y[W.split == 2], index=te_dates)
    scale = mase_scale(ref, args.target, args.train_end)
    P = {"persistence": pd.DataFrame(persistence(W.X[W.split == 2], H), index=te_dates),
         "seasonal_naive_7": pd.DataFrame(seasonal_naive(W.X[W.split == 2], H), index=te_dates)}
    per_seed = {}
    for m in models:
        runs = []
        n_run = 1 if m == "ridge" else args.n_seeds
        for s in range(n_run):
            _, pte, d = run_cfg(m, best[m], 100 + s)
            runs.append(pd.DataFrame(pte, index=pd.to_datetime(d)))
            print(f"  final {LABEL[m]} seed {100 + s}: test MAE={np.abs(runs[-1].reindex(te_dates).values - Y_ref.values).mean() * ref.t_std:.1f}", flush=True)
        per_seed[m] = runs
        P[f"{LABEL[m]} (to hop)"] = sum(runs) / len(runs)

    common = te_dates
    for k, v in P.items():
        common = common.intersection(v.index)
    Yc = Y_ref.loc[common].values
    rows, preds = {}, {}
    for k, v in P.items():
        preds[k] = v.loc[common].values
        rows[k] = metrics(preds[k], Yc, ref, scale)
    for m in models:
        if len(per_seed[m]) > 1:
            ms = [metrics(r.loc[common].values, Yc, ref, scale) for r in per_seed[m]]
            agg = {k: float(np.mean([x[k] for x in ms])) for k in ms[0]}
            agg["MAE_std"] = float(np.std([x["MAE"] for x in ms]))
            rows[f"{LABEL[m]} (TB {len(ms)} seed)"] = agg

    budget = (f"{args.trials} lan thu cho mo hinh moi, cac mo hinh con lai dung cau hinh tu file" if (args.load_config and tuning) else "cau hinh dung lai tu file") if args.load_config else f"{args.trials} lan thu/mo hinh"
    print(f"\nKET QUA TEST sau tinh chinh cong bang ({len(common)} cua so, horizon {H}, {budget})")
    print(f"{'mo hinh':<24}{'MAE':>11}{'RMSE':>11}{'MAPE%':>8}{'MASE':>7}")
    for k, v in rows.items():
        extra = f"  (±{v['MAE_std']:.1f})" if "MAE_std" in v else ""
        print(f"{k:<24}{v['MAE']:>11.1f}{v['RMSE']:>11.1f}{v['MAPE%']:>8.2f}{v['MASE']:>7.3f}{extra}")

    cand = [k for k in rows if "(to hop)" in k or k in ("persistence", "seasonal_naive_7")]
    best_k = min(cand, key=lambda k: rows[k]["MAE"])
    print(f"\nMo hinh tot nhat theo MAE: {best_k}.  Diebold-Mariano (thong ke am: hang dau tot hon hang cot):")
    dm = {}
    for k in cand:
        if k == best_k:
            continue
        st, p = dm_test(preds[best_k], preds[k], Yc, H)
        dm[f"{best_k}_vs_{k}"] = {"stat": st, "p": p}
        print(f"  {best_k} vs {k:<22} DM={st:>6.2f}  p={p:.3f}" + ("  *" if p < 0.05 else ""))
    s4 = "S4D (to hop)"
    if s4 in preds:
        print("S4D (to hop) doi dau:")
        for k in cand:
            if k == s4:
                continue
            st, p = dm_test(preds[s4], preds[k], Yc, H)
            dm[f"S4D_vs_{k}"] = {"stat": st, "p": p}
            print(f"  S4D vs {k:<22} DM={st:>6.2f}  p={p:.3f}" + ("  *" if p < 0.05 else ""))

    os.makedirs(args.out, exist_ok=True)
    tag = f"{args.target}_{'+'.join(feats)}_H{H}" + ("_fut" if args.future else "")
    with open(os.path.join(args.out, f"stage3_{tag}.json"), "w") as f:
        json.dump({"args": vars(args), "best_config": {m: {k: (list(x) if isinstance(x, tuple) else x) for k, x in c.items()}
                                                          for m, c in best.items()},
                   "tuning": tuning, "results": rows, "dm": dm, "best_model": best_k, "n_test": int(len(common)),
                   "mase_scale": scale, "seconds": time.time() - t0}, f, indent=2, ensure_ascii=False)
    out = pd.DataFrame({"date": common, "true_h1": Yc[:, 0] * ref.t_std + ref.t_mean})
    for k, v in preds.items():
        out[k] = v[:, 0] * ref.t_std + ref.t_mean
    out.to_csv(os.path.join(args.out, f"stage3_preds_{tag}.csv"), index=False)
    np.savez(os.path.join(args.out, f"stage3_preds_{tag}.npz"), dates=common.values.astype("datetime64[D]"),
             true=Yc * ref.t_std + ref.t_mean,
             **{re.sub(r"\W+", "_", k).strip("_"): v * ref.t_std + ref.t_mean for k, v in preds.items()})


if __name__ == "__main__":
    main()
