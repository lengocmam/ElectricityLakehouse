"""Giai doan 2: so sanh day du S4D voi baseline tren cung giao thuc.

Mo hinh: persistence, seasonal naive 7, ridge, LightGBM, DLinear, PatchTST (don bien), S4D.
Chi so: MAE, RMSE, MAPE, MASE (thang do = sai so seasonal-naive-7 trong mau train) + kiem dinh Diebold-Mariano.
Cac mo hinh mang chay nhieu seed; bao cao trung binh +- do lech va ca DU BAO TO HOP (trung binh cac seed).

Vi du:
  python train2.py --target load_mean_mw --features cal,wx,hol
  python train2.py --target gen_hydro --features cal,hydro --horizon 7
"""
import argparse, json, os, time
import numpy as np
import pandas as pd
import torch
import torch.nn as nn
from scipy import stats

from data import prepare
from models import build_model
from baselines import persistence, seasonal_naive, ridge_fit_predict, lightgbm_fit_predict


def mase_scale(prep, target, train_end, period=7):
    s = prep.df.loc[prep.df.date <= pd.Timestamp(train_end), target].values.astype(float)
    return float(np.nanmean(np.abs(s[period:] - s[:-period])))


def metrics(pred_z, true_z, prep, scale):
    p, t = pred_z * prep.t_std + prep.t_mean, true_z * prep.t_std + prep.t_mean
    e = p - t
    mae = float(np.abs(e).mean())
    return {"MAE": mae, "RMSE": float(np.sqrt((e ** 2).mean())),
            "MAPE%": float(np.mean(np.abs(e) / np.maximum(np.abs(t), 1e-9)) * 100), "MASE": mae / scale}


def dm_test(pred1_z, pred2_z, true_z, H):
    """Diebold-Mariano (ham mat mat = |sai so|), phuong sai Newey-West, hieu chinh Harvey. Tra ve (thong ke, p hai phia).
    Thong ke am: mo hinh 1 co sai so nho hon mo hinh 2."""
    d = (np.abs(pred1_z - true_z) - np.abs(pred2_z - true_z)).mean(axis=1)
    n, db = len(d), d.mean()
    g = lambda k: np.mean((d[k:] - db) * (d[:n - k] - db))
    var = max(g(0) + 2 * sum(g(k) for k in range(1, H)), 1e-12)
    stat = db / np.sqrt(var / n) * np.sqrt((n + 1 - 2 * H + H * (H - 1) / n) / n)
    return float(stat), float(2 * (1 - stats.t.cdf(abs(stat), df=n - 1)))


def run_nn(prep, args, name, seed, device):
    torch.manual_seed(seed); np.random.seed(seed)
    W = prep.win
    X = torch.tensor(W.X, device=device); Y = torch.tensor(W.Y, device=device)
    use_f = bool(getattr(args, "future", 0)) and name in ("s4d", "timexer", "smamba") and W.F is not None
    Fu = torch.tensor(W.F, device=device) if use_f else None
    args.n_future = int(W.F.shape[-1]) if use_f else 0
    fwd = (lambda idx: model(X[idx], Fu[idx])) if use_f else (lambda idx: model(X[idx]))
    tr, va, te = (np.where(W.split == i)[0] for i in range(3))
    model = build_model(name, X.shape[-1], args.horizon, args.lookback, args).to(device)
    ssm = [p for p in model.parameters() if getattr(p, "_no_wd", False)]
    rest = [p for p in model.parameters() if not getattr(p, "_no_wd", False)]
    groups = [{"params": rest, "weight_decay": args.wd}]
    if ssm:
        groups.append({"params": ssm, "weight_decay": 0.0, "lr": min(args.lr, 1e-3)})
    opt = torch.optim.AdamW(groups, lr=args.lr)
    sched = torch.optim.lr_scheduler.CosineAnnealingLR(opt, T_max=args.epochs)
    loss_fn = nn.SmoothL1Loss(beta=0.5)

    def predict(idx):
        model.eval()
        with torch.no_grad():
            return torch.cat([fwd(idx[i:i + 256]) for i in range(0, len(idx), 256)]).cpu().numpy()

    best, state, bad = 1e9, None, 0
    for ep in range(args.epochs):
        model.train()
        perm = np.random.permutation(tr)
        for i in range(0, len(perm), args.batch):
            b = perm[i:i + args.batch]
            opt.zero_grad()
            loss_fn(fwd(b), Y[b]).backward()
            nn.utils.clip_grad_norm_(model.parameters(), 1.0)
            opt.step()
        sched.step()
        v = np.abs(predict(va) - W.Y[va]).mean() * prep.t_std
        if v < best:
            best, bad, state = v, 0, {k: t.clone() for k, t in model.state_dict().items()}
        else:
            bad += 1
            if bad >= args.patience:
                break
    model.load_state_dict(state)
    return predict(te), best, ep + 1, sum(p.numel() for p in model.parameters())


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", default="unified_daily_v2.csv")
    ap.add_argument("--target", default="load_mean_mw")
    ap.add_argument("--features", default="cal,wx,hol")
    ap.add_argument("--models", default="s4d,dlinear,patchtst")
    ap.add_argument("--lookback", type=int, default=56)
    ap.add_argument("--horizon", type=int, default=1)
    ap.add_argument("--train_end", default="2025-06-30")
    ap.add_argument("--val_end", default="2025-12-31")
    ap.add_argument("--d_model", type=int, default=64)
    ap.add_argument("--n_layers", type=int, default=3)
    ap.add_argument("--d_state", type=int, default=64)
    ap.add_argument("--dropout", type=float, default=0.1)
    ap.add_argument("--dt_min", type=float, default=0.01)
    ap.add_argument("--dt_max", type=float, default=0.5)
    ap.add_argument("--lr", type=float, default=2e-3)
    ap.add_argument("--wd", type=float, default=0.05)
    ap.add_argument("--epochs", type=int, default=80)
    ap.add_argument("--patience", type=int, default=15)
    ap.add_argument("--batch", type=int, default=64)
    ap.add_argument("--n_seeds", type=int, default=5)
    ap.add_argument("--out", default="results")
    args = ap.parse_args()

    feats = [f for f in args.features.split(",") if f]
    names = [m for m in args.models.split(",") if m]
    device = "cuda" if torch.cuda.is_available() else "cpu"
    prep = prepare(args.csv, args.target, feats, args.lookback, args.horizon, args.train_end, args.val_end)
    W, H = prep.win, args.horizon
    te = W.split == 2
    Yte = W.Y[te]
    scale = mase_scale(prep, args.target, args.train_end)
    n = [int((W.split == i).sum()) for i in range(3)]
    print(f"muc tieu={args.target}  dac trung={prep.feature_names}")
    print(f"cua so: train={n[0]} val={n[1]} test={n[2]}  lookback={args.lookback} horizon={H}  MASE scale={scale:.1f}  {device}")

    preds, rows = {}, {}      # preds: du bao tren tap test (khong gian z), mot mang cho moi mo hinh/to hop
    preds["persistence"] = persistence(W.X[te], H)
    preds["seasonal_naive_7"] = seasonal_naive(W.X[te], H)
    rp, alpha = ridge_fit_predict(W.X, W.Y, W.split, H)
    preds[f"ridge(a={alpha})"] = rp[te]
    preds["lightgbm"] = lightgbm_fit_predict(W.X, W.Y, W.split, H)[te]
    for k, p in preds.items():
        rows[k] = metrics(p, Yte, prep, scale)

    t0 = time.time()
    label = {"s4d": "S4D", "dlinear": "DLinear", "patchtst": "PatchTST"}
    for nm in names:
        per, ps, params = [], [], 0
        for s in range(args.n_seeds):
            p, v, ep, params = run_nn(prep, args, nm, s, device)
            ps.append(p); per.append(metrics(p, Yte, prep, scale))
            print(f"  {label[nm]} seed {s}: val MAE={v:.1f} test MAE={per[-1]['MAE']:.1f} (epoch {ep})")
        agg = {k: float(np.mean([m[k] for m in per])) for k in per[0]}
        agg["MAE_std"] = float(np.std([m["MAE"] for m in per]))
        agg["params"] = params
        rows[f"{label[nm]} (TB {args.n_seeds} seed)"] = agg
        ens = np.mean(ps, axis=0)
        preds[f"{label[nm]} (to hop)"] = ens
        rows[f"{label[nm]} (to hop)"] = metrics(ens, Yte, prep, scale)

    print(f"\nKET QUA TEST (don vi goc, TB {H} buoc du bao)  MASE<1: tot hon seasonal-naive-7 trong mau train")
    print(f"{'mo hinh':<26}{'MAE':>11}{'RMSE':>11}{'MAPE%':>8}{'MASE':>7}")
    for k, v in rows.items():
        extra = f"  (±{v['MAE_std']:.1f})" if "MAE_std" in v else ""
        print(f"{k:<26}{v['MAE']:>11.1f}{v['RMSE']:>11.1f}{v['MAPE%']:>8.2f}{v['MASE']:>7.3f}{extra}")

    # Diebold-Mariano: moi mo hinh/to hop vs baseline tot nhat (theo MAE) trong nhom baseline
    base_keys = ["persistence", "seasonal_naive_7"] + [k for k in rows if k.startswith("ridge")] + ["lightgbm"]
    best_base = min(base_keys, key=lambda k: rows[k]["MAE"])
    print(f"\nDiebold-Mariano (ham mat mat = |sai so|), tham chieu = {best_base}. Thong ke am: mo hinh tot hon tham chieu.")
    dm = {}
    for k in preds:
        if k == best_base:
            continue
        st, p = dm_test(preds[k], preds[best_base], Yte, H)
        dm[k] = {"stat": st, "p": p}
        print(f"  {k:<26} DM={st:>6.2f}  p={p:.3f}" + ("  *" if p < 0.05 else ""))
    s4 = "S4D (to hop)"
    if s4 in preds:
        print("\nS4D (to hop) doi dau tung mo hinh (am: S4D tot hon):")
        for k in preds:
            if k == s4:
                continue
            st, p = dm_test(preds[s4], preds[k], Yte, H)
            dm[f"S4D_vs_{k}"] = {"stat": st, "p": p}
            print(f"  vs {k:<24} DM={st:>6.2f}  p={p:.3f}" + ("  *" if p < 0.05 else ""))

    os.makedirs(args.out, exist_ok=True)
    tag = f"{args.target}_{'+'.join(feats) or 'target'}_L{args.lookback}_H{H}"
    with open(os.path.join(args.out, f"stage2_{tag}.json"), "w") as f:
        json.dump({"args": vars(args), "windows": n, "mase_scale": scale, "results": rows, "dm": dm,
                   "best_baseline": best_base, "seconds": time.time() - t0}, f, indent=2, ensure_ascii=False)
    df = pd.DataFrame({"date": pd.to_datetime(W.dates[te]), "true": Yte[:, 0] * prep.t_std + prep.t_mean})
    for k, p in preds.items():
        df[k] = p[:, 0] * prep.t_std + prep.t_mean
    df.to_csv(os.path.join(args.out, f"stage2_preds_{tag}.csv"), index=False)


if __name__ == "__main__":
    main()
