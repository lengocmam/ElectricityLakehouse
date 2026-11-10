#!/bin/sh
# Chi chay S-Mamba (tinh chinh 8 lan thu, co luu tien trinh de chay tiep sau khi bi ngat); LightGBM/TimeXer/S4D lay tu results/timexer. Tong hop: python compare4.py
PY="${PYTHON:-python}"
task() {
  if ls results/smamba/$1/stage3_*_fut.json >/dev/null 2>&1; then echo "=== $1 da xong, bo qua"; return; fi
  echo "=== $1"
  CFG="results/timexer/$1/stage3_$2_$5_H$4_fut.json"
  [ -f "results/smamba/$1/best_config_tuned.json" ] && CFG="results/smamba/$1/best_config_tuned.json"
  $PY stage3.py --target "$2" --features "$3" --horizon "$4" --future 1 --models smamba --trials 8 \
      --load_config "$CFG" --out results/smamba/$1 > /tmp/sm_$1.log 2>&1
}
task load_H7  load_mean_mw cal,wx,hol 7 "cal+wx+hol"
task load_H1  load_mean_mw cal,wx,hol 1 "cal+wx+hol"
task hydro_H1 gen_hydro    cal,hydro  1 "cal+hydro"
task hydro_H7 gen_hydro    cal,hydro  7 "cal+hydro"
