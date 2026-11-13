#!/bin/sh
# S4D + LightGBM (dung cau hinh da tinh chinh san) + TimeXer (tinh chinh moi, 8 lan thu). Co the chay lai: bo qua bai toan da co ket qua.
PY="${PYTHON:-python}"
task() {  # ten target features horizon file_cau_hinh
  if ls results/timexer/$1/stage3_*_fut.json >/dev/null 2>&1; then echo "=== $1 da xong, bo qua"; return; fi
  echo "=== $1"
  $PY stage3.py --target "$2" --features "$3" --horizon "$4" --future 1 --models lightgbm,timexer,s4d --trials 8 \
      --load_config "$5" --out results/timexer/$1 > /tmp/tx_$1.log 2>&1
}
task load_H7  load_mean_mw cal,wx,hol 7 "results/fut/stage3_load_mean_mw_cal+wx+hol_H7_fut.json"
task load_H1  load_mean_mw cal,wx,hol 1 "results/fut/stage3_load_mean_mw_cal+wx+hol_H1_fut.json"
task hydro_H1 gen_hydro    cal,hydro  1 "results/fut/stage3_gen_hydro_cal+hydro_H1_fut.json"
task hydro_H7 gen_hydro    cal,hydro  7 "results/fut/stage3_gen_hydro_cal+hydro_H7_fut.json"
