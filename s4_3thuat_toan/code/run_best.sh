#!/bin/sh
# Chay nhanh voi cau hinh da tinh chinh san (khong tinh chinh lai). Dung:  sh run_best.sh [load_H7|load_H1|hydro_H1|hydro_H7|all]
# Moi bai toan: 3 mo hinh x 5 seed. Thiet bi tu dong chon GPU neu co.
set -e
PY="${PYTHON:-python}"
run() {  # ten target features horizon
  echo "=== $1"
  $PY stage3.py --target "$2" --features "$3" --horizon "$4" --future 1 --models lightgbm,timexer,smamba,s4d \
      --load_config "best_configs/$1.json" --out "results/best_$1" 2>&1 | grep -v -i warning | sed -n '/KET QUA/,/^S4D (to hop) doi dau/p'
}
W="${1:-all}"
[ "$W" = load_H7 ]  || [ "$W" = all ] && run load_H7  load_mean_mw cal,wx,hol 7
[ "$W" = load_H1 ]  || [ "$W" = all ] && run load_H1  load_mean_mw cal,wx,hol 1
[ "$W" = hydro_H1 ] || [ "$W" = all ] && run hydro_H1 gen_hydro    cal,hydro  1
[ "$W" = hydro_H7 ] || [ "$W" = all ] && run hydro_H7 gen_hydro    cal,hydro  7
true
