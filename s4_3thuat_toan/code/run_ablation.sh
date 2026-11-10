#!/bin/sh
# Ablation nhom dac trung cho MOT target. Dung: sh run_ablation.sh <target> <thu_muc_ra> <file_cau_hinh_tot_nhat.json> [horizon]
# Mo hinh: lightgbm, s4d (DLinear chi dung kenh target nen khong doi theo dac trung).
# Cau hinh sieu tham so GIU NGUYEN cho moi tap dac trung (khong tinh chinh lai) de so sanh sach.
set -e
T="$1"; OUT="$2"; CFG="$3"; H="${4:-7}"
PY="${PYTHON:-python}"
run() {  # ten  nhom_dac_trung  (bo qua neu da co ket qua)
  if ls "$OUT/$1"/stage3_preds_*.npz >/dev/null 2>&1; then echo "=== $1 da co, bo qua"; return; fi
  echo "=== $1  [$2]"
  $PY stage3.py --target "$T" --features "$2" --horizon "$H" --models lightgbm,s4d --n_seeds 3 \
      --load_config "$CFG" --out "$OUT/$1" 2>&1 | grep -v -i warning | sed -n '/KET QUA/,/^$/p'
}
run base ""
run cal cal
run cal_hol cal,hol
run cal_hol_wx cal,hol,wx
run cal_hol_wx_hydro cal,hol,wx,hydro
run full cal,hol,wx,hydro,mix
run no_cal hol,wx,hydro,mix
run no_hol cal,wx,hydro,mix
run no_wx cal,hol,hydro,mix
run no_hydro cal,hol,wx,mix
