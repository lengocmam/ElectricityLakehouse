#!/bin/sh
# Tinh chinh MO RONG cho 3 mo hinh tu bai bao: 12 lan thu moi mo hinh, khong gian tim kiem co them 'anchor' (hoc phan lech so voi hom nay / cung thu tuan truoc).
# Luu tien trinh tung lan thu; chay lai se tiep tuc. LightGBM lay tu results/timexer de so sanh.
PY="${PYTHON:-python}"
task() {  # ten target features horizon
  if ls results/tuned2/$1/stage3_*_fut.json >/dev/null 2>&1; then echo "=== $1 da xong"; return; fi
  echo "=== $1"
  CFG=""; [ -f results/tuned2/$1/best_config_tuned.json ] && CFG="--load_config results/tuned2/$1/best_config_tuned.json"
  $PY stage3.py --target "$2" --features "$3" --horizon "$4" --future 1 --models timexer,smamba,s4d --trials 12 --search_anchor 1 $CFG \
      --out results/tuned2/$1 > /tmp/t2_$1.log 2>&1
}
task load_H1 load_mean_mw cal,wx,hol 1
task load_H7 load_mean_mw cal,wx,hol 7
task hydro_H7 gen_hydro cal,hydro 7
task hydro_H1 gen_hydro cal,hydro 1
