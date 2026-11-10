#!/bin/sh
# Tai lap toan bo du lieu v2 tu 3 CSV goc. Dung:  sh run_all.sh <thu_muc_3_csv_goc>
set -e
SRC="${1:-/mnt/user-data/uploads}"
python build_holiday_calendar.py                                   # lich ngay le -> vn_holiday_calendar.csv
python build_hourly_v2.py "$SRC" vn_holiday_calendar.csv hourly_v2.csv
python build_daily_v2.py  "$SRC" hourly_v2.csv vn_holiday_calendar.csv unified_daily_v2.csv
python audit_data.py      "$SRC" hourly_v2.csv unified_daily_v2.csv   # phai bao 20/20 PASS
