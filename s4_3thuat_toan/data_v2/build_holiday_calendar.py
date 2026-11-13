"""Tao lich ngay le Viet Nam 2023-2026 -> vn_holiday_calendar.csv

Nguon: goi `holidays` (country VN, ban 0.107) + hieu chinh thu cong, doi chieu voi thong bao chinh thuc:
  - Tet 2024: nghi 8-14/02/2024; Quoc khanh 2024: 31/08-03/09/2024
  - Tet 2025: nghi 25/01-02/02/2025
  - Tet 2026: nghi 14-22/02/2026 (mung 1 = 17/02/2026); Quoc khanh 2026: 29/08-02/09/2026
Tet 2023: nghi 20-26/01/2023, Quoc khanh 2023: 01-04/09/2023 (van ban 8056/VPCP-KGVX ngay 01/12/2022) - DA DOI CHIEU.
Tet Duong lich 2026: nghi 01-04/01/2026, hoan doi ngay lam viec thu Sau 02/01 sang thu Bay 10/01/2026
(van ban 12729/VPCP-KGVX ngay 25/12/2025) - DA DOI CHIEU; 10/01/2026 duoc danh dau la ngay lam bu.

Chay:  pip install holidays lunardate pandas ;  python build_holiday_calendar.py
"""
import numpy as np
import pandas as pd
import holidays

START, END = "2023-01-01", "2026-12-31"

# Hieu chinh thu cong
EXTRA = {"2026-01-02": "Tết Dương lịch (nghỉ hoán đổi)"}   # ky nghi Tet Duong lich 2026 keo dai 4 ngay (1-4/01), lam bu 10/01
EXCLUDE = {"2026-11-24"}                                    # Ngay Van hoa VN: khong phai ngay nghi
MAKEUP_SATURDAYS = ["2024-05-04", "2025-04-26", "2026-01-10", "2026-08-22"]  # thu Bay lam bu (hoan doi ngay nghi)
TET_DAY1 = ["2023-01-22", "2024-02-10", "2025-01-29", "2026-02-17"]  # mung 1 Tet


def group_of(name, month):
    if name is None or (isinstance(name, float) and np.isnan(name)):
        return ""
    if "Nguyên Đán" in name or "29 Tết" in name or "Giao thừa" in name:
        return "tet"
    if "Tết Dương lịch" in name:
        return "new_year"
    if "Hùng Vương" in name:
        return "hung_kings"
    if "Chiến thắng" in name or "Lao động" in name:
        return "reunification_labour"
    if "Quốc khánh" in name:
        return "national_day"
    return "reunification_labour" if month in (4, 5) else ("national_day" if month in (8, 9) else "other")


def main(out="vn_holiday_calendar.csv"):
    vn = holidays.country_holidays("VN", years=range(2023, 2027))
    names = {pd.Timestamp(d): n for d, n in vn.items()}
    for d in EXCLUDE:
        names.pop(pd.Timestamp(d), None)
    for d, n in EXTRA.items():
        names[pd.Timestamp(d)] = n

    cal = pd.DataFrame({"date": pd.date_range(START, END)})
    cal["holiday_name"] = cal.date.map(names)
    cal["is_public_holiday"] = cal.holiday_name.notna().astype(int)
    cal["holiday_group"] = [group_of(n, d.month) for n, d in zip(cal.holiday_name, cal.date)]
    dow = cal.date.dt.day_of_week
    cal["is_weekend"] = (dow >= 5).astype(int)
    cal["is_makeup_workday"] = cal.date.isin(pd.to_datetime(MAKEUP_SATURDAYS)).astype(int)
    off = (cal.is_public_holiday == 1) | ((cal.is_weekend == 1) & (cal.is_makeup_workday == 0))
    cal["is_working_day"] = (~off).astype(int)

    # Khoi nghi lien tuc = chuoi ngay nghi (le + cuoi tuan) co chua it nhat 1 ngay le
    run = (off != off.shift()).cumsum()
    has_hol = cal.is_public_holiday.groupby(run).transform("max") == 1
    brk = off & has_hol
    cal["is_holiday_break"] = brk.astype(int)
    size = off.groupby(run).transform("size")
    idx = off.groupby(run).cumcount() + 1
    cal["break_len"] = np.where(brk, size, 0)
    cal["break_day_index"] = np.where(brk, idx, 0)
    tet_run = cal.holiday_group.eq("tet").groupby(run).transform("max")
    cal["is_tet_break"] = (brk & tet_run).astype(int)

    # khoang cach den / tu ky nghi gan nhat (0 neu dang trong ky nghi, cat o 14)
    starts = cal.date[brk & (idx == 1)].values
    ends = cal.date[brk & (idx == size)].values
    d = cal.date.values
    nxt = np.array([((starts[starts >= x].min() - x).astype("timedelta64[D]").astype(int)
                     if (starts >= x).any() else 14) for x in d])
    prv = np.array([((x - ends[ends <= x].max()).astype("timedelta64[D]").astype(int)
                     if (ends <= x).any() else 14) for x in d])
    cal["days_to_next_break"] = np.where(brk, 0, np.minimum(nxt, 14))
    cal["days_since_break"] = np.where(brk, 0, np.minimum(prv, 14))

    # lech so voi mung 1 Tet gan nhat, cat o +-21 ngay
    t1 = pd.to_datetime(TET_DAY1).values
    off_days = np.array([(x - t1).astype("timedelta64[D]").astype(int) for x in d])
    nearest = off_days[np.arange(len(d)), np.abs(off_days).argmin(axis=1)]
    cal["tet_offset"] = np.clip(nearest, -21, 21)

    cal["date"] = cal.date.dt.strftime("%Y-%m-%d")
    cal.to_csv(out, index=False)
    print(cal.shape, "->", out)
    print("so ngay le:", int(cal.is_public_holiday.sum()), "| so ngay trong ky nghi lien tuc:", int(cal.is_holiday_break.sum()))


if __name__ == "__main__":
    main()
