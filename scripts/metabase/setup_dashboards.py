# -*- coding: utf-8 -*-
"""Master Runner for Metabase Lakehouse Dashboards.

Can provision all 3 dashboards or individual dashboards:
    python scripts/metabase/setup_dashboards.py --all
    python scripts/metabase/setup_dashboards.py --dashboard 1
    python scripts/metabase/setup_dashboards.py --dashboard 2
    python scripts/metabase/setup_dashboards.py --dashboard 3

Or invoke individual scripts directly:
    python scripts/metabase/setup_dashboard_01.py
    python scripts/metabase/setup_dashboard_02.py
    python scripts/metabase/setup_dashboard_03.py
"""
import argparse
import sys
from common import DEFAULT_PASS, DEFAULT_USER, get_metabase_url
from setup_dashboard_01 import deploy_dashboard_01
from setup_dashboard_02 import deploy_dashboard_02
from setup_dashboard_03 import deploy_dashboard_03
from setup_dashboard_04 import deploy_dashboard_04
from setup_dashboard_05 import deploy_dashboard_05
from setup_dashboard_06 import deploy_dashboard_06

def main():
    parser = argparse.ArgumentParser(description="Master Provisioner for Vietnam Electricity Lakehouse BI Dashboards")
    parser.add_argument("--dashboard", choices=["1", "2", "3", "4", "5", "6", "all"], default="all", help="Target dashboard to deploy (1-6, or all)")
    parser.add_argument("--url", default=None, help="Metabase URL (e.g. http://localhost:3000)")
    parser.add_argument("--user", default=DEFAULT_USER, help="Metabase admin username")
    parser.add_argument("--password", default=DEFAULT_PASS, help="Metabase admin password")
    args = parser.parse_args()

    url = get_metabase_url(args.url)
    print("="*75)
    print("VIETNAM ELECTRICITY LAKEHOUSE BI - DASHBOARD PROVISIONER")
    print(f"Target Metabase URL : {url}")
    print(f"Admin User          : {args.user}")
    print(f"Selected Scope      : Dashboard {args.dashboard}")
    print("="*75)

    dashboards_deployed = []

    if args.dashboard in ("1", "all"):
        d1_id = deploy_dashboard_01(url, args.user, args.password)
        dashboards_deployed.append(("01. Cân Bằng Cung - Cầu & Bù Trừ NLTT", d1_id))

    if args.dashboard in ("2", "all"):
        d2_id = deploy_dashboard_02(url, args.user, args.password)
        dashboards_deployed.append(("02. Khí Hậu Cực Đoan & Thủy Điện", d2_id))

    if args.dashboard in ("3", "all"):
        d3_id = deploy_dashboard_03(url, args.user, args.password)
        dashboards_deployed.append(("03. Mất Cân Đối Vùng Miền & Siêu Lưới 500kV", d3_id))

    if args.dashboard in ("4", "all"):
        d4_id = deploy_dashboard_04(url, args.user, args.password)
        dashboards_deployed.append(("04. Giám Sát Vận Hành Pipeline & Chất Lượng Dữ Liệu", d4_id))

    if args.dashboard in ("5", "all"):
        d5_id = deploy_dashboard_05(url, args.user, args.password)
        dashboards_deployed.append(("05. Bức Tranh Điều Độ Đa Nguồn 24 Giờ", d5_id))

    if args.dashboard in ("6", "all"):
        d6_id = deploy_dashboard_06(url, args.user, args.password)
        dashboards_deployed.append(("06. Dự Báo Phụ Tải & Thủy Điện Bằng AI (S4D Deep Learning)", d6_id))

    print("\n" + "#"*75)
    print("SUMMARY: ALL REQUESTED DASHBOARDS READY ON METABASE!")
    print("#"*75)
    for name, did in dashboards_deployed:
        print(f"  • {name} -> {url}/dashboard/{did}")
    print("#"*75 + "\n")

if __name__ == "__main__":
    main()
