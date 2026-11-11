# -*- coding: utf-8 -*-
"""Master Runner for Metabase Lakehouse Dashboards.

Manages the 2 core operational dashboards:
    python scripts/metabase/setup_dashboards.py --all
    python scripts/metabase/setup_dashboards.py --dashboard 1
    python scripts/metabase/setup_dashboards.py --dashboard 2

Or invoke individual scripts directly:
    python scripts/metabase/setup_dashboard_01.py
    python scripts/metabase/setup_dashboard_02.py
"""
import argparse
import sys
from common import DEFAULT_PASS, DEFAULT_USER, get_metabase_url
from setup_dashboard_01 import deploy_dashboard_01
from setup_dashboard_02 import deploy_dashboard_02

def main():
    parser = argparse.ArgumentParser(description="Master Provisioner for Vietnam Electricity Lakehouse BI Dashboards")
    parser.add_argument("--dashboard", choices=["1", "2", "all"], default="all", help="Target dashboard to deploy (1, 2, or all)")
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
        dashboards_deployed.append(("01. Cân Bằng Cung - Cầu & Chuyển Dịch Năng Lượng", d1_id))

    if args.dashboard in ("2", "all"):
        d2_id = deploy_dashboard_02(url, args.user, args.password)
        dashboards_deployed.append(("02. Thời Tiết Cực Đoan, An Ninh Nước & Thủy Điện", d2_id))

    print("\n" + "#"*75)
    print("SUMMARY: ALL REQUESTED DASHBOARDS READY ON METABASE!")
    print("#"*75)
    for name, did in dashboards_deployed:
        print(f"  • {name} -> {url}/dashboard/{did}")
    print("#"*75 + "\n")

if __name__ == "__main__":
    main()
