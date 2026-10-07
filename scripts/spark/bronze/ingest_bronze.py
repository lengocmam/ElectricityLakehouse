import argparse

from bronze.evn import main as ingest_evn
from bronze.hydro import main as ingest_hydro
from bronze.nsmo import main as ingest_nsmo
from bronze.open_meteo import main as ingest_open_meteo


INGESTIONS = {
    "evn": ingest_evn,
    "evn_hydro": ingest_hydro,
    "open_meteo": ingest_open_meteo,
    "nsmo": ingest_nsmo,
}


def main():
    """Execute the bronze ingestion pipeline for a specific dataset."""
    parser = argparse.ArgumentParser()

    parser.add_argument("dataset", choices=INGESTIONS)
    parser.add_argument("--run-mode", choices=["incremental", "backfill"], default="incremental")
    parser.add_argument("--start-date", default=None)
    parser.add_argument("--end-date", default=None)

    args = parser.parse_args()

    start_date = args.start_date if args.start_date not in (None, "", "None") else None
    end_date = args.end_date if args.end_date not in (None, "", "None") else None

    if args.dataset in {"open_meteo", "evn", "evn_hydro", "nsmo"}:
        INGESTIONS[args.dataset](run_mode=args.run_mode, start_date=start_date, end_date=end_date)
    else:
        INGESTIONS[args.dataset]()


if __name__ == "__main__":
    main()