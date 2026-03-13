import argparse
import subprocess
import sys
from datetime import datetime, timedelta, timezone

import pandas as pd

from src.config import AIRPORT_SEED_PATH
from src.extract.extract_raw_data import (
    extract_arrivals_raw,
    extract_departures_raw,
    extract_weather_raw,
)


def parse_args() -> argparse.Namespace:
    """
    Parse command-line arguments for the airport pipeline runner.
    """
    parser = argparse.ArgumentParser(
        description="Run the full airport data pipeline for a single airport and date."
    )
    parser.add_argument(
        "--airport-icao",
        required=True,
        help="ICAO code of the airport, for example LEMD.",
    )
    parser.add_argument(
        "--date",
        required=True,
        help="Date to process in YYYY-MM-DD format.",
    )
    return parser.parse_args()


def load_airport_metadata(airport_icao: str) -> dict:
    """
    Load airport metadata from the seed CSV and return the matching airport row.
    """
    airports_df = pd.read_csv(AIRPORT_SEED_PATH)
    airport_row = airports_df.loc[airports_df["airport_icao"] == airport_icao]

    if airport_row.empty:
        raise ValueError(f"Airport {airport_icao} not found in seed file.")

    return airport_row.iloc[0].to_dict()


def build_unix_day_window(run_date: str) -> tuple[int, int]:
    """
    Build a UTC Unix timestamp window [begin, end) for a given date.
    """
    start_dt = datetime.strptime(run_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    end_dt = start_dt + timedelta(days=1)

    begin = int(start_dt.timestamp())
    end = int(end_dt.timestamp())

    return begin, end


def run_module(module_name: str, airport_icao: str, run_date: str) -> None:
    """
    Execute a Python module with airport and date arguments.
    """
    command = [
        sys.executable,
        "-m",
        module_name,
        "--airport-icao",
        airport_icao,
        "--date",
        run_date,
    ]
    subprocess.run(command, check=True)


def main() -> None:
    """
    Run the full airport data pipeline for one airport and one date.
    """
    args = parse_args()
    airport_icao = args.airport_icao
    run_date = args.date

    airport_metadata = load_airport_metadata(airport_icao)
    latitude = float(airport_metadata["latitude"])
    longitude = float(airport_metadata["longitude"])

    begin, end = build_unix_day_window(run_date)

    print("Starting raw extraction...")
    extract_arrivals_raw(
        airport_icao=airport_icao,
        begin=begin,
        end=end,
        run_date=run_date,
    )
    extract_departures_raw(
        airport_icao=airport_icao,
        begin=begin,
        end=end,
        run_date=run_date,
    )
    extract_weather_raw(
        airport_icao=airport_icao,
        latitude=latitude,
        longitude=longitude,
        run_date=run_date,
    )

    print("Running staging transforms...")
    run_module("src.transform.arrivals_transform", airport_icao, run_date)
    run_module("src.transform.departures_transform", airport_icao, run_date)
    run_module("src.transform.weather_transform", airport_icao, run_date)

    print("Building marts...")
    run_module("src.transform.airport_hourly_operations", airport_icao, run_date)
    run_module("src.transform.airport_hourly_operations_enriched", airport_icao, run_date)

    print("Pipeline completed successfully.")


if __name__ == "__main__":
    main()