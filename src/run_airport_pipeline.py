import argparse
import json
from datetime import datetime, timedelta, timezone

import pandas as pd

from src.config import AIRPORT_SEED_PATH, DATA_DIR
from src.extract.extract_raw_data import (
    extract_arrivals_raw,
    extract_departures_raw,
    extract_weather_raw,
)
from src.load.bigquery_loader import load_airport_operations_to_bigquery
from src.quality.check_airport_operations import run_airport_operations_checks
from src.transform.airport_hourly_operations import build_airport_hourly_operations
from src.transform.airport_hourly_operations_enriched import (
    build_airport_hourly_operations_enriched,
)
from src.transform.arrivals_transform import transform_arrivals_dataframe
from src.transform.departures_transform import transform_departures_dataframe
from src.transform.publish_airport_operations import (
    build_published_dataset,
    save_published_dataset,
)
from src.transform.weather_transform import transform_weather_dataframe
from src.utils.path_builders import (
    build_arrivals_raw_path,
    build_arrivals_staging_path,
    build_departures_raw_path,
    build_departures_staging_path,
    build_enriched_mart_path,
    build_operations_mart_path,
    build_weather_raw_path,
    build_weather_staging_path,
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


def ensure_output_directories() -> None:
    """
    Ensure local pipeline output directories exist.
    """
    (DATA_DIR / "staging").mkdir(parents=True, exist_ok=True)
    (DATA_DIR / "marts").mkdir(parents=True, exist_ok=True)
    (DATA_DIR / "published").mkdir(parents=True, exist_ok=True)


def main() -> None:
    """
    Run the full airport data pipeline for one airport and one date.
    """
    args = parse_args()
    airport_icao = args.airport_icao
    run_date = args.date

    ensure_output_directories()

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

    print("Loading raw JSON files...")
    with build_arrivals_raw_path(airport_icao, run_date).open("r", encoding="utf-8") as f:
        arrivals_raw = json.load(f)

    with build_departures_raw_path(airport_icao, run_date).open("r", encoding="utf-8") as f:
        departures_raw = json.load(f)

    with build_weather_raw_path(airport_icao, run_date).open("r", encoding="utf-8") as f:
        weather_raw = json.load(f)

    print("Building staging tables...")
    arrivals_df = transform_arrivals_dataframe(arrivals_raw)
    departures_df = transform_departures_dataframe(departures_raw)
    weather_df = transform_weather_dataframe(weather_raw, airport_icao)

    arrivals_staging_path = build_arrivals_staging_path(airport_icao, run_date)
    departures_staging_path = build_departures_staging_path(airport_icao, run_date)
    weather_staging_path = build_weather_staging_path(airport_icao, run_date)

    arrivals_df.to_csv(arrivals_staging_path, index=False)
    departures_df.to_csv(departures_staging_path, index=False)
    weather_df.to_csv(weather_staging_path, index=False)

    print("Building marts...")
    operations_df = build_airport_hourly_operations(arrivals_df, departures_df)
    operations_path = build_operations_mart_path(airport_icao, run_date)
    operations_df.to_csv(operations_path, index=False)

    enriched_df = build_airport_hourly_operations_enriched(operations_df, weather_df)
    enriched_path = build_enriched_mart_path(airport_icao, run_date)
    enriched_df.to_csv(enriched_path, index=False)

    print("Publishing consolidated dataset...")
    published_df = build_published_dataset()
    published_path = save_published_dataset(published_df)

    print(f"Published dataset saved to: {published_path}")

    print("Running data quality checks...")
    run_airport_operations_checks(published_df)

    print("Loading published dataset to BigQuery...")
    table_id = load_airport_operations_to_bigquery(published_df)
    print(f"Loaded published dataset into {table_id}")

    print("Pipeline completed successfully.")


if __name__ == "__main__":
    main()