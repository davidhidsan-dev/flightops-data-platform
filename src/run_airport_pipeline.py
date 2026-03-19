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
from src.utils.logger import get_logger
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

logger = get_logger(__name__)


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
    Build a UTC Unix timestamp window [begin, end] for a given date.
    """
    start_dt = datetime.strptime(run_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    end_dt = start_dt + timedelta(days=1) - timedelta(seconds=1)

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

def confirm_bigquery_load(has_warning: bool = False) -> bool:
    """
    Ask the user whether to load the published dataset into BigQuery.

    If the run has warnings, require a second confirmation before proceeding.
    """
    while True:
        answer = input(
            "Do you want to load the published dataset into BigQuery? (Y/N): "
        ).strip().upper()

        if answer == "N":
            return False

        if answer == "Y":
            if not has_warning:
                return True

            while True:
                warning_answer = input(
                    "This run has warnings and may be incomplete. "
                    "Do you still want to load it into BigQuery? (Y/N): "
                ).strip().upper()

                if warning_answer == "Y":
                    return True
                if warning_answer == "N":
                    return False

                print("Please answer Y or N.")

        print("Please answer Y or N.")

def main() -> None:
    """
    Run the full airport data pipeline for one airport and one date.
    """
    args = parse_args()
    airport_icao = args.airport_icao
    run_date = args.date

    try:
        logger.info(
            f"Pipeline started for airport={airport_icao} date={run_date}"
        )

        ensure_output_directories()

        airport_metadata = load_airport_metadata(airport_icao)
        latitude = float(airport_metadata["latitude"])
        longitude = float(airport_metadata["longitude"])

        logger.info(
            f"Airport metadata loaded for airport={airport_icao} "
            f"latitude={latitude} longitude={longitude}"
        )

        begin, end = build_unix_day_window(run_date)
        logger.info(
            f"Built UTC window for date={run_date} begin={begin} end={end}"
        )

        logger.info("Starting raw extraction")
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
        logger.info("Raw extraction completed")

        logger.info("Loading raw JSON files")
        with build_arrivals_raw_path(airport_icao, run_date).open("r", encoding="utf-8") as f:
            arrivals_raw = json.load(f)

        with build_departures_raw_path(airport_icao, run_date).open("r", encoding="utf-8") as f:
            departures_raw = json.load(f)

        with build_weather_raw_path(airport_icao, run_date).open("r", encoding="utf-8") as f:
            weather_raw = json.load(f)

        logger.info(f"Loaded arrivals raw records={len(arrivals_raw)}")
        logger.info(f"Loaded departures raw records={len(departures_raw)}")
        logger.info(
            f"Loaded weather raw hourly records={len(weather_raw['hourly']['time'])}"
        )

        has_operational_warning = len(arrivals_raw) == 0 or len(departures_raw) == 0

        if has_operational_warning:
            logger.warning(
                f"One or more operational source datasets are empty for "
                f"airport={airport_icao} date={run_date}. "
                f"Pipeline output may be incomplete for this run."
            )

        logger.info("Building staging tables")
        arrivals_df = transform_arrivals_dataframe(arrivals_raw)
        departures_df = transform_departures_dataframe(departures_raw)
        weather_df = transform_weather_dataframe(weather_raw, airport_icao)

        logger.info(f"Arrivals staging DataFrame built rows={len(arrivals_df)}")
        logger.info(f"Departures staging DataFrame built rows={len(departures_df)}")
        logger.info(f"Weather staging DataFrame built rows={len(weather_df)}")

        if arrivals_df.empty:
            logger.warning(
                f"Arrivals staging DataFrame is empty for airport={airport_icao} date={run_date}"
            )
        if departures_df.empty:
            logger.warning(
                f"Departures staging DataFrame is empty for airport={airport_icao} date={run_date}"
            )
        if weather_df.empty:
            logger.warning(
                f"Weather staging DataFrame is empty for airport={airport_icao} date={run_date}"
            )

        arrivals_staging_path = build_arrivals_staging_path(airport_icao, run_date)
        departures_staging_path = build_departures_staging_path(airport_icao, run_date)
        weather_staging_path = build_weather_staging_path(airport_icao, run_date)

        arrivals_df.to_csv(arrivals_staging_path, index=False)
        departures_df.to_csv(departures_staging_path, index=False)
        weather_df.to_csv(weather_staging_path, index=False)

        logger.info(f"Arrivals staging saved to {arrivals_staging_path}")
        logger.info(f"Departures staging saved to {departures_staging_path}")
        logger.info(f"Weather staging saved to {weather_staging_path}")

        logger.info("Building marts")
        operations_df = build_airport_hourly_operations(arrivals_df, departures_df)
        operations_path = build_operations_mart_path(airport_icao, run_date)
        operations_df.to_csv(operations_path, index=False)

        logger.info(
            f"Airport hourly operations mart built rows={len(operations_df)}"
        )
        logger.info(f"Airport hourly operations mart saved to {operations_path}")

        enriched_df = build_airport_hourly_operations_enriched(operations_df, weather_df)
        enriched_path = build_enriched_mart_path(airport_icao, run_date)
        enriched_df.to_csv(enriched_path, index=False)

        logger.info(
            f"Airport hourly operations enriched mart built rows={len(enriched_df)}"
        )
        logger.info(
            f"Airport hourly operations enriched mart saved to {enriched_path}"
        )

        logger.info("Building published dataset")
        published_df = build_published_dataset()
        published_path = save_published_dataset(published_df)

        logger.info(f"Published dataset built rows={len(published_df)}")
        logger.info(f"Published dataset saved to {published_path}")

        if published_df.empty:
            logger.warning("Published dataset is empty after consolidation")

        logger.info("Running data quality checks on published dataset")
        run_airport_operations_checks(published_df)
        logger.info("Data quality checks completed successfully")

        if confirm_bigquery_load(has_warning=has_operational_warning):
            logger.info("Loading published dataset to BigQuery")
            table_id = load_airport_operations_to_bigquery(published_df)
            logger.info(f"BigQuery load completed table={table_id}")
        else:
            if has_operational_warning:
                logger.info("BigQuery load skipped after warning confirmation prompt")
            else:
                logger.info("BigQuery load skipped by user")

        logger.info(
            f"Pipeline completed successfully for airport={airport_icao} date={run_date}"
        )

    except Exception as error:
        logger.error(
            f"Pipeline failed for airport={airport_icao} date={run_date}: {error}"
        )
        raise


if __name__ == "__main__":
    main()