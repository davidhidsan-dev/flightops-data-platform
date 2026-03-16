import argparse
import pandas as pd

from src.config import DATA_DIR
from src.utils.path_builders import (
    build_enriched_mart_path,
    build_operations_mart_path,
    build_weather_staging_path,
)


def parse_args() -> argparse.Namespace:
    """
    Parse command-line arguments for the enriched hourly operations mart script.
    """
    parser = argparse.ArgumentParser(
        description="Build the airport_hourly_operations_enriched mart table."
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


def build_airport_hourly_operations_enriched(
    operations_df: pd.DataFrame,
    weather_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Enrich hourly airport operations with weather data and derived flags.
    """
    enriched_df = operations_df.merge(
        weather_df,
        left_on=["airport_icao", "operation_hour_utc"],
        right_on=["airport_icao", "weather_hour_utc"],
        how="left",
    )

    enriched_df = enriched_df.drop(columns=["weather_hour_utc"])

    traffic_threshold = enriched_df["total_flights_observed"].quantile(0.75)

    enriched_df["is_rainy_hour"] = enriched_df["precipitation_mm"] > 0
    enriched_df["is_high_wind_hour"] = enriched_df["wind_speed_10m_kmh"] >= 20
    enriched_df["is_high_traffic_hour"] = (
        enriched_df["total_flights_observed"] >= traffic_threshold
    )

    enriched_df = enriched_df.sort_values(
        by=["airport_icao", "operation_hour_utc"]
    ).reset_index(drop=True)

    return enriched_df


def main() -> None:
    """
    Load the airport_hourly_operations mart table and the weather staging table,
    join them by airport and hour, and build the final enriched operations dataset.
    """
    args = parse_args()
    airport_icao = args.airport_icao
    run_date = args.date

    marts_dir = DATA_DIR / "marts"
    marts_dir.mkdir(parents=True, exist_ok=True)

    operations_path = build_operations_mart_path(airport_icao, run_date)
    weather_path = build_weather_staging_path(airport_icao, run_date)

    operations_df = pd.read_csv(
        operations_path,
        parse_dates=["operation_hour_utc"],
    )

    weather_df = pd.read_csv(
        weather_path,
        parse_dates=["weather_hour_utc"],
    )

    enriched_df = build_airport_hourly_operations_enriched(
        operations_df,
        weather_df,
    )

    output_path = build_enriched_mart_path(airport_icao, run_date)
    enriched_df.to_csv(output_path, index=False)

    print(f"Rows written: {len(enriched_df)}")
    print(f"Enriched mart file saved to: {output_path}")
    print("Sample:")
    print(enriched_df.head())


if __name__ == "__main__":
    main()