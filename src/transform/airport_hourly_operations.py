import argparse
import pandas as pd

from src.config import DATA_DIR
from src.utils.path_builders import (
    build_arrivals_staging_path,
    build_departures_staging_path,
    build_operations_mart_path,
)


def parse_args() -> argparse.Namespace:
    """
    Parse command-line arguments for the hourly operations mart script.
    """
    parser = argparse.ArgumentParser(
        description="Build the airport_hourly_operations mart table."
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


def main() -> None:
    """
    Load arrivals and departures staging files, aggregate them by airport and hour,
    and build the airport_hourly_operations mart table.
    """
    args = parse_args()
    airport_icao = args.airport_icao
    run_date = args.date

    marts_dir = DATA_DIR / "marts"
    marts_dir.mkdir(parents=True, exist_ok=True)

    arrivals_path = build_arrivals_staging_path(airport_icao, run_date)
    departures_path = build_departures_staging_path(airport_icao, run_date)

    arrivals_df = pd.read_csv(arrivals_path, parse_dates=["observed_arrival_hour_utc"])
    departures_df = pd.read_csv(departures_path, parse_dates=["observed_departure_hour_utc"])

    arrivals_hourly = (
        arrivals_df.groupby(
            ["arrival_airport_icao", "observed_arrival_hour_utc"],
            as_index=False
        )
        .agg(
            observed_arrivals_count=("icao24", "count"),
            unique_arrival_aircraft_count=("icao24", "nunique"),
        )
        .rename(
            columns={
                "arrival_airport_icao": "airport_icao",
                "observed_arrival_hour_utc": "operation_hour_utc",
            }
        )
    )

    departures_hourly = (
        departures_df.groupby(
            ["departure_airport_icao", "observed_departure_hour_utc"],
            as_index=False
        )
        .agg(
            observed_departures_count=("icao24", "count"),
            unique_departure_aircraft_count=("icao24", "nunique"),
        )
        .rename(
            columns={
                "departure_airport_icao": "airport_icao",
                "observed_departure_hour_utc": "operation_hour_utc",
            }
        )
    )

    operations_df = arrivals_hourly.merge(
        departures_hourly,
        on=["airport_icao", "operation_hour_utc"],
        how="outer",
    )

    count_columns = [
        "observed_arrivals_count",
        "unique_arrival_aircraft_count",
        "observed_departures_count",
        "unique_departure_aircraft_count",
    ]

    operations_df[count_columns] = operations_df[count_columns].fillna(0).astype(int)

    operations_df["total_flights_observed"] = (
        operations_df["observed_arrivals_count"]
        + operations_df["observed_departures_count"]
    )

    operations_df["total_unique_aircraft_observed"] = (
        operations_df["unique_arrival_aircraft_count"]
        + operations_df["unique_departure_aircraft_count"]
    )

    operations_df = operations_df.sort_values(
        by=["airport_icao", "operation_hour_utc"]
    ).reset_index(drop=True)

    output_path = build_operations_mart_path(airport_icao, run_date)
    operations_df.to_csv(output_path, index=False)

    print(f"Rows written: {len(operations_df)}")
    print(f"Mart file saved to: {output_path}")
    print("Sample:")
    print(operations_df.head())


if __name__ == "__main__":
    main()