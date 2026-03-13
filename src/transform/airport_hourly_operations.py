import pandas as pd

from src.config import DATA_DIR


def main() -> None:
    """
    Load arrivals and departures staging files, aggregate them by airport and hour,
    and build the airport_hourly_operations mart table.

    The script:
    - reads arrivals and departures staging CSV files
    - aggregates arrivals by airport and observed arrival hour
    - aggregates departures by airport and observed departure hour
    - standardizes both aggregations to a common schema
    - outer joins both hourly tables
    - fills missing count values with zero
    - derives final operational metrics
    - saves the result as a mart CSV file
    """
    staging_dir = DATA_DIR / "staging"
    marts_dir = DATA_DIR / "marts"
    marts_dir.mkdir(parents=True, exist_ok=True)

    arrivals_path = staging_dir / "arrivals_LEMD_2026-03-07_table.csv"
    departures_path = staging_dir / "departures_LEMD_2026-03-07_table.csv"

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

    output_path = marts_dir / "airport_hourly_operations_LEMD_2026-03-07.csv"
    operations_df.to_csv(output_path, index=False)

    print(f"Rows written: {len(operations_df)}")
    print(f"Mart file saved to: {output_path}")
    print("Sample:")
    print(operations_df.head())


if __name__ == "__main__":
    main()