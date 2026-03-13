from pathlib import Path
import pandas as pd

from src.config import DATA_DIR


def main() -> None:
    """
    Run basic data quality checks on the published airport operations dataset.

    The script validates:
    - non-null values in key columns
    - uniqueness at airport-hour grain
    - non-negative count metrics
    - consistency of derived total flight counts
    """
    published_path = DATA_DIR / "published" / "airport_hourly_operations_enriched.csv"

    df = pd.read_csv(published_path, parse_dates=["operation_hour_utc"])

    print(f"Rows loaded: {len(df)}")

    # Check 1: airport_icao must not be null
    null_airport_count = df["airport_icao"].isna().sum()
    print(f"Null airport_icao rows: {null_airport_count}")
    if null_airport_count > 0:
        raise ValueError("Data quality check failed: airport_icao contains null values.")

    # Check 2: operation_hour_utc must not be null
    null_hour_count = df["operation_hour_utc"].isna().sum()
    print(f"Null operation_hour_utc rows: {null_hour_count}")
    if null_hour_count > 0:
        raise ValueError("Data quality check failed: operation_hour_utc contains null values.")

    # Check 3: no duplicates at airport-hour grain
    duplicate_count = df.duplicated(subset=["airport_icao", "operation_hour_utc"]).sum()
    print(f"Duplicate airport-hour rows: {duplicate_count}")
    if duplicate_count > 0:
        raise ValueError("Data quality check failed: duplicate airport-hour rows found.")

    # Check 4: count metrics must be non-negative
    count_columns = [
        "observed_arrivals_count",
        "observed_departures_count",
        "total_flights_observed",
        "unique_arrival_aircraft_count",
        "unique_departure_aircraft_count",
        "total_unique_aircraft_observed",
    ]

    negative_counts = (df[count_columns] < 0).sum().sum()
    print(f"Negative count values: {negative_counts}")
    if negative_counts > 0:
        raise ValueError("Data quality check failed: negative values found in count columns.")

    # Check 5: total_flights_observed must equal arrivals + departures
    expected_total = (
        df["observed_arrivals_count"] + df["observed_departures_count"]
    )
    invalid_total_count = (df["total_flights_observed"] != expected_total).sum()
    print(f"Invalid total_flights_observed rows: {invalid_total_count}")
    if invalid_total_count > 0:
        raise ValueError(
            "Data quality check failed: total_flights_observed does not match arrivals + departures."
        )

    print("All data quality checks passed successfully.")


if __name__ == "__main__":
    main()