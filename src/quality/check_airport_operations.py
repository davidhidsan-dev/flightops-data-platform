import pandas as pd

from src.utils.logger import get_logger
from src.utils.path_builders import build_published_dataset_path

logger = get_logger(__name__)


def run_airport_operations_checks(df: pd.DataFrame) -> None:
    """
    Run basic data quality checks on the airport operations dataset.

    The function validates:
    - non-null values in key columns
    - uniqueness at airport-hour grain
    - non-negative count metrics
    - consistency of derived total flight counts
    """
    logger.info(f"Rows loaded={len(df)}")
    logger.info("Starting data quality checks for published dataset")

    # Check 1: airport_icao must not be null
    null_airport_count = df["airport_icao"].isna().sum()
    logger.info(f"Null airport_icao rows={null_airport_count}")
    if null_airport_count > 0:
        logger.error("Data quality check failed: airport_icao contains null values.")
        raise ValueError("Data quality check failed: airport_icao contains null values.")
    logger.info("Check passed: airport_icao has no null values")

    # Check 2: operation_hour_utc must not be null
    null_hour_count = df["operation_hour_utc"].isna().sum()
    logger.info(f"Null operation_hour_utc rows={null_hour_count}")
    if null_hour_count > 0:
        logger.error("Data quality check failed: operation_hour_utc contains null values.")
        raise ValueError("Data quality check failed: operation_hour_utc contains null values.")
    logger.info("Check passed: operation_hour_utc has no null values")

    # Check 3: no duplicates at airport-hour grain
    duplicate_count = df.duplicated(subset=["airport_icao", "operation_hour_utc"]).sum()
    logger.info(f"Duplicate airport-hour rows={duplicate_count}")
    if duplicate_count > 0:
        logger.error("Data quality check failed: duplicate airport-hour rows found.")
        raise ValueError("Data quality check failed: duplicate airport-hour rows found.")
    logger.info("Check passed: airport-hour grain has no duplicates")

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
    logger.info(f"Negative count values={negative_counts}")
    if negative_counts > 0:
        logger.error("Data quality check failed: negative values found in count columns.")
        raise ValueError("Data quality check failed: negative values found in count columns.")
    logger.info("Check passed: count columns contain no negative values")

    # Check 5: total_flights_observed must equal arrivals + departures
    expected_total = (
        df["observed_arrivals_count"] + df["observed_departures_count"]
    )
    invalid_total_count = (df["total_flights_observed"] != expected_total).sum()
    logger.info(f"Invalid total_flights_observed rows={invalid_total_count}")
    if invalid_total_count > 0:
        logger.error(
            "Data quality check failed: total_flights_observed does not match arrivals + departures."
        )
        raise ValueError(
            "Data quality check failed: total_flights_observed does not match arrivals + departures."
        )
    logger.info("Check passed: total_flights_observed matches arrivals plus departures")

    logger.info("All data quality checks passed successfully")


def main() -> None:
    """
    Load the published airport operations dataset and run data quality checks.
    """
    published_path = build_published_dataset_path()
    logger.info(f"Loading published dataset from {published_path}")

    df = pd.read_csv(published_path, parse_dates=["operation_hour_utc"])

    run_airport_operations_checks(df)


if __name__ == "__main__":
    main()