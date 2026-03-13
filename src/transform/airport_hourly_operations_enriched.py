import pandas as pd

from src.config import DATA_DIR


def main() -> None:
    """
    Load the airport_hourly_operations mart table and the weather staging table,
    join them by airport and hour, and build the final enriched operations dataset.

    The script:
    - reads the hourly operations mart CSV
    - reads the weather staging CSV
    - joins both datasets on airport and UTC hour
    - derives simple weather and traffic flags
    - sorts the final result
    - saves the enriched dataset as a mart CSV file
    """
    marts_dir = DATA_DIR / "marts"
    staging_dir = DATA_DIR / "staging"
    marts_dir.mkdir(parents=True, exist_ok=True)

    operations_path = marts_dir / "airport_hourly_operations_LEMD_2026-03-07.csv"
    weather_path = staging_dir / "weather_LEMD_2026-03-07_table.csv"

    operations_df = pd.read_csv(
        operations_path,
        parse_dates=["operation_hour_utc"],
    )

    weather_df = pd.read_csv(
        weather_path,
        parse_dates=["weather_hour_utc"],
    )

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

    output_path = marts_dir / "airport_hourly_operations_enriched_LEMD_2026-03-07.csv"
    enriched_df.to_csv(output_path, index=False)

    print(f"Rows written: {len(enriched_df)}")
    print(f"Traffic threshold used: {traffic_threshold}")
    print(f"Enriched mart file saved to: {output_path}")
    print("Sample:")
    print(enriched_df.head())


if __name__ == "__main__":
    main()