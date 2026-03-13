from pathlib import Path
import pandas as pd

from src.config import DATA_DIR


def main() -> None:
    """
    Read all airport_hourly_operations_enriched mart files, combine them into a
    single published dataset, and save the consolidated CSV output.

    The script:
    - finds all enriched mart CSV files
    - loads and concatenates them
    - derives operation_date from operation_hour_utc
    - removes duplicate rows
    - sorts the final dataset
    - saves the published CSV file
    """
    marts_dir = DATA_DIR / "marts"
    published_dir = DATA_DIR / "published"
    published_dir.mkdir(parents=True, exist_ok=True)

    input_files = sorted(
        marts_dir.glob("airport_hourly_operations_enriched_*.csv")
    )

    if not input_files:
        raise FileNotFoundError(
            "No enriched mart files found in data/marts/."
        )

    dataframes = []

    for file_path in input_files:
        df = pd.read_csv(
            file_path,
            parse_dates=["operation_hour_utc"],
        )
        dataframes.append(df)

    published_df = pd.concat(dataframes, ignore_index=True)

    published_df["operation_date"] = published_df["operation_hour_utc"].dt.date

    published_df = published_df.drop_duplicates()

    published_df = published_df.sort_values(
        by=["airport_icao", "operation_hour_utc"]
    ).reset_index(drop=True)

    output_path = published_dir / "airport_hourly_operations_enriched.csv"
    published_df.to_csv(output_path, index=False)

    print(f"Files processed: {len(input_files)}")
    print(f"Rows written: {len(published_df)}")
    print(f"Published dataset saved to: {output_path}")
    print("Sample:")
    print(published_df.head())


if __name__ == "__main__":
    main()