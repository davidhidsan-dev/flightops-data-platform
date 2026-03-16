from pathlib import Path
import pandas as pd

from src.config import DATA_DIR
from src.utils.path_builders import build_published_dataset_path


def build_published_dataset() -> pd.DataFrame:
    """
    Read all airport_hourly_operations_enriched mart files and combine them
    into a single published DataFrame.
    """
    marts_dir = DATA_DIR / "marts"

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

    return published_df


def save_published_dataset(df: pd.DataFrame) -> Path:
    """
    Save the published DataFrame to the published layer and return the output path.
    """
    published_dir = DATA_DIR / "published"
    published_dir.mkdir(parents=True, exist_ok=True)

    output_path = build_published_dataset_path()
    df.to_csv(output_path, index=False)

    return output_path


def main() -> None:
    """
    Build and save the consolidated published dataset.
    """
    published_df = build_published_dataset()
    output_path = save_published_dataset(published_df)

    print(f"Rows written: {len(published_df)}")
    print(f"Published dataset saved to: {output_path}")
    print("Sample:")
    print(published_df.head())


if __name__ == "__main__":
    main()