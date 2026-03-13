import pandas as pd
from google.cloud import bigquery

from src.config import DATA_DIR, BIGQUERY_PROJECT_ID, BIGQUERY_DATASET


def main() -> None:
    """
    Load the published airport operations dataset into a BigQuery table.

    The script:
    - reads the published CSV dataset
    - parses datetime columns
    - loads the data into BigQuery
    - replaces the destination table on each run
    """
    input_path = DATA_DIR / "published" / "airport_hourly_operations_enriched.csv"

    df = pd.read_csv(input_path)

    df["operation_hour_utc"] = pd.to_datetime(df["operation_hour_utc"], utc=True)

    table_id = (
        f"{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET}.airport_hourly_operations_enriched"
    )

    client = bigquery.Client(project=BIGQUERY_PROJECT_ID)

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        autodetect=True,
    )

    job = client.load_table_from_dataframe(
        df,
        table_id,
        job_config=job_config,
    )

    job.result()

    table = client.get_table(table_id)

    print(f"Loaded {table.num_rows} rows into {table_id}")


if __name__ == "__main__":
    main()