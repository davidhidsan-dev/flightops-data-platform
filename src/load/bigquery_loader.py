import pandas as pd
from google.cloud import bigquery

from src.config import BIGQUERY_DATASET, BIGQUERY_PROJECT_ID
from src.utils.logger import get_logger
from src.utils.path_builders import build_published_dataset_path

logger = get_logger(__name__)


def load_airport_operations_to_bigquery(df: pd.DataFrame) -> str:
    """
    Load the published airport operations dataset into a BigQuery table.

    Returns:
        str: Fully qualified BigQuery table id.
    """
    df = df.copy()
    df["operation_hour_utc"] = pd.to_datetime(df["operation_hour_utc"], utc=True)

    table_id = (
        f"{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET}.airport_hourly_operations_enriched"
    )

    logger.info(f"Preparing BigQuery load table={table_id} rows={len(df)}")

    if df.empty:
        logger.warning("Published dataset is empty before BigQuery load")

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

    logger.info(f"BigQuery load completed table={table_id}")

    return table_id


def main() -> None:
    """
    Load the published airport operations dataset into BigQuery.
    """
    input_path = build_published_dataset_path()
    logger.info(f"Loading published dataset from {input_path}")

    df = pd.read_csv(input_path)

    table_id = load_airport_operations_to_bigquery(df)

    client = bigquery.Client(project=BIGQUERY_PROJECT_ID)
    table = client.get_table(table_id)

    logger.info(f"Loaded {table.num_rows} rows into {table_id}")


if __name__ == "__main__":
    main()