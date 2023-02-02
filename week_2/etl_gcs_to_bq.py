from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials


@task(retries=3)
def extract_from_gcs(color: str, year: int, month: int) -> Path:
    """Download trip data from GCS"""
    gcs_path = f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.get_directory(from_path=gcs_path, local_path=f"../data/")
    return Path(f"../data/{gcs_path}")


@task()
def read_data(path: Path) -> pd.DataFrame:
    """Data read from the parquet file"""
    df = pd.read_parquet(path)
    return df

@task()
def write_bq(df: pd.DataFrame) -> None:
    """Write DataFrame to BiqQuery"""

    gcp_credentials_block = GcpCredentials.load("zoom-gcp-creds")

    df.to_gbq(
        destination_table="dezoomcamp.rides",
        project_id="datazoomcamp-375017",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append",
    )


@flow(log_prints=True)
def etl_gcs_to_bq(months: list[int]=[2,3], year=2019, color="yellow"):
    """Main ETL flow to load data from GCP bucket storage into Big Query"""
    total_rows = 0
    for month in months:
        path = extract_from_gcs(color, year, month)
        df = read_data(path)
        write_bq(df)
        total_rows += len(df)
    print("total rows processed:", total_rows)
        



if __name__ == "__main__":
    etl_gcs_to_bq([2,3], 2019, "yellow")
