import pandas as pd
from prefect import flow, task
from pathlib import Path
from prefect_gcp.cloud_storage import GcsBucket


@task()
def fetch_data(url):
    df = pd.read_csv(url)
    return df


@task()
def clean_data(data):
    """Fix dtype issues"""
    data["pickup_datetime"] = pd.to_datetime(data["pickup_datetime"])
    data["dropOff_datetime"] = pd.to_datetime(data["dropOff_datetime"])
    print(data.head(2))
    print(f"columns: {data.dtypes}")
    print(f"rows: {len(data)}")
    return data

@task()
def write_local(df: pd.DataFrame, dataset_file: str) -> Path:
    """Write DataFrame out locally as parquet file"""
    path = Path(f"data_fhv/{dataset_file}.csv.gz")
    df.to_parquet(path, compression="gzip")
    return path


@task()
def write_gcs(path: Path) -> None:
    """Upload local parquet file to GCS"""
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.upload_from_path(from_path=path, to_path=path)
    return

@flow(log_prints=True)
def load_fhV_data_to_gcs_buckets():
    year = 2019
    for month in range(1, 13):
        dataset_file = f"fhv_tripdata_{year}-{month:02}"
        url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_{year}-{month:02}.csv.gz"
        print(url)
        df = fetch_data(url)
        print(df.head())
        cleaned_data = clean_data(df)
        path = write_local(cleaned_data, dataset_file)
        write_gcs(path)

if __name__ == "__main__":
    load_fhV_data_to_gcs_buckets()



