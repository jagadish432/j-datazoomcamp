import io
import os
import requests
import pandas as pd
import pyarrow
import yaml
import time
from yaml.loader import SafeLoader
from google.cloud import storage

"""
Pre-reqs: 
1. `pip install pandas pyarrow google-cloud-storage`
2. Set GOOGLE_APPLICATION_CREDENTIALS to your project/service-account key
3. Set GCP_GCS_BUCKET as your bucket or change default value of BUCKET
"""

# services = ['fhv','green','yellow']
init_url = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/'
# switch out the bucketname
BUCKET = os.environ.get("GCP_GCS_BUCKET", "dtc_data_lake_datazoomcamp-375017")


def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    """
    # # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # # (Ref: https://github.com/googleapis/python-storage/issues/74)
    # storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    # storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB

    client = storage.Client()
    bucket = client.bucket(bucket)
    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)


def get_schema(service: str) -> dict:
   schema_dictionary = load_schema_dictionary(service)
   return schema_dictionary


def web_to_gcs(year, service):
    for i in range(12):
        
        # sets the month part of the file_name string
        month = '0'+str(i+1)
        month = month[-2:]

        # csv file_name 
        file_name = service + "/" + service + '_tripdata_' + year + '-' + month + '.csv.gz'

        # download it using requests via a pandas df
        request_url = init_url + file_name
        print("request url - ", request_url)

        # Get schema
        schema = get_schema(service=service)
        # read it back into a parquet file
        df_data = pd.read_csv(request_url, dtype=schema)
        print(df_data.info())

        if service == "yellow":
            df_data["tpep_pickup_datetime"] = pd.to_datetime(df_data["tpep_pickup_datetime"])
            df_data["tpep_dropoff_datetime"] = pd.to_datetime(df_data["tpep_dropoff_datetime"])
        elif service == "green":
            df_data["lpep_pickup_datetime"] = pd.to_datetime(df_data["lpep_pickup_datetime"])
            df_data["lpep_dropoff_datetime"] = pd.to_datetime(df_data["lpep_dropoff_datetime"])
        elif service == "fhv":
            df_data["pickup_datetime"] = pd.to_datetime(df_data["pickup_datetime"])
            df_data["dropOff_datetime"] = pd.to_datetime(df_data["dropOff_datetime"])

        file_name = file_name.replace('.csv', '.parquet')
        df_data.to_parquet(file_name, engine='pyarrow')
        print(f"Parquet: {file_name}")

        # upload it to gcs 
        upload_to_gcs(BUCKET, f"{file_name}", file_name)
        print(f"GCS: {service}/{file_name}")


# loading schema for respective service type - green/yellow/fhv
def load_schema_dictionary(service: str):
    schema = None
    with open('schema.yml') as f:
        schema = yaml.load(f, Loader=SafeLoader)
        print(schema)
        print(type(schema))
        schema = schema[service]
    return schema



# print("loading Green trips Data")
# web_to_gcs('2019', 'green')
# web_to_gcs('2020', 'green')

# print("loading Yello trips Data")
# web_to_gcs('2019', 'yellow')
# web_to_gcs('2020', 'yellow')

print("loading FHV trips Data")
web_to_gcs('2019', 'fhv')