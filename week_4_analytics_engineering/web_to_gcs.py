import io
import os
import requests
import pandas as pd
import pyarrow
from google.cloud import storage
from pathlib import Path


init_url = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/'
BUCKET = "zoomcamp-week4"


def write_local(url: str, dataset_file: str) -> Path:
    """Write DataFrame out locally as parquet file"""
    path = Path(f"{dataset_file}")
    with open(dataset_file, "wb") as f:
        r = requests.get(url)
        f.write(r.content)
    return path


def upload_to_gcs(bucket, object_name, local_file):
    client = storage.Client()
    bucket = client.bucket(bucket)
    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)


def web_to_gcs(year, service):
    for i in range(12):
        
        # sets the month part of the file_name string
        month = '0'+str(i+1)
        month = month[-2:]

        # csv file_name 
        file_name = service + '_tripdata_' + year + '-' + month + '.csv.gz'

        # download it using requests via a pandas df
        request_url = init_url + service + '/' + file_name
        print(request_url)

        # upload it to gcs 
        upload_path = write_local(request_url, file_name)
        upload_to_gcs(BUCKET, f"{service}/{year}/{file_name}", file_name)
        print(f"GCS: {service}/{file_name}")


web_to_gcs('2019', 'fhv')

