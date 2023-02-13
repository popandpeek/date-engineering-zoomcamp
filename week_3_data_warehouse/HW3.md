## Question 1 - What is the count for fhv vehicle records for year 2019? 
## Answer - 43,244,696

import io
import os
import requests
import pandas as pd
import pyarrow
from google.cloud import storage
from pathlib import Path


init_url = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/'
BUCKET = "zoomcamp-week3"


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
        request_url = init_url + file_name

        # upload it to gcs 
        upload_path = write_local(request_url, file_name)
        upload_to_gcs(BUCKET, f"{file_name}", file_name)
        print(f"GCS: {service}/{file_name}")


web_to_gcs('2019', 'fhv')

CREATE OR REPLACE EXTERNAL TABLE `*zoomcamp_fh_data.fhv_2019`
OPTIONS (
  format = 'CSV',
  uris = ['*/fhv_tripdata_2019-*.csv.gz']
);

## Question 2 - What is the estimated amount of data that will be read when this query is executed on the External Table and the Table?
## Answer - 0 MB for the External Table and 317.94MB for the BQ Table

SELECT COUNT(DISTINCT Affiliated_base_number) FROM `t-diagram-374918.zoomcamp_fh_data.fhv_external`;

SELECT COUNT(DISTINCT Affiliated_base_number) FROM `t-diagram-374918.zoomcamp_fh_data.fhv_gq`;

## Question 3 - How many records have both a blank (null) PUlocationID and DOlocationID in the entire dataset?
## Answer - 717,748

SELECT COUNT(*)
FROM `t-diagram-374918.zoomcamp_fh_data.fhv_gq`
WHERE PUlocationID IS NULL AND DOlocationID IS NULL;

## Question 4 - What is the best strategy to optimize the table if query always filter by pickup_datetime and order by affiliated_base_number?
## Answer - Partition by pickup_datetime Cluster on affiliated_base_number

Cluster on pickup_datetime Cluster on affiliated_base_number = 1.92 GB
Partition by pickup_datetime Cluster on affiliated_base_number = 183.47 MB
Partition by pickup_datetime Partition by affiliated_base_number = ILLEGAL
Partition by affiliated_base_number Cluster on pickup_datetime = ILLEGAL

CREATE OR REPLACE TABLE `t-diagram-374918.zoomcamp_fh_data.fhv_partitioned_clustered`  
PARTITION BY DATE(pickup_datetime)
CLUSTER BY affiliated_base_number  AS
SELECT * FROM `t-diagram-374918.zoomcamp_fh_data.fhv_gq`

## Question 5 - Implement the optimized solution you chose for question 4. Write a query to retrieve the distinct affiliated_base_number between pickup_datetime 2019/03/01 and 2019/03/31 (inclusive).
## Answer - 647.87 MB for non-partitioned table and 23.06 MB for the partitioned table

SELECT COUNT(DISTINCT affiliated_base_number)
FROM `t-diagram-374918.zoomcamp_fh_data.fhv_gq` 
WHERE DATE(pickup_datetime) BETWEEN "2019-03-01" AND "2019-03-31";

SELECT COUNT(DISTINCT affiliated_base_number)
FROM `t-diagram-374918.zoomcamp_fh_data.fhv_partitioned_clustered` 
WHERE DATE(pickup_datetime) BETWEEN "2019-03-1" AND "2019-03-31";

## Question 6 - Where is the data stored in the External Table you created?
## Answer - GCP Bucket

## Question 7 - It is best practice in Big Query to always cluster your data:
## Answer - True