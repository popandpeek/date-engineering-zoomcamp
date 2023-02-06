### Question 1: number of rows = 447,770

from pathlib import Path
import pandas as pd
from prefect import flow,task
from prefect_gcp.cloud_storage import GcsBucket


@task(retries=3)
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read data from web into pandas DataFrame"""

    df = pd.read_csv(dataset_url)
    return df


@task(log_prints=True)
def clean(df: pd.DataFrame) -> pd.DataFrame:
    """Fix dtype issues"""
    df["lpep_pickup_datetime"] = pd.to_datetime(df["lpep_pickup_datetime"])
    df["lpep_dropoff_datetime"] = pd.to_datetime(df["lpep_dropoff_datetime"])
    print(df.head(2))
    print(f"columns: {df.dtypes}")
    print(f"rows: {len(df)}")
    return df


@task()
def write_local(df: pd.DataFrame, color: str, dataset_file: str) -> Path:
    """Write Datafram out locally as a parquet file"""
    path = Path(f"data/{color}/{dataset_file}.parquet")
    df.to_parquet(path, compression="gzip")
    return path


@task()
def write_gcs(path: Path) -> None:
    """Uploading local parquet file to GCS"""
    gcs_block = GcsBucket.load("prefect-zoomcamp-block")
    gcs_block.upload_from_path(
        from_path=f"{path}",
        to_path=path
    )
    return 


@flow
def etl_web_to_gcs() -> None:
    """The main ETL function"""
    color = "green"
    year = 2020
    month = 1
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"

    df = fetch(dataset_url)
    df_clean = clean(df)
    path = write_local(df_clean, color, dataset_file)
    write_gcs(path)



if __name__ == '__main__':
    etl_web_to_gcs()

### Question 2: Cron scheduling params: 0 5 1 * *

Flow
etl-parent-flow
Work Queue
default
Infrastructure
anonymous-60b6e2a7-ab2d-4e68-bd77-c15a365121ba
Schedule
At 05:00 AM on day 1 of the month (UTC)
Created
2023/02/04 04:40:32 PM
Last Updated
2023/02/05 01:42:03 PM

### Question 3: Loading data to BQ: 14,851,920

 etl_web_to_gcs.py
 
 from pathlib import Path
import pandas as pd
from prefect import flow,task
from prefect_gcp.cloud_storage import GcsBucket


@task(retries=3)
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read data from web into pandas DataFrame"""

    df = pd.read_csv(dataset_url)
    return df


@task(log_prints=True)
def clean(df: pd.DataFrame) -> pd.DataFrame:
    """Fix dtype issues"""
    df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"])
    df["tpep_dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"])
    print(df.head(2))
    print(f"columns: {df.dtypes}")
    print(f"rows: {len(df)}")
    return df


@task()
def write_local(df: pd.DataFrame, color: str, dataset_file: str) -> Path:
    """Write Datafram out locally as a parquet file"""
    path = Path(f"data/{color}/{dataset_file}.parquet")
    df.to_parquet(path, compression="gzip")
    return path


@task()
def write_gcs(path: Path) -> None:
    """Uploading local parquet file to GCS"""
    gcs_block = GcsBucket.load("prefect-zoomcamp-block")
    gcs_block.upload_from_path(
        from_path=f"{path}",
        to_path=path
    )
    return 


@flow
def etl_web_to_gcs() -> None:
    """The main ETL function"""
    color = "yellow"
    year = 2019
    months = [2, 3]
    for month in months:
        dataset_file = f"{color}_tripdata_{year}-{month:02}"
        dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"

        df = fetch(dataset_url)
        df_clean = clean(df)
        path = write_local(df_clean, color, dataset_file)
        write_gcs(path)



if __name__ == '__main__':
    etl_web_to_gcs()

etl_gcs_to_bq.py

from pathlib import Path
import pandas as pd
from prefect import flow,task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials


@task(log_prints=True)
def extract_from_gcs(color: str, year: int, month: int) -> Path:
    """Download trip data from GCS"""
    gcs_path = f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
    gcs_block = GcsBucket.load("prefect-zoomcamp-block")
    gcs_block.get_directory(from_path=gcs_path, local_path=f"data/")
    return Path(f"{gcs_path}")



@task(log_prints=True)
def write_bq(df: pd.DataFrame) -> None:
    """Write DataFrame to Big Query"""
    
    gcp_credentials_block = GcpCredentials.load("zoomcamp-gcp-creds")    

    df.to_gbq(
        destination_table="prefect_zoomcamp.rides",
        project_id="t-diagram-374918",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append",
    )


@flow()
def etl_gcs_to_bq():
    """Main ETL flow to load data into Big Query"""
    color = "yellow"
    year = 2019
    months = [2, 3]
    for month in months:
        path = extract_from_gcs(color, year, month)
        df = pd.read_parquet(path)
        write_bq(df)

if __name__ == "__main__":
    etl_gcs_to_bq()

### Question 4: Github storage block: 88,605

prefect deployment build etl_web_to_gcs.py:etl_web_to_gcs --name gh-block-test -sb github/zoomcamp-github --apply

###
### A complete description of a Prefect Deployment for flow 'etl-web-to-gcs'
###
name: gh-block-test
description: The main ETL function
version: 2cc7f635a23e98513747ebba219942c6
# The work queue that will handle this deployment's runs
work_queue_name: default
tags: []
parameters: {}
schedule: null
infra_overrides: {}
infrastructure:
  type: process
  env: {}
  labels: {}
  name: null
  command: null
  stream_output: true
  working_dir: null
  block_type_slug: process
  _block_type_slug: process

###
### DO NOT EDIT BELOW THIS LINE
###
flow_name: etl-web-to-gcs
manifest_path: null
storage:
  repository: https://github.com/popandpeek/date-engineering-zoomcamp/blob/main/week_2_workflow_orchestration/prefect/flows/02_gcp/etl_web_to_gcs.py
  reference: root
  access_token: '**********'
  _block_document_id: e45a489e-0ecf-4ea1-a427-0bfc09ef181f
  _block_document_name: zoomcamp-github
  _is_anonymous: false
  block_type_slug: github
  _block_type_slug: github
path: ''
entrypoint: etl_web_to_gcs.py:etl_web_to_gcs
parameter_openapi_schema:
  title: Parameters
  type: object
  properties: {}
  required: null
  definitions: null

### Question 5: Email or Slack Notifications: 514,392

email result:

Flow run etl-web-to-gcs/sage-ape entered state `Completed` at 2023-02-06T02:34:07.943917+00:00.
Flow ID: 001f5807-d056-4eed-9291-6632ec8864af
Flow run ID: 14b0af88-70e4-44b4-8921-c8fb7f7d335d
Flow run URL: https://app.prefect.cloud/account/9701a9e6-754e-4d95-9062-7d6ad678adf2/workspace/703894dd-2a89-4f2d-bf25-ecf37c2450db/flow-runs/flow-run/14b0af88-70e4-44b4-8921-c8fb7f7d335d
State message: All states completed.

### Question 6: Secrets: 8

from prefect.blocks.system import Secret

secret_block = Secret.load("prefect-fake-password")

# Access the stored secret
secret_block.get()

********

	
