from pendulum import datetime, duration
from io import StringIO

from airflow import Dag
from airflow.models.param import Param

import pandas as pd
import requests
from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

S3_CONN_ID = "minio_conn"
BUCKET = "fact-bucket"


@task
def create_bucket():
    s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)
    if not s3_hook.check_for_bucket(bucket_name=BUCKET):
        s3_hook.create_bucket(bucket_name=BUCKET)

@task
def upload_to_s3(cat_fact_number):
    # Instantiate
    s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)

    # Base URL
    url = "http://catfact.ninja/fact"

    # Grab data
    res = requests.get(url).json()

    # Convert JSON to csv
    res_df = pd.DataFrame.from_dict([res])
    res_csv = res_df.to_csv()

    # Take string, upload to S3 using predefined method
    s3_hook.load_string(
        res_csv,
        "cat_fact_{0}.csv".format(cat_fact_number),
        bucket_name=BUCKET,
        replace=True,
    )


@task
def process_data(cat_fact_number):
    """Reads data from S3, processes, and saves to new S3 file"""
    # Connect to S3
    s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)

    # Read data
    data = StringIO(
        s3_hook.read_key(
            key="cat_fact_{0}.csv".format(cat_fact_number), bucket_name=BUCKET
        )
    )
    df = pd.read_csv(data, sep=",")

    # Process data
    processed_data = df[["fact"]]
    print(processed_data)

    # Save processed data to CSV on S3
    s3_hook.load_string(
        processed_data.to_csv(),
        "cat_fact_{0}_processed.csv".format(cat_fact_number),
        bucket_name=BUCKET,
        replace=True,
    )


@dag(
    start_date=datetime(2021, 1, 1),
    max_active_runs=1,
    schedule="@daily",
    default_args={"retries": 1, "retry_delay": duration(minutes=1)},
    catchup=False,
    params={
        "names": Param(
            ["Linda", "Martha", "Thomas"],
            type="array",
            description="Define the list of names for which greetings should be generated in the logs."
            " Please have one name per line.",
            title="Names to greet",
        ),
        "english": Param(True, type="boolean", title="English"),
        "german": Param(True, type="boolean", title="German (Formal)"),
        "french": Param(True, type="boolean", title="French"),
    },

)
def intermediary_data_storage_dag():
    create_bucket() >> upload_to_s3(cat_fact_number=1) >> process_data(cat_fact_number=1)


intermediary_data_storage_dag()
