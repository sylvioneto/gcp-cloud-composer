# Based on https://github.com/apache/airflow/blob/main/airflow/providers/google/cloud/example_dags/example_postgres_to_gcs.py

"""
Example DAG using PostgresToGoogleCloudStorageOperator.
This dag exports tables from a Postgres instance to Google Cloud Storage

Requirement: make sure the dag postgres_restore completed successfully at least once.
"""
import os
from datetime import datetime
import string

from airflow import models
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
from airflow.operators.dummy import DummyOperator


CONN_ID = "DVDRENTAL_DB"
GCS_DATA_LAKE_BUCKET = os.environ.get("GCS_DATA_LAKE_BUCKET")
FILE_PREFIX = "dvdrental/{{ ds }}/"


with models.DAG(
    dag_id='postgres_to_datalake',
    start_date=datetime(2022, 1, 1),
    schedule_interval="0 1 * * *",
    catchup=False,
    tags=['cloudsql', 'postgres', 'gcs'],
) as dag:

    table_list = ["customer", "rental", "film", "inventory"]

    def extract_table(table: string):
        return PostgresToGCSOperator(
            task_id="extract_table_{}".format(table),
            postgres_conn_id=CONN_ID,
            sql="select * from {};".format(table),
            bucket=GCS_DATA_LAKE_BUCKET,
            filename=FILE_PREFIX+"{}.csv".format(table),
            export_format='csv',
            gzip=False,
            use_server_side_cursor=True,
        )

    task_root = DummyOperator(
        task_id='group_tasks',
        dag=dag)

    for t in table_list:
        task_root >> extract_table(t)
