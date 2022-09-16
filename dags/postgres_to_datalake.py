# Based on https://github.com/apache/airflow/blob/main/airflow/providers/google/cloud/example_dags/example_postgres_to_gcs.py

"""
Example DAG using PostgresToGoogleCloudStorageOperator.
This dag exports tables from a Postgres instance to Google Cloud Storage

Requirement: make sure the dag postgres_restore completed successfully at least once.
"""
import os
from datetime import datetime

from airflow import models
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator


CONN_ID="DVDRENTAL_DB"
GCS_DATA_LAKE_BUCKET=os.environ.get("GCS_DATA_LAKE_BUCKET")
FILE_PREFIX="dvdrental/{{ ds }}/"


with models.DAG(
    dag_id='postgres_to_datalake',
    start_date=datetime(2022, 1, 1),
    schedule_interval=None,
    #schedule_interval="0 1 * * *",
    catchup=False,
    tags=['example'],
) as dag:

    task_customer = PostgresToGCSOperator(
        task_id="get_customer",
        postgres_conn_id=CONN_ID,
        sql="select customer_id, email, store_id from customer;",
        bucket=GCS_DATA_LAKE_BUCKET,
        filename=FILE_PREFIX+"customer.csv",
        export_format='csv',
        gzip=False,
        use_server_side_cursor=True,
    )

    task_rental = PostgresToGCSOperator(
        task_id="get_rental",
        postgres_conn_id=CONN_ID,
        sql="select customer_id, inventory_id, rental_date from rental;",
        bucket=GCS_DATA_LAKE_BUCKET,
        filename=FILE_PREFIX+"rental.csv",
        export_format='csv',
        gzip=False,
        use_server_side_cursor=True,
    )

    task_film = PostgresToGCSOperator(
        task_id="get_film",
        postgres_conn_id=CONN_ID,
        sql="select film_id, title, description from film;",
        bucket=GCS_DATA_LAKE_BUCKET,
        filename=FILE_PREFIX+"film.csv",
        export_format='csv',
        gzip=False,
        use_server_side_cursor=True,
    )

    task_inventory = PostgresToGCSOperator(
        task_id="get_inventory",
        postgres_conn_id=CONN_ID,
        sql="select inventory_id, film_id, store_id from inventory;",
        bucket=GCS_DATA_LAKE_BUCKET,
        filename=FILE_PREFIX+"inventory.csv",
        export_format='csv',
        gzip=False,
        use_server_side_cursor=True,
    )

