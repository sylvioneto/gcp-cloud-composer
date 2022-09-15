"""
Example Airflow DAG for Google BigQuery service testing dataset operations.
"""
import os
from datetime import datetime
from airflow import models
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

CONN_ID="DVDRENTAL_DB"
DATASET_NAME = f"dvdrental"
PROJECT_ID = os.environ.get("GCP_PROJECT")
GCS_DATA_LAKE_BUCKET = os.environ.get("GCS_DATA_LAKE_BUCKET")
FILE_PATH="dvdrental/{{ ds }}"


with models.DAG(
    dag_id='postgres_to_bigquery',
    schedule_interval="@once",
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=["example", "bigquery"],
) as dag:

    # Extract and load customer data
    get_customer = PostgresToGCSOperator(
        task_id="get_customer",
        postgres_conn_id=CONN_ID,
        sql="select customer_id, email, store_id from customer;",
        bucket=GCS_DATA_LAKE_BUCKET,
        filename=FILE_PATH+"/customer.csv",
        export_format='csv',
        gzip=False,
        use_server_side_cursor=True,
    )

    load_customer = GCSToBigQueryOperator(
        task_id='load_customer',
        bucket=GCS_DATA_LAKE_BUCKET,
        source_objects=[FILE_PATH+"/customer.csv"],
        skip_leading_rows=1,
        destination_project_dataset_table="{}.{}".format(DATASET_NAME, "customer"),
        schema_fields=[
            {'name': 'customer_id', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'customer_email', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'store_id', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        ],
        write_disposition='WRITE_TRUNCATE',
    )

    # Extract and load rental data
    get_rental = PostgresToGCSOperator(
        task_id="get_rental",
        postgres_conn_id=CONN_ID,
        sql="select customer_id, inventory_id, rental_date from rental;",
        bucket=GCS_DATA_LAKE_BUCKET,
        filename=FILE_PATH+"/rental.csv",
        export_format='csv',
        gzip=False,
        use_server_side_cursor=True,
    )

    load_rental = GCSToBigQueryOperator(
        task_id='load_rental',
        bucket=GCS_DATA_LAKE_BUCKET,
        source_objects=[FILE_PATH+"/rental.csv"],
        skip_leading_rows=1,
        destination_project_dataset_table="{}.{}".format(DATASET_NAME, "rental"),
        schema_fields=[
            {'name': 'customer_id', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'inventory_id', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'rental_date', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
        ],
        write_disposition='WRITE_TRUNCATE',
    )

    # Extract and load film data
    get_film = PostgresToGCSOperator(
        task_id="get_film",
        postgres_conn_id=CONN_ID,
        sql="select customer_id, inventory_id, rental_date from rental;",
        bucket=GCS_DATA_LAKE_BUCKET,
        filename=FILE_PATH+"/film.csv",
        export_format='csv',
        gzip=False,
        use_server_side_cursor=True,
    )

    load_film = GCSToBigQueryOperator(
        task_id='load_film',
        bucket=GCS_DATA_LAKE_BUCKET,
        source_objects=[FILE_PATH+"/film.csv"],
        skip_leading_rows=1,
        destination_project_dataset_table="{}.{}".format(DATASET_NAME, "film"),
        schema_fields=[
            {'name': 'film_id', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'title', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'description', 'type': 'STRING', 'mode': 'NULLABLE'},
        ],
        write_disposition='WRITE_TRUNCATE',
    )

get_customer >> load_customer
get_rental >> load_rental
get_film >> load_film
