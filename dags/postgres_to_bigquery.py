"""
Example Airflow DAG for Google BigQuery service testing dataset operations.
"""
import os
from datetime import datetime
from airflow import models
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator

CONN_ID="DVDRENTAL_DB"
DATASET_NAME = f"dvdrental"
PROJECT_ID = os.environ.get("GCP_PROJECT")
GCS_DATA_LAKE_BUCKET = os.environ.get("GCS_DATA_LAKE_BUCKET")
FILE_PREFIX="dvdrental/tests/"


with models.DAG(
    dag_id='postgres_to_bigquery',
    schedule_interval=None,
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=["example", "bigquery"],
) as dag:

    create_dataset = BigQueryCreateEmptyDatasetOperator(task_id="create_dataset", dataset_id=DATASET_NAME)

    # Extract and load customer data
    get_customer = PostgresToGCSOperator(
        task_id="get_customer",
        postgres_conn_id=CONN_ID,
        sql="select customer_id, email, store_id from customer;",
        bucket=GCS_DATA_LAKE_BUCKET,
        filename=FILE_PREFIX+"customer.csv",
        export_format='csv',
        gzip=False,
        use_server_side_cursor=True,
    )

    load_customer = GCSToBigQueryOperator(
        task_id='load_customer',
        bucket=GCS_DATA_LAKE_BUCKET,
        source_objects=[FILE_PREFIX+"customer.csv"],
        skip_leading_rows=1,
        destination_project_dataset_table="{}.{}".format(DATASET_NAME, "customer"),
        schema_fields=[
            {'name': 'customer_id', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'customer_email', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'store_id', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        ],
        write_disposition='WRITE_TRUNCATE',
    )

create_dataset >> get_customer >> load_customer
