"""
This example shows how to trigger a Datalake dag, and then load the data into BigQuery.
"""
import os
from datetime import datetime
from airflow import models
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator

CONN_ID = "DVDRENTAL_DB"
DATASET_NAME = "dvdrental"
PROJECT_ID = os.environ.get("GCP_PROJECT")
GCS_DATA_LAKE_BUCKET = os.environ.get("GCS_DATA_LAKE_BUCKET")
BUCKET_PREFIX = "dvdrental/"


with models.DAG(
    dag_id='datalake_to_dw',
    schedule_interval="@once",
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=["example", "bigquery"],
) as dag:

    trigger_datalake_dag = TriggerDagRunOperator(
        task_id="trigger_datalake_dag",
        trigger_dag_id="postgres_to_datalake",
        wait_for_completion=True,
        poke_interval=10,  # seconds
        execution_date="{{ execution_date }}"
    )

    create_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create_dataset", dataset_id=DATASET_NAME)

    # example with autodetect schema
    bq_load_customer = GCSToBigQueryOperator(
        task_id='bq_load_customer',
        bucket=GCS_DATA_LAKE_BUCKET,
        source_objects=[BUCKET_PREFIX+"customer.csv"],
        skip_leading_rows=1,
        destination_project_dataset_table="{}.{}".format(
            DATASET_NAME, "customer"),
        autodetect=True,
        write_disposition='WRITE_TRUNCATE',
    )

    bq_load_rental = GCSToBigQueryOperator(
        task_id='bq_load_rental',
        bucket=GCS_DATA_LAKE_BUCKET,
        source_objects=[BUCKET_PREFIX+"rental.csv"],
        skip_leading_rows=1,
        destination_project_dataset_table="{}.{}".format(DATASET_NAME, "rental"),
        autodetect=True,
        write_disposition='WRITE_TRUNCATE',
    )

    bq_load_film = GCSToBigQueryOperator(
        task_id='bq_load_film',
        bucket=GCS_DATA_LAKE_BUCKET,
        source_objects=[BUCKET_PREFIX+"film.csv"],
        skip_leading_rows=1,
        destination_project_dataset_table="{}.{}".format(DATASET_NAME, "film"),
        autodetect=True,
        write_disposition='WRITE_TRUNCATE',
    )

    # example with Schema Fields
    bq_load_inventory = GCSToBigQueryOperator(
        task_id='bq_load_inventory',
        bucket=GCS_DATA_LAKE_BUCKET,
        source_objects=[BUCKET_PREFIX+"inventory.csv"],
        skip_leading_rows=1,
        destination_project_dataset_table="{}.{}".format(
            DATASET_NAME, "inventory"),
        schema_fields=[
            {'name': 'inventory_id', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'film_id', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'store_id', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        ],
        write_disposition='WRITE_TRUNCATE',
    )

# Task hierarchy
trigger_datalake_dag >> create_dataset >> [
    bq_load_customer, bq_load_rental, bq_load_film, bq_load_inventory]
