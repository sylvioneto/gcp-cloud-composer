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
        task_id="create_dataset",
        dataset_id=DATASET_NAME,
        location="US"
    )

    table_list = ["customer", "rental", "film", "inventory"]

    def load_table(table):
        object_name = "dvdrental/" + table + "/dt={{ ds }}/records.csv"

        return GCSToBigQueryOperator(
            task_id='bq_load_{}'.format(table),
            bucket=GCS_DATA_LAKE_BUCKET,
            source_objects=[object_name],
            skip_leading_rows=1,
            destination_project_dataset_table="{}.{}".format(DATASET_NAME, table),
            autodetect=True,
            write_disposition='WRITE_TRUNCATE',
        )

    for t in table_list:
        create_dataset >> load_table(t)

trigger_datalake_dag >> create_dataset
