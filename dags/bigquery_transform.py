"""
This example shows how to transform your data with a BigQuery job.
"""
import os
from datetime import datetime
from airflow import models
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator


PROJECT_ID = os.environ.get("GCP_PROJECT")
DATASET_NAME = f"dvdrental"


with models.DAG(
    dag_id='bigquery_transform',
    schedule_interval="@once",
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=["example", "bigquery"],
) as dag:

    CUSTOMER_RENTALS_QUERY = (
        '''
        WITH rental_movies AS(
            SELECT
                r.customer_id,
                r.rental_date,
                f.title
            FROM `{0}.{1}.rental` r
            JOIN `{0}.{1}.inventory` i ON (r.inventory_id=i.inventory_id)
            JOIN `{0}.{1}.film` f ON (i.film_id=f.film_id))
        SELECT
            c.customer_id,
            c.customer_email,
            ARRAY_AGG(
                STRUCT(
                    rm.rental_date,
                    rm.title as film_title
                )
            ) AS rentals
        FROM
            `{0}.{1}.customer` c
        LEFT JOIN
            rental_movies rm
        ON
            (rm.customer_id=c.customer_id)
        GROUP BY
            c.customer_id,
            c.customer_email
        '''.format(PROJECT_ID, DATASET_NAME)
    )

    bq_transform_job = BigQueryInsertJobOperator(
        task_id="bq_transform_job",
        configuration={
            "query": {
                "query": CUSTOMER_RENTALS_QUERY,
                "useLegacySql": False,
                "destinationTable": {
                    "projectId": PROJECT_ID,
                    "datasetId": DATASET_NAME,
                    "tableId": "customer_rentals",
                },
                "createDisposition": "CREATE_IF_NEEDED",
                "writeDisposition": "WRITE_TRUNCATE",
            }
        },
        location="US",
    )
