"""
This example shows how to transform your data with a BigQuery job.
"""
import os
from datetime import datetime
from airflow import models
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator


PROJECT_ID = os.environ.get("GCP_PROJECT")
DATASET_NAME = "dvdrental"


with models.DAG(
    dag_id='bigquery_transform',
    schedule_interval="@once",
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=["example", "bigquery"],
) as dag:

    CUSTOMER_RENTALS_QUERY = (
    '''WITH rental_movies AS(
            SELECT
                r.customer_id,
                r.rental_date,
                r.return_date,
                f.title
            FROM `syl-data-analytics.dvdrental.rental` r
            JOIN `syl-data-analytics.dvdrental.inventory` i ON (r.inventory_id=i.inventory_id)
            JOIN `syl-data-analytics.dvdrental.film` f ON (i.film_id=f.film_id))
        SELECT
            c.customer_id,
            c.email as customer_email,
            ARRAY_AGG(
                STRUCT(
                    rm.title as film_title,
                    rm.rental_date,
                    rm.return_date
                )
            ) AS rentals
        FROM
            `syl-data-analytics.dvdrental.customer` c
        LEFT JOIN
            rental_movies rm
        ON
            (rm.customer_id=c.customer_id)
        GROUP BY
            1,
            2
    '''.format(PROJECT_ID, DATASET_NAME)
    )

    create_customer_rentals = BigQueryInsertJobOperator(
        task_id="create_customer_rentals",
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
    )
