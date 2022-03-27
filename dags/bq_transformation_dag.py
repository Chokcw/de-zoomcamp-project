import os
import logging

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator, BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator



PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'olist')

DATASET = "olist"
DATASET_LIST = {
    'orders': {'partitioning': 'order_purchase_timestamp'}, 
    'customers': None,
    'order_items': None,
    'products': None,
} # may choose to cluster instead of partition for order items
INPUT_PART = "raw"
INPUT_FILETYPE = "parquet"

default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

def read_file(filepath):
    with open(filepath, "r") as f:
        return f.read()


# NOTE: DAG declaration - using a Context Manager (an implicit way)
with DAG(
    dag_id="bq_transformation_dag",
    schedule_interval="@once",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['dtc-de'],
) as dag:

    CREATE_AGGREGATED_TBL_QUERY = (
        read_file("sql/aggregate.sql".format(BIGQUERY_DATASET=BIGQUERY_DATASET, DATASET=DATASET))
    )

    # Create a partitioned table from external table
    bq_create_staging_table_job = BigQueryInsertJobOperator(
        task_id="bq_create_aggregated_table",
        configuration={
            "query": {
                "query": CREATE_AGGREGATED_TBL_QUERY,
                "useLegacySql": False,
            }
        }
    )

    bq_create_staging_table_job