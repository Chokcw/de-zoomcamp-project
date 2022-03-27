import os
import logging

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator, BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'trips_data_all')

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

# NOTE: DAG declaration - using a Context Manager (an implicit way)
with DAG(
    dag_id="bq_transformation_dag",
    schedule_interval="@once",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['dtc-de'],
) as dag:

    for file, config in DATASET_LIST.items():

        CREATE_BQ_PARTITIONED_TBL_QUERY = (
            f"CREATE OR REPLACE TABLE {BIGQUERY_DATASET}.{DATASET}_{file} \
            PARTITION BY DATE(order_purchase_timestamp) \
            AS \
            SELECT * FROM {BIGQUERY_DATASET}.{DATASET}_orders_external_table;"
        )

        CREATE_BQ_NORMAL_TBL_QUERY = (
            f"CREATE OR REPLACE TABLE {BIGQUERY_DATASET}.{DATASET}_{file} \
            AS \
            SELECT * FROM {BIGQUERY_DATASET}.{DATASET}_customers_external_table;"
        )

        if config:
            ds_col = config['partitioning']
            query = CREATE_BQ_PARTITIONED_TBL_QUERY
            task_id = f"bq_create_{DATASET}_{file}_paritioned_table_task"
        
        else:
            query = CREATE_BQ_NORMAL_TBL_QUERY
            task_id = f"bq_create_{DATASET}_{file}_normal_table_task"

        move_files_gcs_task = GCSToGCSOperator(
            task_id=f'move_{DATASET}_{file}_files_task',
            source_bucket=BUCKET,
            source_object=f'{INPUT_PART}/{DATASET}_{file}_*.{INPUT_FILETYPE}',
            destination_bucket=BUCKET,
            destination_object=f'{file}/{DATASET}_{file}',
            move_object=False
        )

        bigquery_external_table_task = BigQueryCreateExternalTableOperator(
            task_id=f"bq_{DATASET}_{file}_external_table_task",
            table_resource={
                "tableReference": {
                    "projectId": PROJECT_ID,
                    "datasetId": BIGQUERY_DATASET,
                    "tableId": f"{DATASET}_{file}_external_table",
                },
                "externalDataConfiguration": {
                    "autodetect": "True",
                    "sourceFormat": f"{INPUT_FILETYPE.upper()}",
                    "sourceUris": [f"gs://{BUCKET}/{file}/*"],
                },
            },
        )

        # Create a partitioned table from external table
        bq_create_staging_table_job = BigQueryInsertJobOperator(
            task_id=task_id,
            configuration={
                "query": {
                    "query": query,
                    "useLegacySql": False,
                }
            }
        )

    move_files_gcs_task >> bigquery_external_table_task >> bq_create_staging_table_job