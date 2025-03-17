from datetime import timedelta

import dependencies.flattener.utils as utils
import dependencies.flattener.constants as constants
import dependencies.flattener.bq as bq
import dependencies.flattener.flatten as flatten


import airflow # type: ignore
from airflow import DAG  # type: ignore
from airflow.decorators import task  # type: ignore
from airflow.utils.dates import days_ago  # type: ignore

default_args = {
    'start_date': airflow.utils.dates.days_ago(1),
    'retries': 3,
    'retry_delay': timedelta(seconds=15),
    'owner': 'airflow'
}

dag = DAG(
    'flattener',
    default_args=default_args,
    description='Pipeline for flattening Connect study data',
    start_date=days_ago(1)
)

@task()
def check_api_health() -> None:
    """
    Task to verify flattener container is up.
    """
    utils.logger.info("Checking flattening API status")
    try:
        result = utils.check_service_health(constants.PROCESSOR_ENDPOINT)
        if result['status'] != 'healthy':
            error_msg = f"API health check failed. Status: {result['status']}"
            utils.logger.error(error_msg)
            raise Exception(error_msg)

        utils.logger.info(f"The API is healthy! Response: {result}")
    except Exception as e:
        raise Exception(f"API health check failed: {str(e)}")

@task()
def table_to_parquet(table_name: str) -> None:
    try:
        bq.bq_to_parquet(constants.BQ_PROJECT_ID, constants.BQ_RAW_DATASET, table_name, constants.GCS_FLATTENED_BUCKET)
    except Exception as e:
        raise Exception(f"Unable to save {table_name} as Parquet")

@task()
def flatten_parquet(table_name: str) -> None:
    try:
        flatten.flatten_parquet(table_name, constants.GCS_FLATTENED_BUCKET)
    except Exception as e:
        raise Exception(f"Unable to flatten {table_name} Parquet files: {str(e)}")

@task()
def parquet_to_table(table_name: str) -> None:
    try:
        bq.parquet_to_bq(constants.BQ_PROJECT_ID, constants.BQ_FLATTENED_DATASET, table_name, constants.GCS_FLATTENED_BUCKET)
    except Exception as e:
        raise Exception(f"Error moving {table_name} Parquet file into BigQuery: {str(e)}")

with dag:
    api_health_check = check_api_health()
    bq_to_parquet = table_to_parquet.expand(table_name=constants.RAW_TABLES)
    flatten_files = flatten_parquet.expand(table_name=constants.RAW_TABLES)
    parquet_to_bq = parquet_to_table.expand(table_name=constants.RAW_TABLES)

    api_health_check >> bq_to_parquet >> flatten_files >> parquet_to_bq