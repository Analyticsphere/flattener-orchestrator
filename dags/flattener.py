from datetime import timedelta

import dependencies.flattener.utils as utils
import dependencies.flattener.constants as constants
import dependencies.flattener.gcp as gcp
import dependencies.flattener.flatten as flatten

import time
import airflow # type: ignore
from airflow import DAG  # type: ignore
from airflow.decorators import task  # type: ignore
from airflow.utils.dates import days_ago  # type: ignore
from airflow.models.param import Param  # type: ignore
from airflow.exceptions import AirflowSkipException  # type: ignore

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
    start_date=days_ago(1),
    params={
        'trigger_firestore_backup': Param(
            default=False,
            type='boolean',
            description='When set to True, triggers Firestore backup and data refresh'
        )
    }
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
def firestore_backup(**context) -> None:
    """
    Task to backup Firestore data.
    Only executes if trigger_firestore_backup parameter is True.
    Otherwise, it raises an AirflowSkipException to mark the task as skipped.
    """
    if not context['params']['trigger_firestore_backup']:
        utils.logger.info("Skipping Firestore backup as parameter is set to False")
        raise AirflowSkipException
    else:
        try:
            gcp.pubsub_firestore_backup(constants.BQ_PROJECT_ID, constants.FIRESTORE_REFRESH_TOPIC)

            # Wait 60 seconds for Firestore backup to complete; usually takes <1 second
            time.sleep(60)
        except Exception as e:
            raise Exception(f"Unable to backup/refresh Firestore data: {str(e)}")

@task(trigger_rule="none_failed")
def table_to_parquet(table_name: str) -> None:
    try:
        gcp.bq_to_parquet(constants.BQ_PROJECT_ID, constants.BQ_RAW_DATASET, table_name, constants.GCS_FLATTENED_BUCKET)
    except Exception as e:
        raise Exception(f"Unable to save {table_name} as Parquet: {str(e)}")

@task(trigger_rule="none_failed")
def flatten_parquet(table_name: str) -> None:
    try:
        flatten.flatten_parquet(table_name, constants.GCS_FLATTENED_BUCKET)
    except Exception as e:
        raise Exception(f"Unable to flatten {table_name} Parquet files: {str(e)}")

@task(trigger_rule="none_failed")
def parquet_to_table(table_name: str) -> None:
    try:
        gcp.parquet_to_bq(constants.BQ_PROJECT_ID, constants.BQ_FLATTENED_DATASET, table_name, constants.GCS_FLATTENED_BUCKET)
    except Exception as e:
        raise Exception(f"Error moving {table_name} Parquet file into BigQuery: {str(e)}")

with dag:
    api_health_check = check_api_health()
    firestore_refresh = firestore_backup()
    
    bq_to_parquet = table_to_parquet.expand(table_name=constants.RAW_TABLES)
    flatten_files = flatten_parquet.expand(table_name=constants.RAW_TABLES)
    parquet_to_bq = parquet_to_table.expand(table_name=constants.RAW_TABLES)

    # Simple linear dependency chain
    api_health_check >> firestore_refresh >> bq_to_parquet >> flatten_files >> parquet_to_bq