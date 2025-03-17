import dependencies.flattener.utils as utils

def bq_to_parquet(project_id: str, dataset_id: str, table_id: str, destination_bucket) -> None:
    utils.logger.info(f"Saving {table_id} to Parquet file in {destination_bucket}")

    utils.make_api_call(
        endpoint="table_to_parquet",
        json_data={
            "project_id": project_id,
            "dataset_id": dataset_id,
            "table_id": table_id,
            "destination_bucket": destination_bucket
        }
    )

def parquet_to_bq(project_id: str, dataset_id: str, table_id: str, destination_bucket) -> None:
    utils.logger.info(f"Moving {table_id} Parquet file into BigQuery")

    utils.make_api_call(
        endpoint="parquet_to_table",
        json_data={
            "project_id": project_id,
            "dataset_id": dataset_id,
            "table_id": table_id,
            "destination_bucket": destination_bucket
        }
    )