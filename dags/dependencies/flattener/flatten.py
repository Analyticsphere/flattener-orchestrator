import dependencies.flattener.utils as utils


def flatten_parquet(table_id: str, destination_bucket) -> None:
    utils.logger.info(f"Flattening {table_id} Parquet files")

    utils.make_api_call(
        endpoint="flatten_parquet",
        json_data={
            "table_id": table_id,
            "destination_bucket": destination_bucket
        }
    )
