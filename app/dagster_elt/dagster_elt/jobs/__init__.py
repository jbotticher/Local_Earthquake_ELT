from dagster import job
from dagster_elt.ops.ops import fetch_earthquake_data, upload_to_s3
from dagster_elt.assets.dbt.dbt import dbt_warehouse
from dagster_elt.assets.airbyte.airbyte import raw_earthquake



@job
def earthquake_pipeline():
    data = fetch_earthquake_data()
    upload_to_s3(data)
    raw_earthquake()
    dbt_warehouse()