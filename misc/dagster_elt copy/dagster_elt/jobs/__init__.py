from dagster import job
from dagster_elt.ops.ops import fetch_earthquake_data, upload_to_s3
from dagster_elt.assets.dbt.dbt import dbt_warehouse



@job
def earthquake_pipeline():
    data = fetch_earthquake_data()
    upload_to_s3(data)
    dbt_warehouse()
