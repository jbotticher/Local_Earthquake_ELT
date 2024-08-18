from dagster import job, define_asset_job, sensor, RunRequest, SkipReason
from dagster_elt.ops.ops import fetch_earthquake_data, upload_to_s3
from dagster_elt.assets.dbt.dbt import dbt_warehouse
from dagster_elt.assets.airbyte.airbyte import raw_earthquake
from dagster_dbt import build_dbt_asset_selection
from ..assets.dbt.dbt import dbt_warehouse



# Select relevant dbt assets
dbt_earthquake_selection = build_dbt_asset_selection([dbt_warehouse], "stg_flatten_raw").downstream()

@job
def earthquake_pipeline():
    data = fetch_earthquake_data()
    upload_to_s3(data)
    raw_earthquake()


dbt_earthquake_job = define_asset_job(name="dbt_earthquake", selection=dbt_earthquake_selection)