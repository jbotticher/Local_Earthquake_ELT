from dagster import Definitions, EnvVar
from dagster_elt.jobs import earthquake_pipeline, dbt_earthquake_job
from dagster_elt.schedules import earthquake_pipeline_schedule
from dagster_elt.assets.airbyte.airbyte import raw_earthquake
from dagster_elt.assets.dbt.dbt import dbt_warehouse, dbt_warehouse_resource
from dagster_elt.resources import AirbyteResource



defs = Definitions(
    assets=[raw_earthquake, dbt_warehouse],
    jobs=[earthquake_pipeline, dbt_earthquake_job],
    schedules=[earthquake_pipeline_schedule],
    resources={
        "airbyte_conn": AirbyteResource(
            server_name=EnvVar("AIRBYTE_SERVER_NAME"),
            username=EnvVar("AIRBYTE_USERNAME"),
            password=EnvVar("AIRBYTE_PASSWORD"),
            connection_id=EnvVar('AIRBYTE_CONNECTION_ID')
        )
         ,"dbt_warehouse_resource": dbt_warehouse_resource
     }
)
