from dagster import Definitions, EnvVar
from dagster_elt.jobs import earthquake_pipeline
from dagster_elt.schedules import earthquake_pipeline_schedule
# from dagster_elt.assets.airbyte.airbyte import airbyte_assets
from dagster_elt.assets.dbt.dbt import dbt_warehouse, dbt_warehouse_resource
# from dagster_elt.resources import airbyte_resource



defs = Definitions(
    assets=[dbt_warehouse]
    # dbt_warehouse, 
#     jobs=[earthquake_pipeline],
#     schedules=[earthquake_pipeline_schedule],
#     resources={
#         "dbt_warehouse_resource":dbt_warehouse_resource
#     }
)



        # "conn_airbyte": AirbyteResource(
        #     server_name=EnvVar("AIRBYTE_SERVER_NAME"),
        #     username=EnvVar("AIRBYTE_USERNAME"),
        #     password=EnvVar("AIRBYTE_PASSWORD"),
        #     port=EnvVar('AIRBYTE_PORT')

        # ),