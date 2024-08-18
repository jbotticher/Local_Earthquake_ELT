from dagster_airbyte import load_assets_from_airbyte_instance, AirbyteResource
# from dagster_elt.resources import airbyte_resource
from dagster import EnvVar


airbyte_resource = AirbyteResource(
    host=EnvVar('AIRBYTE_SERVER_NAME'),
    username=EnvVar('AIRBYTE_USERNAME'),
    password=EnvVar('AIRBYTE_PASSWORD'),
    port=EnvVar('AIRBYTE_PORT')
)

airbyte_assets = load_assets_from_airbyte_instance(
    airbyte_resource
)
