# from dagster_airbyte import AirbyteResource
# from dagster import EnvVar
# # from dotenv import load_dotenv
# # import os



# # load_dotenv()

# # airbyte_resource = AirbyteResource(
# #     host=os.getenv("AIRBYTE_SERVER_NAME"),
# #     usernam=os.getenv("AIRBYTE_USERNAME"),
# #     password=os.getenv("AIRBYTE_PASSWORD"),
# #     port=os.getenv("AIRBYTE_PORT")  
# # )

# airbyte_resource = AirbyteResource(
#     host=EnvVar('AIRBYTE_SERVER_NAME'),
#     username=EnvVar('AIRBYTE_USERNAME'),
#     password=EnvVar('AIRBYTE_PASSWORD'),
#     port=EnvVar('AIRBYTE_PORT')
# )



# from dagster import ConfigurableResource

# class AirbyteResource(ConfigurableResource):    
#     server_name: str
#     username: str
#     password: str
#     port: str