from dagster import ConfigurableResource

class AirbyteResource(ConfigurableResource):    
    server_name: str
    username: str
    password: str
    connection_id: str