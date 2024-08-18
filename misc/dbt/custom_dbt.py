import os
from pathlib import Path
from dagster import asset, OpExecutionContext
from dagster_elt.resources import DbtResource

# Define the dbt asset using the custom DbtResource
@asset
def dbt_warehouse(context: OpExecutionContext, dbt_warehouse_resource: DbtResource) -> None:
    # Define the path to the dbt project directory
    dbt_project_dir = Path(__file__).joinpath("..", "..", "..", "..", "..", "dbt_earthquake", "warehouse").resolve()

    # Create a function to run dbt commands using the custom DbtResource
    def run_dbt_command(context: OpExecutionContext, dbt_resource: DbtResource, command: str) -> None:
        """Run a dbt command using the provided DbtResource."""
        # Construct the dbt command
        dbt_cmd = f"dbt {command} --project-dir {dbt_project_dir}"
        
        # Execute the dbt command
        context.log.info(f"Running dbt command: {dbt_cmd}")
        result = os.system(dbt_cmd)
        
        # Check if the command was successful
        if result != 0:
            raise Exception(f"dbt command failed with exit code {result}")

    # Run the dbt parse command to generate the manifest.json
    run_dbt_command(context, dbt_warehouse_resource, "parse")
    
    # Load the manifest file path
    manifest_path = Path("target").joinpath("manifest.json")
    
    # Verify if the manifest file was created
    if not manifest_path.exists():
        raise Exception(f"Manifest file not found at {manifest_path}")
    
    # Run dbt models
    run_dbt_command(context, dbt_warehouse_resource, "run")
