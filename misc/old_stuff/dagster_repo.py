from dagster import repository
from earthquake_elt.App.project.pipeline.dagster_pipeline_inc import earthquake_pipeline, schedule

@repository
def my_repository():
    return [
        earthquake_pipeline,
        schedule
    ]