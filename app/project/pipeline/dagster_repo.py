from dagster import repository
from earthquake_elt.app.project.pipeline.dagster_pipeline_inc import earthquake_pipeline

@repository
def my_repository():
    return [earthquake_pipeline]