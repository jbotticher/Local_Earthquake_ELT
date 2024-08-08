from dagster import repository
from pipeline.dagster_pipeline import earthquake_pipeline

@repository
def my_repository():
    return [earthquake_pipeline]