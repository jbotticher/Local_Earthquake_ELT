from dagster import ScheduleDefinition
from dagster_elt.jobs import earthquake_pipeline, dbt_earthquake_job

# earthquake_pipeline_schedule = ScheduleDefinition(job=earthquake_pipeline, cron_schedule="*/5 * * * *")


# Schedule for earthquake_pipeline to run every 5 minutes
earthquake_pipeline_schedule = ScheduleDefinition(
    job=earthquake_pipeline,
    cron_schedule="*/1 * * * *"  # Every 5 minutes
)

# Schedule for dbt_earthquake_job to run every 3 minutes
dbt_earthquake_job_schedule = ScheduleDefinition(
    job=dbt_earthquake_job,
    cron_schedule="*/3 * * * *"  # Every 3 minutes
)