from dagster import ScheduleDefinition
from dagster_elt.jobs import earthquake_pipeline

earthquake_pipeline_schedule = ScheduleDefinition(job=earthquake_pipeline, cron_schedule="*/5 * * * *")