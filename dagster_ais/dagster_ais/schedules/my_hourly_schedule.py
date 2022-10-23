from dagster import schedule

from dagster_ais.jobs.dump_to_s3 import dump_to_s3


@schedule(cron_schedule="0 * * * *", job=dump_to_s3, execution_timezone="Europe/Oslo")
def my_hourly_schedule(_context):
    """
    A schedule definition. This example schedule runs once each hour.

    For more hints on running jobs with schedules in Dagster, see our documentation overview on
    schedules:
    https://docs.dagster.io/overview/schedules-sensors/schedules
    """
    run_config = {}
    return run_config
