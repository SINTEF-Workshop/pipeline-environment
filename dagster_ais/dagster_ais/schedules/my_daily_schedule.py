from dagster import schedule

from dagster_ais.jobs.generate_trajectories import generate_trajectories


@schedule(cron_schedule="0 0 * * *", job=generate_trajectories, execution_timezone="Europe/Oslo")
def my_daily_schedule(_context):
    """
    A schedule definition. This example schedule runs once each hour.

    For more hints on running jobs with schedules in Dagster, see our documentation overview on
    schedules:
    https://docs.dagster.io/overview/schedules-sensors/schedules
    """
    run_config = {}
    return run_config
