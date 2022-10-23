from dagster import load_assets_from_package_module, repository

from dagster_ais.assets.my_assets import trajectory_model
from dagster_ais.jobs.dump_to_s3 import dump_to_s3 
from dagster_ais.jobs.generate_trajectories import generate_trajectories
from dagster_ais.schedules.my_hourly_schedule import my_hourly_schedule
from dagster_ais.schedules.my_daily_schedule import my_daily_schedule

@repository
def dagster_ais():
    jobs = [ dump_to_s3, generate_trajectories ]
    assets = [ trajectory_model ]
    schedules = [ my_hourly_schedule, my_daily_schedule ]

    return jobs + assets + schedules