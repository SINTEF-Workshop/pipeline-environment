from dagster import load_assets_from_package_module, repository
from dagster_pipelines import assets
from turtle import pos
from dagster import load_assets_from_package_module, repository
from dagster import with_resources

from dagster_pipelines.assets.my_assets import trajectory_model, trajectory_forecast, ml_model_job
from dagster_pipelines.assets.simple_etl import extract, transform, load
from dagster_pipelines.assets.trajectory_assets import ais_messages, ais_messages_cleaned, trajectory_list, trajectory_dataframe, trajectory_table
from dagster_pipelines.jobs.dump_to_s3 import dump_to_s3 
from dagster_pipelines.jobs.generate_trajectories import generate_trajectories
from dagster_pipelines.schedules.my_hourly_schedule import my_hourly_schedule
from dagster_pipelines.schedules.my_daily_schedule import my_daily_schedule
from dagster_pipelines.resources.my_resources import track_resource, postgres, nats_client

@repository
def dagster_pipelines():
    jobs = [ dump_to_s3, generate_trajectories, ml_model_job ]
    assets = with_resources(
        definitions=[ 
            extract, transform, load,
            trajectory_model, trajectory_forecast, ais_messages, 
            ais_messages_cleaned, trajectory_list, trajectory_dataframe, trajectory_table
        ],
        resource_defs={"track_maker": track_resource, "postgres": postgres, "nats_client": nats_client}
    )
    schedules = [ my_hourly_schedule, my_daily_schedule ]

    return jobs + assets + schedules