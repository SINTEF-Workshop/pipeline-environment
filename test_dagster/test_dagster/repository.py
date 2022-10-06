from dagster import load_assets_from_package_module, repository

from test_dagster.assets.my_assets import trajectory_model
from test_dagster.jobs.dump_to_s3 import dump_to_s3 
from test_dagster.jobs.generate_trajectories import generate_trajectories

@repository
def test_dagster():
    jobs = [dump_to_s3, generate_trajectories]
    assets = [ trajectory_model ]

    return jobs + assets