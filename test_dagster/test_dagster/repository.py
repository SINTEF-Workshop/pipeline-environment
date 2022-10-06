from dagster import load_assets_from_package_module, repository

from test_dagster.assets.my_assets import my_asset, another_asset
from test_dagster.jobs.dump_to_s3 import dump_to_s3 
from test_dagster.jobs.generate_trajectories import generate_trajectories

@repository
def test_dagster():
    jobs = [dump_to_s3, generate_trajectories]
    assets = [my_asset, another_asset]

    return jobs + assets