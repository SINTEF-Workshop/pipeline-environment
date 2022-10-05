from dagster import load_assets_from_package_module, repository

from test_dagster.assets.my_assets import my_asset, another_asset
from test_dagster.jobs.dump_to_s3 import dump_to_s3 

@repository
def test_dagster():
    jobs = [dump_to_s3]
    assets = [my_asset, another_asset]

    return jobs + assets