from dagster import load_assets_from_package_module, repository

from dagster_ais import assets


@repository
def dagster_ais():
    return [load_assets_from_package_module(assets)]
