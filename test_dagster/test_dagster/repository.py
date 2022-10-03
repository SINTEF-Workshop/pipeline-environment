from dagster import load_assets_from_package_module, repository

from test_dagster import assets


@repository
def test_dagster():
    return [load_assets_from_package_module(assets)]
