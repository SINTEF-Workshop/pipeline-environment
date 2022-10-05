from dagster import asset

@asset
def my_asset():
    return [1, 2, 3]

@asset
def another_asset(my_asset):
    return my_asset + 3