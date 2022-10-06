from dagster import job 

from test_dagster.ops.trajectory_ops import collect_data, create_dataframe, create_tracks, save_to_pickle, create_geo_dataframe, save_to_postgis
from test_dagster.ops.dump_to_s3_ops import get_interval
from test_dagster.resources.my_resources import nats_client, postgres, track_resource
# from dagster_ais.resources.other_resources import track_resource, slow_storage

@job(
    resource_defs={
        # "slow_storage": slow_storage,
        "track_maker": track_resource,
        "postgres": postgres,
        "nats_client": nats_client
    },
    config={
        "ops": {
            "collect_data": {
                "config": {
                    "subject_name": "ais",
                    "durable_name": "ais_durable"
                }
            }
        }
    }
)
def generate_trajectories():
    """
    A job definition. This example job has a single op.
    """
    interval = get_interval()
    collect = collect_data(interval)
    df = create_dataframe(collect)
    trips = create_tracks(df)
    save_to_pickle(trips)
    # geo = create_geo_dataframe(pkl)
    save_to_postgis(trips)
    