from dagster import job 

from dagster_ais.ops.trajectory_ops import collect_data, create_tracks, save_to_pickle, create_geo_dataframe, save_to_postgis, clean_ais, create_routes, gen_trips
from dagster_ais.ops.dump_to_s3_ops import get_interval
from dagster_ais.resources.my_resources import nats_client, postgres, track_resource
# from dagster_ais.resources.other_resources import track_resource, slow_storage

@job(
    resource_defs={
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
    Job that generates trajectories and stores them in postgres, as well as to a pickle file
    """
    interval = get_interval()
    data = collect_data(interval)
    cleaned = clean_ais(data)
    routes = create_routes(cleaned)
    trips = gen_trips(routes)
    save_to_pickle(trips)
    save_to_postgis(trips)