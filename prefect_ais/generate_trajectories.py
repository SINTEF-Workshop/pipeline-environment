from prefect import flow, task, context
import nats
import asyncio
import pandas as pd

from comp.slow_store import SlowStorage
from comp.nats_conn import NatsConn
from comp.track_maker import TrackMaker
from comp.postgres_conn import PostgresConn


@task
def collect_messages(slow_storage, nc):
    asyncio.run(slow_storage.consumer(slow_storage.append_to_dataframe, "ais_0"))
    print(slow_storage.dataframe_list)

    return slow_storage.dataframe_list

@task
def create_dataframe(collected_messages):
    df = pd.concat(collected_messages, sort=False)

    return df

@task
def create_tracks(dataframe, track_maker):
    trips = asyncio.run(track_maker.create_trajectories(dataframe))
    print(trips)

    return trips

@task
def save_to_pickle(trips):
    path = 'data/trajectories/trips.pkl'
    trips.to_pickle(path)

    return path

@task
def create_geo_dataframe(path_to_pickle, track_maker):
    gdf = track_maker.load_trips(path_to_pickle)
    print(gdf)

    return gdf

@task
def save_to_postgis(geodataframe, postgres):
    asyncio.run(postgres.geodf_to_postgis(geodataframe))

@flow
async def generate_trajectories():
    slow = SlowStorage()
    nc = NatsConn()
    track_maker = TrackMaker()
    postgres = PostgresConn()
    
    messages = collect_messages(slow, nc)
    dataframe = create_dataframe(messages)
    trips = create_tracks(dataframe, track_maker)
    pkl = save_to_pickle(trips)
    gdf = create_geo_dataframe(pkl, track_maker)
    save_to_postgis(gdf, postgres)

# if __name__ == "__main__":
#     asyncio.run(generate_trajectories())