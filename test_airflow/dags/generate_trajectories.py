import json
import minio
import asyncio
import dateutil.parser
import pendulum
import nats
import os
import pytz
from datetime import timedelta, datetime
import pandas as pd
import asyncio

from airflow.decorators import dag, task
from track_maker import TrackMaker
from minio_conn import MinioClient
from postgres_conn import PostgresConn
from slow_store import SlowStorage


@dag(
    schedule_interval=None,
    start_date=pendulum.datetime(2021, 8, 8, tz="UTC"),
    catchup=False,
    tags=['AIS'],
)
def generate_trajectories():
    slow_storage = SlowStorage()
    track_maker = TrackMaker()
    postgres = PostgresConn()

    home = '/workspaces/pipeline-environment/test_airflow/dags'
    """
    ### AIS-Pipeline
    """
    @task
    def collect_messages():
        asyncio.run(slow_storage.consumer(slow_storage.append_to_dataframe))
        print(slow_storage.dataframe_list)
        return slow_storage.dataframe_list

    @task
    def create_dataframe(collected_messages):
        df = pd.concat(collected_messages, sort=False)
        return df

    @task
    def create_tracks(dataframe):
        trips = asyncio.run(track_maker.create_trajectories(dataframe))
        print(trips)
        return trips

    @task
    def save_to_pickle(trips):
        path = f'{home}/data/trajectories/trips.pkl'
        trips.to_pickle(path)
        return path

    @task
    def create_geo_dataframe(path_to_pickle):
        gdf = track_maker.load_trips(path_to_pickle)
        print(gdf)
        return gdf

    @task
    def save_to_postgis(geodataframe):
        asyncio.run(postgres.geodf_to_postgis(geodataframe))

    collection = collect_messages()
    df = create_dataframe(collection)
    trips = create_tracks(df)
    pkl = save_to_pickle(trips)
    gdf = create_geo_dataframe(pkl)
    save_to_postgis(gdf)

generate_trajectories = generate_trajectories() 