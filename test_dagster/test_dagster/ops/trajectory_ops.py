from dagster import get_dagster_logger, op 
import asyncio
import pandas as pd
import json
import geopandas as gpd
from datetime import datetime, timedelta
import dateutil.parser

@op(required_resource_keys={"nats_client"})
async def collect_data(context, interval: tuple[datetime, datetime]) -> list[pd.DataFrame]:
    """ Collect messages from nats stream and store in DataFrame """
    nats_conn = context.resources.nats_client
    end_time, start_time = interval

    # Connect to local nats server
    nc = await nats_conn.get_connection("localhost:4222")
    js = nc.jetstream()

    # Subscribe to the stream
    psub = await js.subscribe("ais", durable="fff")
    
    # Collect messages to DataFrame
    df_list = await get_messages(psub, end_time, start_time)

    # Close NATS connection
    await nc.close()

    logger = get_dagster_logger()
    logger.info(df_list)

    return df_list

@op
def create_dataframe(collect_messages: list[pd.DataFrame]) -> pd.DataFrame:
    """ Create a singe DataFrame from the list of DataFrames"""
    df = pd.concat(collect_messages)
    return df

@op(required_resource_keys={"track_maker"})
def create_tracks(context, dataframe: pd.DataFrame) -> gpd.GeoDataFrame:
    """ Creates the trajectories and stores them in a GeoDataFrame """
    logger = get_dagster_logger()
    logger.info(dataframe)
    trips = asyncio.run(context.resources.track_maker.create_trajectories(dataframe))
    logger.info(trips)

    return trips

@op
def save_to_pickle(trips: gpd.GeoDataFrame) -> str:
    """ Saves the trajectories to file and returns the path """
    path = 'test_dagster/data/trajectories/trips.pkl'
    trips.to_pickle(path)
    return path

@op(required_resource_keys={"track_maker"})
def create_geo_dataframe(context, path_to_pickle: str) -> gpd.GeoDataFrame:
    gdf = context.resources.track_maker.load_trips(path_to_pickle)
    logger = get_dagster_logger()
    logger.info(gdf)
    return gdf

@op(required_resource_keys={"postgres"})
def save_to_postgis(context, geodataframe: gpd.GeoDataFrame):
    """ Inserts the trajectories to postgis """
    asyncio.run(context.resources.postgres.geodf_to_postgis(geodataframe))

async def get_messages(psub, end_time, start_time):
    dataframe_list = []

    while start_time < end_time:
        # Get message
        msg = await psub.next_msg(20)
        data = msg.data.decode()

        # Extract the date
        date = json.loads(data)["msgtime"]
        timestamp = dateutil.parser.parse(date)

        # Append to file
        data = json.loads(str( data ).lower())
        df = pd.json_normalize(data)
        dataframe_list.append(df)

        # update the time
        start_time = timestamp

        await msg.ack()

    return dataframe_list