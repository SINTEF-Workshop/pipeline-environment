from dagster import get_dagster_logger, op 
import asyncio
import pandas as pd
import json
from datetime import datetime, timedelta
import dateutil.parser

# @op(
#     required_resource_keys={"nats_client"},
#     config_schema={"subject_name": str, "durable_name": str}
# )
# async def collect_messages(context):
#     """ 
#     Collects messages from nats and writes them to a file
#     """
#     nats_conn = context.resources.nats_client
#     nc = await nats_conn.get_connection("localhost:4222")
#     slow_storage = context.resources.slow_storage
#     append_func = context.resources.slow_storage.append_to_dataframe

#     subject = context.op_config["subject_name"]
#     durable = context.op_config["durable_name"]

#     df_list = await slow_storage.consumer(nc, append_func, subject, durable)

#     return df_list

@op(required_resource_keys={"nats_client"})
async def collect_messages2(context, interval: tuple[datetime, datetime]) -> list[pd.DataFrame]:
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

    return df_list

@op
def create_dataframe(collect_messages):
    df = pd.concat(collect_messages)
    return df

@op(required_resource_keys={"track_maker"})
def create_tracks(context, dataframe):
    trips = asyncio.run(context.resources.track_maker.create_trajectories(dataframe))
    logger = get_dagster_logger()
    logger.info(trips)
    return trips

@op
def save_to_pickle(trips):
    path = 'test_dagster/data/trajectories/trips.pkl'
    trips.to_pickle(path)
    return path

@op(required_resource_keys={"track_maker"})
def create_geo_dataframe(context, path_to_pickle):
    gdf = context.resources.track_maker.load_trips(path_to_pickle)
    logger = get_dagster_logger()
    logger.info(gdf)
    return gdf

@op(required_resource_keys={"postgres"})
def save_to_postgis(context, geodataframe):
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