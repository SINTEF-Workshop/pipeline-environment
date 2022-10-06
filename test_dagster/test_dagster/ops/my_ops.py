from dagster import get_dagster_logger, op
import os
import json
import pytz
import pandas as pd
import dateutil.parser
from pendulum import date
from datetime import datetime, timedelta

@op(required_resource_keys={"nats_client"})
async def collect_messages(context, create_dump_file) -> pd.DataFrame:
    """ Collect messages from nats stream and store in DataFrame"""
    nats_conn = context.resources.nats_client
    end_time, start_time = create_dump_file

    # Connect to local nats server
    nc = await nats_conn.get_connection("localhost:4222")
    js = nc.jetstream()

    # Subscribe to the stream
    psub = await js.subscribe("ais", durable="fff")
    
    # Collect messages to DataFrame
    messages_df = await get_messages(start_time, end_time, psub)

    # Close NATS connection
    await nc.close()

    return messages_df

# @op(required_resource_keys={"nats_client"})
# async def collect_data(context, create_dump_file):
#     nats_conn = context.resources.nats_client
#     """ An op definition. This example op outputs a single string. """
#     # Connect to local nats server
#     nc = await nats_conn.get_connection("localhost:4222")

#     # Jetstream
#     js = nc.jetstream()

#     # Pull based consumer on 'ais'.
#     psub = await js.subscribe("ais", durable="fff")

#     end_time = create_dump_file[0]
#     start_time = create_dump_file[1]

#     await get_messages(start_time, end_time, psub)

#     file = f"dagster_ais/data/{format(str(end_time))}"
#     # Close NATS connection
#     await nc.close()

#     return file

@op
def save_to_parquet(interval: tuple[datetime, datetime], messages: pd.DataFrame) -> str:
    """Saves DataFrame to parquet and returns filepath"""
    end_time, start_time = interval
    file_path = f"dagster_ais/data/{format(str(end_time))}"
    messages.to_parquet(file_path)
    return file_path
    
@op(required_resource_keys={"minio_client"})
def dump_file(context, file_name):
    minio_conn = context.resources.minio_client
    """ An op definition. This example op outputs a single string. """
    minio_conn.add_file(file_name, file_name)

async def append_to_file(message, time):
    # append message to file
    file = open(f"dagster_ais/data/{format(str(time))}",  'a')
    file.write(str(message))
    file.write('\n')
    file.close()

async def get_messages(start_time, end_time, psub):
    df = pd.DataFrame()
    while start_time < end_time:
        # # Get message
        msg = await psub.next_msg(20)

        data = json.loads(msg.data.decode())
        logger = get_dagster_logger()
        logger.info("DATA:")
        logger.info(data)

        # Extract the date
        date = data["msgtime"]
        timestamp = dateutil.parser.parse(date)

        # Append to file
        df = df.append(data, ignore_index=True)
        await append_to_file(data, end_time)

        # # update the time
        start_time = timestamp

        await msg.ack()

    return df



@op
def create_dump_file():
    """
    Creates the file to be dumped into s3 bucket
    """
    data = "dagster_ais/data"
    os.makedirs(data, exist_ok=True)
    utc = pytz.utc
    now = utc.localize(datetime.now())
    start_time = utc.localize(datetime.today() - timedelta(days=1))
    file = open(f"{data}/{format(str(now))}",  'a')
    file.close()

    logger = get_dagster_logger()
    logger.info("end_time")
    logger.info(now)
    logger.info("start_time")
    logger.info(start_time)

    return now, start_time

@op
def get_interval() -> tuple[datetime, datetime]:
    """
    Gets the time interval to fetch data for 
    """
    utc = pytz.utc
    # End time is the time the run is initiated
    end_time = utc.localize(datetime.now())
    # Start time defaults to yesterday
    start_time = utc.localize(datetime.today() - timedelta(days=1))

    logger = get_dagster_logger()
    logger.info("end_time")
    logger.info(end_time)
    logger.info("start_time")
    logger.info(start_time)

    return end_time, start_time