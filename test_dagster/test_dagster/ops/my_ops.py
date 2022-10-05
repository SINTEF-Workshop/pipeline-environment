from dagster import Out, get_dagster_logger, op
import json

import dateutil.parser

@op(required_resource_keys={"nats_client"})
async def collect_data(context, create_dump_file):
    nats_conn = context.resources.nats_client
    """ An op definition. This example op outputs a single string. """
    # Connect to local nats server
    nc = await nats_conn.get_connection("localhost:4222")

    # Jetstream
    js = nc.jetstream()

    # Pull based consumer on 'ais'.
    psub = await js.subscribe("ais", durable="fff")

    end_time = create_dump_file[0]
    start_time = create_dump_file[1]

    await get_messages(start_time, end_time, psub)

    file = f"dagster_ais/data/{format(str(end_time))}"
    # Close NATS connection
    await nc.close()

    return file

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
    while start_time < end_time:
        # # Get message
        msg = await psub.next_msg(20)

        data = msg.data.decode()
        logger = get_dagster_logger()
        logger.info("DATA:")
        logger.info(data)

        # Extract the date
        date = json.loads(data)["msgtime"]
        timestamp = dateutil.parser.parse(date)

        # Append to file
        await append_to_file(data, end_time)

        # # update the time
        start_time = timestamp

        await msg.ack()


from dagster import op, get_dagster_logger
from datetime import datetime, timedelta
import pytz
import os
import dateutil.parser

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
