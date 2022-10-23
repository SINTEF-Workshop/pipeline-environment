from dagster import get_dagster_logger, op, HookContext, failure_hook, success_hook
from dagster_slack import slack_on_success, slack_on_failure

import json
import pytz
import pandas as pd
import dateutil.parser
from datetime import datetime, timedelta

@op
def get_interval() -> tuple[datetime, datetime]:
    """ Gets the time interval to fetch data for """
    utc = pytz.utc
    # End time is the time the run is initiated
    end_time = utc.localize(datetime.now())
    # Start time defaults to yesterday
    start_time = utc.localize(datetime.today() - timedelta(days=1))

    return end_time, start_time

@op(required_resource_keys={"nats_client"})
async def collect_messages(context, create_dump_file: tuple[datetime, datetime]) -> pd.DataFrame:
    """ Collect messages from nats stream and store in DataFrame """
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

@op
def save_to_parquet(interval: tuple[datetime, datetime], messages: pd.DataFrame) -> str:
    """Saves DataFrame to parquet and returns filepath"""
    # Using timestamp as unique ID for the file
    end_time, start_time = interval
    file_path = f"dagster_ais/data/{format(str(end_time))}.parquet"
    messages.to_parquet(file_path)

    return file_path
    
@op(required_resource_keys={"minio_client"})
def dump_file(context, file_name: str):
    """Dumps the file to S3 storage"""
    minio_conn = context.resources.minio_client
    minio_conn.add_file(file_name, file_name)

@op(required_resource_keys={'slack'})
def slack_op(context):
    context.resources.slack.chat_postMessage(channel='#pipelines', text=':wave: hey there!')

@success_hook(required_resource_keys={"slack"})
def slack_message_on_success(context: HookContext):
    """ Sends out slack-message on success """
    message = f"Op {context.op.name} finished successfully"
    context.resources.slack.chat_postMessage(channel='#pipelines', text=message)

@failure_hook(required_resource_keys={"slack"})
def slack_message_on_failure(context: HookContext):
    """ Sends out slack-message on failure """
    message = f"Op {context.op.name} failed"
    context.resources.slack.chat_postMessage(channel="#pipelines", text=message)

# Collects the messages into a dataframe and returns it
async def get_messages(start_time, end_time, psub):
    df = pd.DataFrame()
    while start_time < end_time:
        # Get message
        msg = await psub.next_msg(20)
        data = json.loads(msg.data.decode())

        logger = get_dagster_logger()
        logger.info("DATA:")
        logger.info(data)

        # Extract the date
        date = data["msgtime"]
        timestamp = dateutil.parser.parse(date)

        # Append to DataFrame
        df = df.append(data, ignore_index=True)

        # # update the time
        start_time = timestamp

        await msg.ack()

    return df