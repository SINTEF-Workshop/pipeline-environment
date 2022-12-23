from prefect import flow, task, get_run_logger
from datetime import datetime, timedelta
import sys
sys.path.append("./")
import nats
import asyncio
from comp.minio_client import MinioClient
from tasks.prep_tasks import create_dump_file, get_interval
from tasks.stream_tasks import collect_messages, subscribe_to_jetstream, close_nats_connection
from tasks.minio_tasks import dump_file

# <<-------- My Flows -------->>

@flow(name="dump_to_s3")
async def dump_to_s3():
    # Get interval
    logger = get_run_logger()
    logger.info("INFO level log message.")
    now, start = await get_interval()

    # Create dumpfile
    filename = f"data/{format(str(now))}"
    await create_dump_file(filename, now)

    # Get connections
    min = MinioClient()
    nc = await nats.connect("localhost:4222")

    # Subscribe to stream
    psub = await subscribe_to_jetstream(nc, "ais", "fff")

    # Collect messages
    collection = await collect_messages(filename, start, now, psub)

    # Path used in the s3 storage
    s3_file = f"prefect_ais/{filename}"
    dump_task = await dump_file(min, "ais", s3_file, filename, wait_for=[collection])
    
    # Close NATS connection
    await close_nats_connection(nc, wait_for=[dump_task])
    
# if __name__ == "__main__":
#     asyncio.run(dump_to_s3())

