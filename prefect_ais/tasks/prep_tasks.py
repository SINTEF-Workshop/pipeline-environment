from prefect import task

import os
import pytz
from datetime import datetime, timedelta


# Task that returns a tuple of datetimes
@task(name="Get interval")
async def get_interval() -> tuple:

    utc = pytz.utc
    now = utc.localize(datetime.now() )
    start_time = utc.localize(datetime.today() - timedelta(days=1))

    return (now, start_time)

@task(name='create_dump_file')
async def create_dump_file(file, now):
    """
    Creates the file to be dumped into s3 bucket
    """
    os.makedirs(os.path.dirname(file), exist_ok=True)
    file = open(file,  'a')
    file.close()
