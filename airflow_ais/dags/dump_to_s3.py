import json
import dateutil.parser
import pendulum
import nats
import os
import pytz
from datetime import timedelta, datetime
import asyncio
from airflow.decorators import dag, task
from minio_conn import MinioClient

from dag_utils.slack import *

@dag(
    schedule_interval=None,
    start_date=pendulum.datetime(2021, 8, 8, tz="UTC"),
    catchup=False,
    tags=['AIS'],
    default_args = {'on_failure_callback': alert_slack_channel}
)
def dump_to_s3():
    home = '/workspaces/pipeline-environment/airflow_ais/dags'
    """
    ### AIS-Pipeline
    """
    @task
    def create_dump_file():
        """
        Creates the file to be dumped into s3 bucket
        """
        data = f"{home}/data"
        os.makedirs(data, exist_ok=True)
        utc = pytz.utc
        now = utc.localize(datetime.now() + timedelta(hours=2))
        start_time = utc.localize(datetime.today() - timedelta(days=1))
        file = open(f"{home}/data/{format(str(now))}",  'a')
        file.close()

        print("end_time")
        print(now)
        print("start_time")
        print(start_time)

        return_dict = {"now" : now, "start_time": start_time}
        return_json = json.dumps(return_dict, indent=4, sort_keys=True, default=str)

        return return_json

    @task()
    def collect_messages(dates):
        # Pull based consumer on 'ais'.
        dates_dict = json.loads(dates)

        end_time = dateutil.parser.parse(dates_dict["now"])
        start_time = dateutil.parser.parse(dates_dict["start_time"])

        asyncio.run(get_messages(start_time, end_time))

        file_name = format(str(end_time))
        
        file = f"{home}/data/{file_name}"
        s3_path = f"airflow/data/{file_name}"

        return s3_path, file

    @task
    def dump_to_s3(file_info):
        mc = MinioClient()
        s3_path = file_info[0]
        file = file_info[1]
        mc.add_file(s3_path, file)

    async def append_to_file(message, time):
        # append message to file
        file = open(f"{home}/data/{format(str(time))}",  'a')
        file.write(str(message))
        file.write('\n')
        file.close()

    async def get_messages(start_time, end_time):
        nc = await nats.connect("localhost:4222")
        js = nc.jetstream()
        psub = await js.subscribe("ais", durable="hllo")

        while start_time < end_time:
            # # Get message
            msg = await psub.next_msg(20)
         
            data = msg.data.decode()
            print("DATA:")
            print(data)
         
            # Extract the date
            date = json.loads(data)["msgtime"]
            timestamp = dateutil.parser.parse(date)
         
            # Append to file
            await append_to_file(data, end_time)
         
            # # update the time
            start_time = timestamp + timedelta(hours=2)
         
            await msg.ack()

    dates = create_dump_file()
    file_info = collect_messages(dates)
    dump_to_s3(file_info)

dump_to_s3 = dump_to_s3()