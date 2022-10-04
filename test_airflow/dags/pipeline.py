import json
import minio
import dateutil.parser
import pendulum
import nats
import os
import pytz
from datetime import timedelta, datetime
import asyncio
from airflow.decorators import dag, task
from minio_conn import MinioClient

@dag(
    schedule_interval=None,
    start_date=pendulum.datetime(2021, 8, 8, tz="UTC"),
    catchup=False,
    tags=['AIS'],
)
def dump_to_s3():
    home = '/home/erik/datapipelines-benchmark/ais_pipeline/airflow_ais/dags'
    """
    ### AIS-Pipeline
    """
    @task
    def create_dump_file():
        """
        Creates the file to be dumped into s3 bucket
        """
        data = "{home}/data"
        os.makedirs(data, exist_ok=True)
        utc = pytz.utc
        now = utc.localize(datetime.now() - timedelta(hours=2))
        start_time = utc.localize(datetime.today() - timedelta(days=1))
        file = open(f"{home}/data/{format(str(now))}",  'a')
        file.close()

        print("end_time")
        print(now)
        print("start_time")
        print(start_time)

        return_obj = {"now" : now, "start_time": start_time}

        return return_obj

    @task()
    def collect_messages(dates):
        # Pull based consumer on 'ais'.

        print("DATES")
        print(dates)
        end_time = dates["now"]
        start_time = dates["start_time"]

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
        nc = await nats.connect("sandbox-toys-nats.sandbox.svc.cluster.local:4222")
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
            start_time = timestamp
         
            await msg.ack()

    dates = create_dump_file()
    file_info = collect_messages(dates)
    dump_to_s3(file_info)

dump_to_s3 = dump_to_s3()