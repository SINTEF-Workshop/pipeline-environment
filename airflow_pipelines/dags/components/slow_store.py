import asyncio
import json
import nats
from datetime import datetime, timedelta
import dateutil.parser 
import pandas as pd
import pytz

from components.minio_conn import MinioClient
from components.nats_conn import NatsConn

class SlowStorage:
    def __init__(self):
        # Uniform dateformat
        self.utc=pytz.UTC
        self.minio = MinioClient()
        self.nats = NatsConn()
        self.dataframe_list = []
        # Set default time to yesterday
        self.now = self.utc.localize(datetime.now() - timedelta(hours=2))
        self.start_time = self.utc.localize(datetime.today() - timedelta(days=1))
        # File
        self.file = f"data/{format(str(self.now))}"

    async def get_message(self, sub):
        # Process a message
        msg = await sub.next_msg(20)
        return msg

    async def new_subscriber(self, nc, durable_name):
        # Jetstream
        js = nc.jetstream()
        # Pull based consumer on 'ais'.
        psub = await js.subscribe("ais", durable=durable_name)
        return psub

    async def consumer(self, custom_func):
        nc = await self.nats.get_connection("localhost:4222")
        psub = await self.new_subscriber(nc, "ais_test_7")

        await self.get_messages(psub, self.now, self.start_time, custom_func)

    async def append_to_file(self, message, time):
        # append message to file
        file = open(f"data/{format(str(time))}",  'a')
        file.write(str(message))
        file.write('\n')
        file.close()

    async def data_to_json(self, data):
        return json.loads(str( data ).lower())

    async def append_to_dataframe(self, data, time):
        data = await self.data_to_json(data)
            # json to dataframe
        df = pd.json_normalize(data)
        self.dataframe_list.append(df)

    async def get_messages(self, psub, end_time, start_time, custom_func):

        for _ in range(0, 1000):
            # Get message
            msg = await self.get_message(psub)
            data = msg.data.decode()

            # Extract the date
            date = json.loads(data)["msgtime"]
            timestamp = dateutil.parser.parse(date)

            print("now: " + str(start_time))
            print("time: " + str(end_time))

            # Append to file
            await custom_func(data, end_time)

            # update the time
            start_time = timestamp

            await msg.ack()