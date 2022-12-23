import nats
from dagster import get_dagster_logger

class NatsConn:
    log = get_dagster_logger().info

    async def get_jetstream(self, nc):
        # Create jetstream 
        js = nc.jetstream()
        return js

    async def get_connection(self, url):
        # Connect to local nats server
        nc = await nats.connect(url)
        return nc

    async def get_consumer(self, subject_name, nc, durable_name):
        self.log("Connecting to NATS server")
        # Create pull based consumer on 'ais' with chosen name.
        js = nc.jetstream()
        psub = await js.subscribe(subject_name, durable=durable_name)
        return psub

    async def get_message(self, sub):
        # Process a message
        msg = await sub.next_msg(20)
        return msg


    async def ack_all(self, messages):
        for msg in messages:
            await msg.ack()
