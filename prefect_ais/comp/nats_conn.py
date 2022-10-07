import nats

class NatsConn:

    async def get_jetstream(self):
        # Connect to local nats server
        nc = await nats.connect('sandbox-toys-nats.sandbox.svc.cluster.local:4222')

        # Create jetstream 
        js = nc.jetstream()
        return js

    async def get_connection(self, url):
        # Connect to local nats server
        nc = await nats.connect(url)
        return nc

    async def get_consumer(self, name):
        # Create pull based consumer on 'ais' with chosen name.
        js = await self.get_jetstream()

        psub = await js.subscribe("ais", durable=name)
        return psub

    async def get_message(self, sub):
        # Process a message
        msg = await sub.next_msg(20)
        return msg


    async def ack_all(self, messages):
        for msg in messages:
            await msg.ack()

