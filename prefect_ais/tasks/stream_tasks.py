from prefect import task

import json
import dateutil.parser

@task(name='collect_messages')
async def collect_messages(path, start_time, end_time, psub):
    print('Getting message')
    while start_time < end_time:
        # # Get message
        msg = await psub.next_msg(20)

        data = msg.data.decode()
        # print("DATA:")
        print(data)

        # Extract the date
        date = json.loads(data)["msgtime"]
        timestamp = dateutil.parser.parse(date)

        # Append to file
        await append_to_file(path, data, end_time)

        # # update the time
        start_time = timestamp

        await msg.ack()


@task(name='close_nats_connection')
async def close_nats_connection(client):
    print("Closing nats connection")
    await client.close()


# <<-------- Helper function -------->>

async def subscribe_to_jetstream(client, subject, durable_name):
    js = client.jetstream()
    psub = await js.subscribe(subject, durable=durable_name)
    print(psub)
    return psub

async def append_to_file(filename, message, time):
    # append message to file
    file = open(filename,  'a')
    file.write(str(message))
    file.write('\n')
    file.close()

