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

