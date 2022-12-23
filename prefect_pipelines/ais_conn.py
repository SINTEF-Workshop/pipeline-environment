from prefect import flow, task
import requests
import asyncio
import json
import nats

from comp.my_secrets import CLIENT_SECRET

@task(name='get_token', retries=3, retry_delay_seconds=5)
async def get_token():
        url = 'https://id.barentswatch.no/connect/token'
        data={  'grant_type': 'client_credentials',
                'scope': 'ais',
                'client_id': 'erik_nystad98@hotmail.com:ais_client',
                'client_secret': CLIENT_SECRET}
        headers={'Content-Type': 'application/x-www-form-urlencoded'}
        ais_token = requests.post(url, data=data, headers=headers).json()['access_token']
        return ais_token

@task(name='stream_data', retries=5, retry_delay_seconds=5)
async def stream_data(token):
        url = 'https://live.ais.barentswatch.no/v1/combined'
        headers={'Authorization': 'Bearer {token}'.format(token=token)}
        data = requests.get(url, headers=headers, stream=True)

        # Connect to local nats server
        nc = await nats.connect('localhost:4222')

        # Create jetstream 
        js = nc.jetstream()

        for line in data.iter_lines():
            # filter out keep-alive new lines
            if line:
                ack = await js.publish('ais', line)
                print(ack)

@flow(name='ais_stream')
async def ais_stream():
    token = await get_token()
    print(token)
    await stream_data(token)

# if __name__ == "__main__":
#     asyncio.run(ais_stream())
