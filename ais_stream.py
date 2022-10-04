import requests
import asyncio
import json
import nats

CLIENT_SECRET="GulPaprika1991"

class LiveData:
    def __init__(self):
        self.ais_token = self.get_ais_token()

    def get_ais_token(self):
        url = 'https://id.barentswatch.no/connect/token'
        data={  'grant_type': 'client_credentials',
                'scope': 'ais',
                'client_id': 'erik_nystad98@hotmail.com:ais_client',
                'client_secret': CLIENT_SECRET}
        headers={'Content-Type': 'application/x-www-form-urlencoded'}
        ais_token = requests.post(url, data=data, headers=headers).json()['access_token']
        return ais_token

    def get_ais_data(self):
        url = 'https://live.ais.barentswatch.no/v1/combined'
        headers={'Authorization': 'Bearer {token}'.format(token=self.ais_token)}

        return requests.get(url, headers=headers, stream=True)

    async def steam_data(self):
        # Connect to local nats server
        nc = await nats.connect('localhost:4222')

        # Create jetstream 
        js = nc.jetstream()

        # Fetch live data from berentswatch
        data = self.get_ais_data()

        for line in data.iter_lines():
            # filter out keep-alive new lines
            if line:
                ack = await js.publish('ais', line)
                print(ack)

async def run():
    live_data = LiveData()
    live_data.get_ais_token()
    await live_data.steam_data()

asyncio.run(run())
