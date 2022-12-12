import pandas as pd

from sqlalchemy import create_engine
import pendulum
import requests
import pandas as pd
from airflow.decorators import dag, task
from components.my_secrets import USERNAME, PASSWORD, HOST, PORT, DATABASE

now = pendulum.now()
url = "https://hotell.difi.no/download/mattilsynet/smilefjes/tilsyn?download"

@dag(start_date=now, schedule="@daily", catchup=False)
def simple_etl():

    @task()
    def extract(url: str) -> pd.DataFrame:
        # Fetch data from url and load into DataFrame
        mattilsynet_df = ...
        mattilsynet_df = pd.read_csv(url, on_bad_lines='skip', sep=';') # This line will be removed

        return mattilsynet_df

    @task()
    def transform(data: pd.DataFrame) -> pd.DataFrame:
        # Remove columns other than these: "tilsynsobjektid", "orgnummer", "navn" 
        # data = ...

        data = data[["tilsynsobjektid", "orgnummer", "navn"]] # This line will be removed
        
        return data

    @task()
    def load(data: pd.DataFrame) -> pd.DataFrame:
        # Credentials for connecting to Postgres
        connection_string = f"postgresql://{USERNAME}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}"
        engine = create_engine(connection_string)

        # Load data to postgresql
        ...

        data.to_sql("mattilsynet", engine, if_exists='replace', index=False) # This line will be removed

        return data

    data = extract(url)
    data_transformed = transform(data)
    load(data_transformed)

simple_etl()