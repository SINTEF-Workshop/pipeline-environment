import pandas as pd
from dagster import asset, get_dagster_logger
from sqlalchemy import create_engine
from dagster import asset
import pendulum
import requests
import pandas as pd
from dagster_pipelines.components.my_secrets import USERNAME, PASSWORD, HOST, PORT, DATABASE

now = pendulum.now()


@asset()
def extract() -> pd.DataFrame:
    url = "https://hotell.difi.no/download/mattilsynet/smilefjes/tilsyn?download"
    # Fetch data from url and load into DataFrame
    mattilsynet_df = ...
    mattilsynet_df = pd.read_csv(url, on_bad_lines='skip', sep=';') # This line will be removed

    return mattilsynet_df

@asset()
def transform(extract: pd.DataFrame) -> pd.DataFrame:
    # Remove columns other than these: "tilsynsobjektid", "orgnummer", "navn" 
    # data = ...

    data = extract[["tilsynsobjektid", "orgnummer", "navn"]] # This line will be removed
    
    return data

@asset()
def load(transform: pd.DataFrame) -> pd.DataFrame:
    # Credentials for connecting to Postgres
    connection_string = f"postgresql://{USERNAME}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}"
    engine = create_engine(connection_string)

    # Load data to postgresql
    ...

    transform.to_sql("dagster_mat", engine, if_exists='replace', index=False) # This line will be removed

    return transform