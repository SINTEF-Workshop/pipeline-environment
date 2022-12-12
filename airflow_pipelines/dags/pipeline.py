import json
import os
from pydoc import doc
import shutil
from textwrap import dedent
from datetime import datetime, timedelta
# For airflow
import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

home = '/workspace/airflow_pipelines/dags'
loreversion = '0.3.6'

# Postgres values
db_name = 'test_restaurants'
db_user = 'erik'
db_password = 'password'
db_host = 'localhost'
psql = f"psql -d {db_name} -U {db_user} -h {db_host}"

default_args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(0),
    'email': ['erik_nystad98@hotmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
dag = DAG(
    'restaurants_pipeline',
    default_args=default_args,
    description='Pipeline to process restaurants data',
    schedule_interval='@once',
)

# <---- SETUP ----->

clean_database = PostgresOperator(
    task_id='clean_database',
    sql="""
        -- Clean osm
        DROP TABLE IF EXISTS gis_osm_pois_free_1 CASCADE; 
        DROP TABLE IF EXISTS gis_osm_places_a_free_1 CASCADE; 
        DROP TABLE IF EXISTS gis_osm_landuse_a_free_1 CASCADE; 
        -- Clean mattilsynet
        DROP TABLE IF EXISTS mattilsynet CASCADE; 
        DROP TABLE IF EXISTS tilsyns_data_table CASCADE; 
        DROP TABLE IF EXISTS tilsyns_data_table_filtered CASCADE; 
        -- Clean matrikkelen
        DROP SCHEMA IF EXISTS matrikkelen CASCADE; 
        DROP TABLE IF EXISTS matrikkel_data_table CASCADE; 
        DROP SCHEMA IF EXISTS servering CASCADE; 
    """,
    postgres_conn_id='restaurants_conn',
    dag=dag
)

init_psql_extensions = PostgresOperator(
    task_id='init_psql_extensions',
    sql="""
     CREATE EXTENSION IF NOT EXISTS postgis; 
	 CREATE EXTENSION IF NOT EXISTS pg_trgm; 
	 CREATE SCHEMA IF NOT EXISTS servering; 
    """,
    postgres_conn_id='restaurants_conn',
    dag=dag
)

# <---- EXTRACT DATA ----->
# DOWNLOAD
download_osm_data = BashOperator(
    task_id='download_osm_data',
    bash_command= f"wget -O {home}/data/osm_shape_files.shp.zip https://download.geofabrik.de/europe/norway-latest-free.shp.zip",
    dag=dag
)

download_mattilsynet = BashOperator(
    task_id='download_mattilsynet',
    bash_command=f"wget -O {home}/data/mattilsynet.csv https://hotell.difi.no/download/mattilsynet/smilefjes/tilsyn?download",
    dag=dag
)

download_matrikkelen = BashOperator(
    task_id='download_matrikkelen',
    bash_command=f"wget -O {home}/data/matrikkelen.zip https://nedlasting.geonorge.no/api/download/order/4453ae0c-85b9-4ff0-8ea7-6ea57eb44f5f/576b323b-4f2c-4227-ac54-26ce037997e7",
    dag=dag
)

# UNZIP FOLDERS 
unzip_matrikkelen = BashOperator(
    task_id='unzip_matrikkelen',
    bash_command=f"unzip {home}/data/matrikkelen.zip -d {home}/data/matrikkelen",
    dag=dag
)

unzip_osm_data = BashOperator(
    task_id='unzip_osm_data',
    bash_command=f"unzip {home}/data/osm_shape_files.shp.zip -d {home}/data/osm_shape_files",
    dag=dag
)

def delete_zip_files_func():
    """
    Delete zip files from data folder.
    """
    data = f"{home}/data"
    files = [f for f in os.listdir(data) if f.endswith('.zip')]
    for file in files:
        delete_file(f"{data}/{file}")

def delete_file(file):
    """
    Delete a file if it exists.
    """
    if os.path.exists(file):
        os.remove(file)

delete_zip_files = PythonOperator(
    task_id='delete_zip_files',
    python_callable=delete_zip_files_func,
    dag=dag
)

# <---- LOAD DATA ----->
# Load shapefiles into PostgreSQL
load_pois = BashOperator(
    task_id='load_pois',
    bash_command=f"""
		ogr2ogr -f PostgreSQL PG:"dbname={ db_name } user={ db_user } password={ db_password } host={ db_host }" -nlt PROMOTE_TO_MULTI {home}/data/osm_shape_files/gis_osm_pois_free_1.shp
    """,
    dag=dag
)

load_places = BashOperator(
    task_id='load_places',
    bash_command=f"""
        ogr2ogr -f PostgreSQL PG:"dbname={ db_name } user={ db_user } password={ db_password } host={ db_host }" -nlt PROMOTE_TO_MULTI {home}/data/osm_shape_files/gis_osm_places_free_1.shp
    """,
    dag=dag
)

load_landuse = BashOperator(
    task_id='load_landuse',
    bash_command=f"""
        ogr2ogr -f PostgreSQL PG:"dbname={ db_name } user={ db_user } password={ db_password } host={ db_host }" -nlt PROMOTE_TO_MULTI {home}/data/osm_shape_files/gis_osm_landuse_a_free_1.shp
    """,
    dag=dag
)

# OBS requires PGPASSWORD to be exported in the environment (or a postgres database wthouth password)
load_matrikkelen = BashOperator(
    task_id='load_matrikkelen',
    bash_command=f"psql -d {db_name} -U {db_user} -h {db_host} -f {home}/data/matrikkelen/Basisdata_0000_Norge_25833_MatrikkelenAdresse_PostGIS.sql",
    dag=dag
)

create_mattilsyn_table = PostgresOperator(
    task_id='create_mattilsyn_table',
    sql="""
    CREATE TABLE IF NOT EXISTS mattilsynet (tilsynsobjektid	text, orgnummer text, navn text, adrlinje1 text, adrlinje2 text, postnr text, poststed text, tilsynid text, sakref text, status text, dato text, total_karakter text, tilsynsbesoektype text, tema1_no text, tema1_nn text, karakter1 text, tema2_no	text, tema2_nn text, karakter2 text, tema3_no text, tema3_nn text, karakter3 text, tema4_no text, tema4_nn text, karakter4 text);
    """,
    postgres_conn_id='restaurants_conn',
    dag=dag
)

rename_matrikkel_schema = PostgresOperator(
    task_id='rename_matrikkel_schema',
    sql="""
    alter schema matrikkelenadresse_af7c695662794cc196ade54c90b3d221 rename to matrikkelen;
    """,
    postgres_conn_id='restaurants_conn',
    dag=dag
)

def csvToPostgres():
    pg_hook = PostgresHook(postgres_conn_id='restaurants_conn')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    # Load csv into PostgreSQL
    with open(f'{home}/data/mattilsynet.csv', 'r') as f:
        cursor.copy_expert(f"""COPY mattilsynet FROM STDIN WITH DELIMITER ';' CSV HEADER""", f)
    conn.commit()

load_mattilsynet = PythonOperator(
    task_id='load_mattilsynet',
    python_callable=csvToPostgres,
    dag=dag
)


# Define dependencies

# Cleanup and set up folders
clean_database >> init_psql_extensions
init_psql_extensions >> [ download_mattilsynet, download_matrikkelen, download_osm_data ]

# Unzip and load to database
load_mattilsynet << create_mattilsyn_table << download_mattilsynet
load_matrikkelen << unzip_matrikkelen << download_matrikkelen
[ load_pois, load_places, load_landuse ] << unzip_osm_data << download_osm_data

# Delete zip files
[ unzip_matrikkelen, unzip_osm_data ] >> delete_zip_files

rename_matrikkel_schema << load_matrikkelen