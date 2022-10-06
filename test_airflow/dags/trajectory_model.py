import json
import dateutil.parser
import pendulum
import nats
import os
import pytz
from datetime import timedelta, datetime
import asyncio
from airflow.decorators import dag, task
from minio_conn import MinioClient

@dag(
    schedule_interval=None,
    start_date=pendulum.datetime(2021, 8, 8, tz="UTC"),
    catchup=False,
    tags=['AIS'],
)
def trajectory_model():
    home = '/workspaces/pipeline-environment/test_airflow/dags'
    """
    ### AIS-Pipeline
    """
    @task
    def create_dump_file():
        """
        Creates the file to be dumped into s3 bucket
        """
        pass

    test = create_dump_file()

trajectory_model = trajectory_model() 