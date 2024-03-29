from dagster import resource

from dagster_pipelines.components.minio_conn import MinioClient
from dagster_pipelines.components.postgres_conn import PostgresConn
from dagster_pipelines.components.nats_conn import NatsConn
from dagster_pipelines.components.track_maker import TrackMaker
from dagster_slack import slack_resource

@resource
def postgres(init_context):
    return PostgresConn()

@resource
def nats_client(init_context):
    return NatsConn()

@resource
def minio_client(init_context):
    return MinioClient()

@resource
def track_resource(init_context):
    return TrackMaker()