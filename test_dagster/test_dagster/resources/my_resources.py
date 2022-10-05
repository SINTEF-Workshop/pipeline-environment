from dagster import resource

from test_dagster.components.minio_conn import MinioClient
from test_dagster.components.postgres_conn import PostgresConn
from test_dagster.components.nats_conn import NatsConn

@resource
def postgres(init_context):
    return PostgresConn()

@resource
def nats_client(init_context):
    return NatsConn()

@resource
def minio_client(init_context):
    return MinioClient()