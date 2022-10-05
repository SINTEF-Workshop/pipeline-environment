from dagster import job, fs_io_manager, mem_io_manager, op
# from dagster.core.definitions.executor_definition import execute_in_process_executor, executor

from test_dagster.ops.my_ops import collect_data, create_dump_file, dump_file
# from test_dagster.ops.create_dump_file import create_dump_file
# from test_dagster.ops.consume import dump_file, collect_data
# from test_dagster.resources.connection_resources import minio_client, nats_client
from test_dagster.resources.my_resources import minio_client, nats_client, postgres

@op
def simple_op():
    return 'simple_op'

@job(
    resource_defs={
        "nats_client": nats_client, 
        "minio_client": minio_client
    }
)
def dump_to_s3():
    """
    Collects AIS-data and dumps it to s3
    """
    time = create_dump_file()
    file_name = collect_data(time)
    dump_file(file_name)
    