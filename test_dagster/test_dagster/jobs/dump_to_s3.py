from dagster import job, fs_io_manager, mem_io_manager, op
# from dagster.core.definitions.executor_definition import execute_in_process_executor, executor

from test_dagster.ops.dump_to_s3_ops import dump_file, get_interval, collect_messages, save_to_parquet
from test_dagster.resources.my_resources import minio_client, nats_client

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
    interval = get_interval()
    messages = collect_messages(interval)
    file_path = save_to_parquet(interval, messages)
    dump_file(file_path)
    