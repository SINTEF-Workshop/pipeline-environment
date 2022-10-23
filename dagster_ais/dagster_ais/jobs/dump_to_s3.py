from dagster import job, fs_io_manager, mem_io_manager, op
from dagster_ais.components.my_secrets import SLACK_TOKEN
# from dagster.core.definitions.executor_definition import execute_in_process_executor, executor
from dagster_slack import slack_on_failure

from dagster_ais.ops.dump_to_s3_ops import dump_file, get_interval, collect_messages, save_to_parquet, slack_message_on_failure, slack_message_on_success
from dagster_ais.resources.my_resources import minio_client, nats_client, slack_resource

@slack_on_failure("#pipelines")
@job(
    resource_defs={
        "nats_client": nats_client, 
        "minio_client": minio_client,
        "slack": slack_resource.configured(
            {"token": "xoxb-4255919535318-4247996924807-1TKLS5bIOZOjF9cNtmZY9YRC"}
        )
    },
   # hooks={slack_message_on_success, slack_message_on_failure}
)
def dump_to_s3():
    """
    Collects AIS-data and dumps it to s3
    """
    interval = get_interval()
    messages = collect_messages(interval)
    file_path = save_to_parquet(interval, messages)
    dump_file.with_hooks({ slack_message_on_failure, slack_message_on_success })(file_path)

# job_result = dump_to_s3.execute_in_process(
#     run_config={'resources': {'slack': {'config': {'token': SLACK_TOKEN }}}}
# )