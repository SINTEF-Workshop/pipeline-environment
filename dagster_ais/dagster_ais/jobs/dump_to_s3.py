from dagster import job
from dagster_ais.components.my_secrets import SLACK_TOKEN
from dagster_slack import slack_on_failure

from dagster_ais.ops.dump_to_s3_ops import dump_file, get_interval, collect_messages, save_to_parquet
from dagster_ais.ops.my_hooks import slack_message_on_failure, slack_message_on_success
from dagster_ais.resources.my_resources import minio_client, nats_client, slack_resource

@slack_on_failure("#pipelines")
@job(
    resource_defs={
        "nats_client": nats_client, 
        "minio_client": minio_client,
        "slack": slack_resource.configured(
            {"token": SLACK_TOKEN}
        )
    },
)
def dump_to_s3():
    """
    Collects AIS-data and dumps it to s3
    """
    interval = get_interval()
    messages = collect_messages(interval)
    file_path = save_to_parquet(interval, messages)
    dump_file.with_hooks({ slack_message_on_failure, slack_message_on_success })(file_path)
