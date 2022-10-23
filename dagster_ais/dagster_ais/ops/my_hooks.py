from dagster import success_hook, failure_hook, HookContext

@success_hook(required_resource_keys={"slack"})
def slack_message_on_success(context: HookContext):
    """ Sends out slack-message on success """
    message = f"Op {context.op.name} finished successfully"
    context.resources.slack.chat_postMessage(channel='#pipelines', text=message)

@failure_hook(required_resource_keys={"slack"})
def slack_message_on_failure(context: HookContext):
    """ Sends out slack-message on failure """
    message = f"Op {context.op.name} failed"
    context.resources.slack.chat_postMessage(channel="#pipelines", text=message)

