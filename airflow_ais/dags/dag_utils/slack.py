#content of slack.py
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator

#function for alert if a task fails
def alert_slack_channel(context):
    #get dag name for slack message
    last_task: Optional[TaskInstance] = context.get('task_instance')
    dag_name = last_task.dag_id
    task_name = last_task.task_id
    #get error message if task fails
    error_message = context.get('exception') or context.get('reason')
    #get date of dag execution
    execution_date = context.get('execution_date')
    #create slack message body with icons
    title = f':red_circle: AIRFLOW DAG *{dag_name}* - TASK *{task_name}* has failed! :boom:'
    msg_parts = {
    'Execution date': execution_date,
    'Error': error_message
    }
    #format message (add newlines after each part)
    msg = "\n".join([title,*[f"*{key}*: {value}" for key, value in msg_parts.items()]]).strip()
#use SlackWebhookOperator to send message to Slack
    SlackWebhookOperator(
        task_id='notify_slack_channel_alert',
        http_conn_id='slack_webhook',
        message=msg
    ).execute(context=None)


#function for message if DAG finishes
def slack_dag_end(**context):
    #get dag name for slack message
    last_task: Optional[TaskInstance] = context.get('task_instance')
    dag_name = last_task.dag_id
    #get date of dag execution
    execution_date = context.get('execution_date')
    #create slack message body with icons
    title = f':large_green_circle: AIRFLOW DAG *{dag_name}* has successfully finished! :tada:'
    msg_parts = {
        'Execution date': execution_date,
    }
    #format message (add newlines after each part)
    msg = "\n".join([title, *[f"*{key}*: {value}" for key, value in msg_parts.items()]]).strip()
    #use SlackWebhookOperator to send message to Slack
    SlackWebhookOperator(
        task_id='notify_slack_channel_success',
        http_conn_id='slack_webhook',
        message=msg,
    ).execute(context=None)