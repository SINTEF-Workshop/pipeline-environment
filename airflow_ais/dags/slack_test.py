from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator

from dag_utils.slack import *

default_args = {
    'owner'                 : 'airflow',
    'description'           : 'slack testing',
    'depend_on_past'        : False,
    'start_date'            : datetime(2021, 10, 1),
    'email_on_failure'      : False,
    'email_on_retry'        : False,
    'retries'               : 0,
    'on_failure_callback': alert_slack_channel
}

def my_fun():
    print("---------------------")
    print("THIS IS A SLACK MESSAGE DAG")
    #raise an error to trigger slack alert
    # raise Exception('I know Python!') 

with DAG('slack_notify',
         default_args=default_args,
         schedule_interval="0 5 * * *",
         catchup=False) as dag:
     
    t0 = PythonOperator(
            task_id="dummy",
            python_callable=my_fun
            )
    slack_end = PythonOperator(
            task_id='Notify_Slack_DAG_success',
            python_callable=slack_dag_end,
            trigger_rule="all_success") #only triggers if all previous connecting tasks finish successfully

t0 >> slack_end