from airflow.operators.slack_operator import SlackAPIPostOperator
from dateutil.relativedelta import relativedelta

from airflow.models import Variable
import logging


def on_failure(context):
    channel = 'data_alert'
    token = Variable.get('slack_token_key')

    task_instance = context.get('task_instance')
    task_id = task_instance.task_id
    dag_id = task_instance.dag_id
    log_url = context.get('task_instance').log_url

    # standard UTC + 9hour
    execution_date = (context.get('execution_date') +
                      relativedelta(hours=9)).strftime('%Y-%m-%d %H:%M:%S')

    # message to slack
    text = f'''
        *[:exclamation: AIRFLOW ERROR REPORT]*
        ■ DAG: _{dag_id}_ 
        ■ Task: _{task_id}_ 
        ■ Execution Date (KST): _{execution_date}_ 
        ■ Log Url : {log_url}'''

    alert = SlackAPIPostOperator(
        task_id='slack_failed', channel=channel, token=token, text=text)

    return alert.execute(context=context)
