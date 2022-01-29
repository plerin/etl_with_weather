from airflow.operators.slack_operator import SlackAPIPostOperator
from dateutil.relativedelta import relativedelta

from airflow.models import Variable
import logging


def on_failure(context):
    # logging.info(context)
    channel = 'data_alert'
    token = Variable.get('slack_token_key')

    task_instance = context.get('task_instance')
    task_id = task_instance.task_id
    dag_id = task_instance.dag_id
    # exception = context.get('exception')
    log_url = context.get('task_instance').log_url

    # 서버가 UTC 시간 기준일 경우 9시간을 더해준다.
    execution_date = (context.get('execution_date') +
                      relativedelta(hours=9)).strftime('%Y-%m-%d %H:%M:%S')
    next_execution_date = (context.get(
        'next_execution_date') + relativedelta(hours=9)).strftime('%Y-%m-%d %H:%M:%S')

    # 슬랙으로 보낼 메세지를 적는다.
    text = f'''
        *[:exclamation: AIRFLOW ERROR REPORT]*
        ■ DAG: _{dag_id}_ 
        ■ Task: _{task_id}_ 
        ■ Execution Date (KST): _{execution_date}_ 
        ■ Log Url : {log_url}'''

    # Slack Operator를 사용하여 메세지 보냄
    alert = SlackAPIPostOperator(
        task_id='slack_failed', channel=channel, token=token, text=text)

    return alert.execute(context=context)
