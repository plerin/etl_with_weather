from airflow import DAG
from airflow.operators.python import PythonOperator
# from airflow.operators import PythonOperator
from airflow.models import Variable
from airflow.hooks.postgres_hook import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from datetime import datetime
from datetime import timedelta

import requests
import logging
import json


s3_config = Variable.get("aws_s3_config", deserialize_json=True)

API_URL = "https://api.openweathermap.org/data/2.5/onecall?lat={lat}&lon={lon}&exclude={exclude}&appid={api_key}&units=metric"
API_KEY = Variable.get("open_weather_api_key")
COORD = {
    'lat': 37.551254,
    'lon': 126.988409
}
EXCLUDE = 'current,minutely,hourly'


def extract(**context):
    logging.info('[START_TASK]_extract')

    response = requests.get(
        API_URL.format(
            lat=COORD['lat'],
            lon=COORD['lon'],
            exclude=EXCLUDE,
            api_key=API_KEY
        )
    )
    data = json.loads(response.text)
    logging.info('[END_TASK]_extract')
    # logging.info(data)

    pass


def read_by_s3():
    logging.info(s3_config)
    bucket = s3_config['bucket']
    hook = S3Hook()
    keys = hook.list_keys(bucket_name=bucket)
    logging.info(keys)
    data = hook.read_key(key='220126.json', bucket_name=bucket)

    j_data = json.loads(data)

    logging.info(j_data["daily"])
    # logging.inf(data)
    # key = 'data'


def get_data_by_api(**context):
    logging.info('[START_TASK]_get_data_by_api')

    response = requests.get(
        API_URL.format(
            lat=COORD['lat'],
            lon=COORD['lon'],
            exclude=EXCLUDE,
            api_key=API_KEY
        )
    )
    data = json.loads(response.text)
    logging.info('[END_TASK]_get_data_by_api')
    # logging.info(data)
    return response.text


def load_into_s3(**context):
    logging.info('[START_TASK]_load_into_s3')

    data = context["task_instance"].xcom_pull(
        key="return_value", task_ids="get_data_by_api")
    execution_date = context['execution_date']
    key = execution_date.strftime('%Y%m%d') + '.json'

    hook = S3Hook()
    bucket = s3_config['bucket']

    obj = hook.get_key(key, bucket_name=bucket)
    logging.info(obj)
    if obj:
        hook.delete_objects(bucket, key)
        logging.info('Delete key name ' + key)
    # //delete_objects

    hook.load_string(data, key, bucket_name=bucket)

    logging.info('[END_TASK]_load_into_s3')
    pass


def transform(**context):
    logging.info('[START_TASK]_transform')

    execution_date = context['execution_date']
    key = execution_date.strftime('%Y%m%d') + '.json'

    hook = S3Hook()
    bucket = s3_config['bucket']
    data = hook.read_key(key=key, bucket_name=bucket)

    j_data = json.loads(data)
    logging.info(j_data)
    logging.info('[END_TASK]_transform')


def load_into_redshift():
    pass


default_args = {
    'owner': 'plerin',
    'start_date': datetime(2022, 1, 1),
    'schedule_interval': '0 0 * * *',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'tag': ['mine']
}

with DAG(
    dag_id='collect_by_full_refresh',
    default_args=default_args,
    max_active_runs=1,
    concurrency=1
) as dag:

    get_data_by_api = PythonOperator(
        task_id='get_data_by_api',
        python_callable=get_data_by_api
    )
    load_into_s3 = PythonOperator(
        task_id='load_into_s3',
        python_callable=load_into_s3
    )

    transform = PythonOperator(
        task_id='transform',
        python_callable=transform
    )
    get_data_by_api >> load_into_s3 >> transform
