from airflow import DAG
from airflow.models import Variable
from airflow import AirflowException
from airflow.operators.python import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


from plugins.alert import on_failure
from plugins.operators.data_quality import DataQualityOperator

from datetime import datetime
from datetime import timedelta

import pendulum
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


def get_Redshift_connection():
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    return hook.get_conn().cursor()


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

    if obj:
        hook.delete_objects(bucket, key)
        logging.info('Delete key name ' + key)

    hook.load_string(data, key, bucket_name=bucket)

    logging.info('[END_TASK]_load_into_s3')
    pass


def transform(**context):
    logging.info('[START_TASK]_transform')

    execution_date = context['execution_date']
    key = execution_date.strftime('%Y%m%d') + '.json'

    # read by s3
    # hook = S3Hook()
    # bucket = s3_config['bucket']
    # data = hook.read_key(key=key, bucket_name=bucket)

    # read by xcom
    data = context["task_instance"].xcom_pull(
        key="return_value", task_ids="get_data_by_api")

    j_data = json.loads(data)

    ret = []
    for d in j_data["daily"]:
        day = datetime.fromtimestamp(d["dt"]).strftime('%Y-%m-%d')
        ret.append("('{}', {}, {}, {})".format(
            day, d["temp"]["day"], d["temp"]["min"], d["temp"]["max"]))

    logging.info(ret)

    cur = get_Redshift_connection()
    insert_sql = """DELETE FROM {schema}.{table}; INSERT INTO {schema}.{table} VALUES """.format(schema='raw_data', table='weather_forecast') + \
        ",".join(ret)
    logging.info(insert_sql)

    try:
        cur.execute(insert_sql)
        cur.execute("COMMIT;")
    except Exception as e:
        cur.execute("Rollback;")
        logging.error("[ERROR with insert_sql. COMPLETE ROLLBACK!]")
        raise AirflowException(e)

    logging.info('[END_TASK]_transform')


def load_into_redshift(**context):
    logging.info("[START_TASK]_load_into_redshift")

    data = context["task_instance"].xcom_pull(
        key="return_value", task_ids="get_data_by_api")

    j_data = json.loads(data)
    logging.info(j_data)
    logging.info("[END_TASK]_load_into_redshift")


kst = pendulum.timezone("Asia/Seoul")

default_args = {
    'owner': 'plerin',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': on_failure

}

with DAG(
    dag_id='etl_by_full',
    default_args=default_args,
    start_date=datetime(2021, 1, 1, tzinfo=kst),
    schedule_interval='*/3 * * * *',
    tags=['mine'],
    max_active_runs=1,
    concurrency=1,
    catchup=True
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

    endRun = DummyOperator(
        task_id='endRun',
        trigger_rule='none_failed_or_skipped'
    )

    # Check for quality issues in ingested data
    tables = ["raw_data.weather_forecast"]
    check_data_quality = DataQualityOperator(task_id='run_data_quality_checks',
                                             redshift_conn_id="redshift_dev_db",
                                             table_names=tables)

    get_data_by_api >> load_into_s3 >> transform >> check_data_quality
    check_data_quality >> endRun
