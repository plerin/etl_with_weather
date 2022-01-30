from airflow import DAG
from airflow.models import Variable
from airflow import AirflowException
from airflow.operators.python import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

# from plugins import slack
from plugins.alert import on_failure
# from plugins.operators.data_quality import DataQualityOperator
from plugins.operators.data_quality import DataQualityOperator

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
    # logging.info(data)
    return response.text


def check_data(ti):

    fetchedDate = ti.xcom_pull(key='return_value', task_ids=[
                               'get_data_by_api'])

    logging.info(ti)

    if fetchedDate[0] is not None:
        return 'load_into_s3'
    return 'endRun'


def load_into_s3(**context):
    logging.info('[START_TASK]_load_into_s3')

    data = context["task_instance"].xcom_pull(
        key="return_value", task_ids="get_data_by_api")
    execution_date = context['execution_date']
    key = execution_date.strftime('%Y%m%d') + '.json'

    hook = S3Hook()
    bucket = s3_config['bucket']

    # return : s3.Object(bucket_name='etl-with-weather', key='20220101.json')
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
    logging.info('[END_TASK]_transform')

    return ret


def load_into_redshift(**context):
    logging.info("[START_TASK]_load_into_redshift")

    schema = context["params"]["schema"]
    table = context["params"]["table"]
    weather_data = context["task_instance"].xcom_pull(
        key="return_value", task_ids="transform")

    cur = get_Redshift_connection()

    # create temp table for upsert(pk_constraint)
    create_sql = f"""DROP TABLE IF EXISTS {schema}.temp_{table};
    CREATE TABLE {schema}.temp_{table} (LIKE {schema}.{table} INCLUDING DEFAULTS); INSERT INTO {schema}.temp_{table} SELECT * FROM {schema}.{table}"""

    try:
        cur.execute(create_sql)
        cur.execute("CCOMMIT;")
    except Exception as e:
        cur.execute("ROLLBACK;")
        logging.error("[Occur_the_error_with_create_sql]_Complete_ROLLBACK!")
        raise Exception(e)

    # insert data into temp_table from origin_table
    insert_sql = f"""INSERT INTO {schema}.temp_{table} VALUES """ + \
        ",".join(weather_data)
    # logging.info(insert_sql)

    try:
        cur.execute(insert_sql)
        cur.execute("COMMIT;")
    except Exception as e:
        cur.execute("Rollback;")
        logging.error("[Occur_the_error_with_insert_sql]_Complete_ROLLBACK!")
        raise AirflowException(str(e))

    # arter origin_table
    alter_sql = f"""DELETE FROM {schema}.{table};
    INSERT INTO {schema}.{table}
    SELECT date, temp, min_temp, max_temp FROM (
        SELECT *, ROW_NUMBER() OVER(PARTITION BY date ORDER BY updated_date DESC) seq
        FROM {schema}.temp_{table}
    )
    WHERE seq = 1;"""

    try:
        cur.execute(alter_sql)
        cur.execute("COMMIT;")
    except Exception as e:
        cur.execute("ROLLBACK;")
        logging.error("[Occur_the_error_with_insert_sql]_Complete_ROLLBACK!")
        raise AirflowException(e)

    logging.info("[END_TASK]_load_into_redshift")


default_args = {
    'owner': 'plerin',
    'start_date': datetime(2022, 1, 1),
    'schedule_interval': '30 0 * * *',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'tag': ['mine'],
    'on_failure_callback': on_failure

}

with DAG(
    dag_id='etl_by_incremental',
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

    load_into_redshift = PythonOperator(
        task_id='load_into_redshift',
        python_callable=load_into_redshift,
        params={
            'schema': 'raw_data',
            'table': 'weather_forecast'
        }
    )

    check_data = BranchPythonOperator(
        task_id='check_data',
        python_callable=check_data,
        do_xcom_push=False
    )

    # Check for quality issues in ingested data
    # DataQualityOperator -> plugins/operators/data_quality.py
    tables = ["raw_data.weather_forecast"]
    check_data_quality = DataQualityOperator(task_id='run_data_quality_checks',
                                             redshift_conn_id="redshift_dev_db",
                                             table_names=tables)

    endRun = DummyOperator(
        task_id='endRun',
        # upstream tasks results(default = all_success = upstream tasks have succeeded)
        trigger_rule='none_failed_or_skipped'
    )

    get_data_by_api >> check_data
    check_data >> [load_into_s3, endRun]
    load_into_s3 >> transform >> load_into_redshift >> check_data_quality
    check_data_quality >> endRun
    # get_data_by_api >> load_into_s3 >> transform >> load_into_redshift
