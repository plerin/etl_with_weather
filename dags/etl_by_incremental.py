from airflow import DAG
from airflow.models import Variable
from airflow import AirflowException
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from datetime import datetime
from datetime import timedelta

import requests
import logging
import json


'''
1/27
redshift 연동
ec2 사용성 확인


앞으로 남은 것
incremental_update 버전 코드 작성
pk 제약사항 확인(upsert)
멱등성 확인
데이터 입력 확인(etl_project_03 data_quality 참조)
slack 연동
'''


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


def read_by_s3():
    logging.info(s3_config)
    bucket = s3_config['bucket']
    hook = S3Hook()
    keys = hook.list_keys(bucket_name=bucket)
    logging.info(keys)
    data = hook.read_key(key='220126.json', bucket_name=bucket)

    j_data = json.loads(data)

    logging.info(j_data["daily"])


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
        cur.execute("COMMIT;")
    except Exception as e:
        cur.execute("ROLLBACK;")
        logging.error("[Occur_the_error_with_create_sql]_Complete_ROLLBACK!")
        raise AirflowException(str(e))

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
        raise AirflowException(e)

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


def connect_redshift(**context):
    execution_date = context['execution_date']
    key = execution_date.strftime('%Y%m%d') + '.json'

    ret = ["('2022-01-27', 0.96, -2.43, 3.14)", "('2022-01-28', -1.7, -3.81, -0.12)", "('2022-01-29', -2.77, -4.56, -1.31)", "('2022-01-30', -1.86, -4.82, 0.02)",
           "('2022-01-31', -0.31, -3.29, 0.89)", "('2022-02-01', -0.12, -2.77, -0.12)", "('2022-02-02', -1.48, -4, -0.44)", "('2022-02-03', -1.92, -4.01, -1.25)"]

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


default_args = {
    'owner': 'plerin',
    'start_date': datetime(2022, 1, 1),
    'schedule_interval': '0 0 * * *',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'tag': ['mine']


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

    get_data_by_api >> load_into_s3 >> transform >> load_into_redshift
