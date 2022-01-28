from airflow import DAG
from airflow.operators.python import PythonOperator

from datetime import datetime, timedelta, tzinfo
import pendulum


def print_hello():
    print("hello!")
    return "hello!"


def print_goodbye():
    print("goodbye!")
    return "goodbye!"


default_args = {
    'owner': 'plerin',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    'hello_world',
    default_args=default_args,
    start_date=datetime(2022, 1, 1),
    catchup=True,
    tags=['mine'],
    schedule_interval='0 0 * * *'
) as dag:

    print_hello = PythonOperator(
        task_id='print_hello',
        # python_callable param points to the function you want to run
        python_callable=print_hello,
        # dag param points to the DAG that this task is a part of
    )

    print_goodbye = PythonOperator(
        task_id='print_goodbye',
        python_callable=print_goodbye,
    )

    # Assign the order of the tasks in our DAG
    print_hello >> print_goodbye
